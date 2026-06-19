"""
Daily backtest data updater for MES and N225MC.

Connects to IB (TWS/Gateway must be running), fetches incremental OHLCV bars,
and pushes to GitHub.

Schedule: 06:30 and 15:55 JST daily via Windows Task Scheduler (run_update.bat).
IB port:  4002 (paper/gateway)

Rollover behaviour
------------------
When the front month changes from the previous run:
  1. Fetch the OLD contract up to its last trading day (14 D lookback).
  2. Fetch the NEW contract for the past 1 week (initial seed).
On subsequent runs the new contract is updated incrementally like normal.

State is persisted in data/.state.json so rollover is detected even if the
script was stopped for several days.
"""
from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
from ib_async import Future, IB, util

# ── configuration ──────────────────────────────────────────────────────────────
IB_HOST = "127.0.0.1"
IB_PORT = 4002
IB_CLIENT_ID = 499  # distinct from trading clients (410 = dual loop)

REPO_DIR = Path(__file__).resolve().parent
DATA_DIR  = REPO_DIR / "data"
LOG_DIR   = REPO_DIR / "logs"
STATE_FILE = DATA_DIR / ".state.json"   # tracks current contract month per instrument
DISCORD_WEBHOOK_FILE = REPO_DIR / "discord_webhook_bachdata.txt"

JST = ZoneInfo("Asia/Tokyo")

ROLL_BUFFER_DAYS    = 7    # roll to next contract when < 7 days remain to expiry
OLD_CONTRACT_LOOKBACK = "14 D"  # how far back to pull old contract at rollover
NEW_CONTRACT_SEED     = "10 D"  # initial lookback for new contract at rollover
IB_PACE_SECONDS       = 3.0    # wait between reqHistoricalData calls (IB pacing)

INSTRUMENTS: dict[str, dict] = {
    "MES": {
        "symbol": "MES",
        "exchange": "CME",
        "currency": "USD",
        "trading_class": "MES",
    },
    "N225MC": {
        "symbol": "N225MC",
        "exchange": "OSE.JPN",
        "currency": "JPY",
        "trading_class": "225MC",
    },
}

# (filename suffix, IB bar size string)
TIMEFRAMES: list[tuple[str, str]] = [
    ("5_mins",  "5 mins"),
    ("30_mins", "30 mins"),
    ("1_day",   "1 day"),
]


# ── discord ────────────────────────────────────────────────────────────────────

def _webhook_url() -> str | None:
    try:
        return DISCORD_WEBHOOK_FILE.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return None


def discord_notify(message: str) -> None:
    url = _webhook_url()
    if not url:
        return
    try:
        payload = json.dumps({"content": message}).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=payload,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "backdata-updater/1.0",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10):
            pass
    except Exception as exc:
        print(f"  [discord] notify failed: {exc!r}")


# ── state file ─────────────────────────────────────────────────────────────────

def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


# ── contract helpers ───────────────────────────────────────────────────────────

def _parse_expiry(exp_str: str) -> datetime | None:
    exp_str = exp_str.strip()
    try:
        if len(exp_str) >= 8:
            return datetime.strptime(exp_str[:8], "%Y%m%d").replace(tzinfo=ZoneInfo("UTC"))
        if len(exp_str) == 6:
            y, m = int(exp_str[:4]), int(exp_str[4:])
            if m == 12:
                return datetime(y + 1, 1, 1, tzinfo=ZoneInfo("UTC"))
            return datetime(y, m + 1, 1, tzinfo=ZoneInfo("UTC"))
    except ValueError:
        pass
    return None


def _make_future(spec: dict, month: str = "", include_expired: bool = False) -> Future:
    c = Future(
        symbol=spec["symbol"],
        exchange=spec["exchange"],
        currency=spec["currency"],
        lastTradeDateOrContractMonth=month,
        includeExpired=include_expired,
    )
    if spec.get("trading_class"):
        c.tradingClass = spec["trading_class"]
    return c


async def find_front_month(ib: IB, spec: dict) -> tuple:
    """Return (qualified_contract, month_str) for the tradable front month."""
    details = await ib.reqContractDetailsAsync(_make_future(spec))
    if not details:
        raise RuntimeError(f"No contract details for {spec['symbol']} on {spec['exchange']}")

    cutoff = datetime.now(ZoneInfo("UTC")) + timedelta(days=ROLL_BUFFER_DAYS)
    candidates: list[tuple[datetime, object]] = []
    for d in details:
        c = d.contract
        exp_dt = _parse_expiry(c.lastTradeDateOrContractMonth or "")
        if exp_dt and exp_dt > cutoff:
            candidates.append((exp_dt, c))

    if not candidates:
        raise RuntimeError(
            f"No tradable front contract for {spec['symbol']} "
            f"(roll buffer {ROLL_BUFFER_DAYS}d)"
        )

    candidates.sort(key=lambda x: x[0])
    qualified = await ib.qualifyContractsAsync(candidates[0][1])
    if not qualified:
        raise RuntimeError(f"Could not qualify {spec['symbol']} front month")
    contract = qualified[0]
    return contract, contract.lastTradeDateOrContractMonth


async def qualify_expired(ib: IB, spec: dict, month: str):
    """Qualify an expired (or near-expiry) contract for historical data pull."""
    qualified = await ib.qualifyContractsAsync(_make_future(spec, month, include_expired=True))
    if not qualified:
        raise RuntimeError(f"Could not qualify expired {spec['symbol']} {month}")
    return qualified[0]


# ── data helpers ───────────────────────────────────────────────────────────────

def _fetch_duration(last_ts: pd.Timestamp | None) -> str:
    """Choose IB durationStr based on staleness."""
    if last_ts is None:
        return "365 D"
    now = pd.Timestamp.now(tz=last_ts.tzinfo)  # match tz so subtraction works
    days = max(int((now - last_ts).days) + 3, 1)
    if days <= 7:
        return "7 D"
    if days <= 30:
        return "30 D"
    if days <= 90:
        return "90 D"
    return "365 D"


async def fetch_bars(ib: IB, contract, bar_size: str, duration: str) -> pd.DataFrame:
    bars = await ib.reqHistoricalDataAsync(
        contract,
        endDateTime="",
        durationStr=duration,
        barSizeSetting=bar_size,
        whatToShow="TRADES",
        useRTH=False,
        formatDate=1,
        keepUpToDate=False,
    )
    if not bars:
        return pd.DataFrame()
    frame = util.df(bars).rename(columns={"date": "datetime"}).set_index("datetime")
    frame.index = pd.to_datetime(frame.index)  # normalise date/datetime to DatetimeIndex
    return frame[["open", "high", "low", "close", "volume"]]


def load_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_parquet(path)
    df.index = pd.to_datetime(df.index)  # normalise date/datetime.date to DatetimeIndex
    return df


def merge_and_save(existing: pd.DataFrame, new: pd.DataFrame, path: Path) -> pd.DataFrame:
    if new.empty:
        return existing
    combined = pd.concat([existing, new]) if not existing.empty else new
    combined = combined[~combined.index.duplicated(keep="last")].sort_index()
    path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(path)
    return combined


def _append(name: str, month: str, new_bars: pd.DataFrame, tf_name: str) -> int:
    """Append new_bars to both continuous and per-contract parquet. Returns count added."""
    continuous_path = DATA_DIR / f"{name}_continuous_{tf_name}.parquet"
    contract_path   = DATA_DIR / f"{name}_{month}_{tf_name}.parquet"
    existing = load_parquet(continuous_path)
    last_ts = pd.Timestamp(existing.index.max()) if not existing.empty else None
    filtered = new_bars[new_bars.index > last_ts] if last_ts is not None else new_bars
    if filtered.empty:
        return 0
    merge_and_save(existing, filtered, continuous_path)
    merge_and_save(load_parquet(contract_path), filtered, contract_path)
    return len(filtered)


# ── per-instrument update logic ────────────────────────────────────────────────

async def _pull_contract(
    ib: IB, name: str, spec: dict, contract, month: str, duration: str, label: str
) -> None:
    """Fetch `duration` of bars for `contract` and append to continuous + per-contract files."""
    for tf_name, bar_size in TIMEFRAMES:
        bars = await fetch_bars(ib, contract, bar_size, duration)
        added = _append(name, month, bars, tf_name)
        if added:
            cont_path = DATA_DIR / f"{name}_continuous_{tf_name}.parquet"
            df = load_parquet(cont_path)
            print(f"  [{tf_name}] {label}: +{added} bars → through {str(df.index.max())[:16]}")
        else:
            print(f"  [{tf_name}] {label}: already up to date")
        await asyncio.sleep(IB_PACE_SECONDS)


async def update_instrument(ib: IB, name: str, spec: dict, state: dict) -> str:
    """Return a short summary string for Discord notification."""
    prev_month = state.get(name, {}).get("contract_month")

    # ── find current front month ──
    contract, month_str = await find_front_month(ib, spec)
    print(f"  front month: {month_str}  conId={contract.conId}")

    rollover = prev_month is not None and prev_month != month_str
    summary_lines: list[str] = []

    if rollover:
        print(f"  ROLLOVER detected: {prev_month} → {month_str}")
        summary_lines.append(f"🔄 ロールオーバー {prev_month} → {month_str}")

        # 1. Pull old contract up to its last trading day
        print(f"  Fetching old contract {prev_month} (last {OLD_CONTRACT_LOOKBACK}) ...")
        try:
            old_contract = await qualify_expired(ib, spec, prev_month)
            await _pull_contract(
                ib, name, spec, old_contract, prev_month,
                OLD_CONTRACT_LOOKBACK, f"old {prev_month}",
            )
        except Exception as exc:
            print(f"  WARNING: could not pull old contract {prev_month}: {exc!r}")

        # 2. Seed new contract with 1-week lookback
        print(f"  Fetching new contract {month_str} seed ({NEW_CONTRACT_SEED}) ...")
        await _pull_contract(
            ib, name, spec, contract, month_str,
            NEW_CONTRACT_SEED, f"new {month_str} seed",
        )

    else:
        # Normal incremental update — each timeframe is independent;
        # a failure in one does not block the others or corrupt state.
        fetch_errors: list[str] = []
        for tf_name, bar_size in TIMEFRAMES:
            try:
                existing = load_parquet(DATA_DIR / f"{name}_continuous_{tf_name}.parquet")
                last_ts  = pd.Timestamp(existing.index.max()) if not existing.empty else None
                duration = _fetch_duration(last_ts)
                print(f"  [{tf_name}] last={str(last_ts)[:16] if last_ts else 'none'}  requesting {duration}")
                bars = await fetch_bars(ib, contract, bar_size, duration)
                added = _append(name, month_str, bars, tf_name)
                if added:
                    cont = load_parquet(DATA_DIR / f"{name}_continuous_{tf_name}.parquet")
                    through = str(cont.index.max())[:16]
                    print(f"  [{tf_name}] +{added} bars → through {through}")
                    summary_lines.append(f"  {tf_name}: +{added} bars → {through}")
                else:
                    print(f"  [{tf_name}] already up to date")
                    summary_lines.append(f"  {tf_name}: 更新なし")
            except Exception as exc:
                fetch_errors.append(tf_name)
                print(f"  [{tf_name}] ERROR: {exc!r}")
            await asyncio.sleep(IB_PACE_SECONDS)

        if fetch_errors:
            raise RuntimeError(f"fetch failed for timeframes: {fetch_errors}")

    # persist current contract month only after all fetches succeeded
    if name not in state:
        state[name] = {}
    state[name]["contract_month"] = month_str

    return "\n".join(summary_lines)


# ── git ────────────────────────────────────────────────────────────────────────

def git_push(today: str) -> None:
    subprocess.run(["git", "add", "data/"], cwd=REPO_DIR, check=True)
    diff = subprocess.run(
        ["git", "diff", "--cached", "--stat"],
        cwd=REPO_DIR, capture_output=True, text=True,
    )
    if not diff.stdout.strip():
        print("Git: nothing to commit.")
        return
    subprocess.run(
        ["git", "commit", "-m", f"data: update through {today}"],
        cwd=REPO_DIR, check=True,
    )
    subprocess.run(["git", "push"], cwd=REPO_DIR, check=True)
    print("Git: pushed.")


# ── main ───────────────────────────────────────────────────────────────────────

async def main() -> int:
    now_jst = datetime.now(JST)
    print(f"=== backdata update {now_jst.strftime('%Y-%m-%d %H:%M JST')} ===\n")

    DATA_DIR.mkdir(exist_ok=True)
    LOG_DIR.mkdir(exist_ok=True)

    state = load_state()
    errors: list[str] = []

    ib = IB()
    fetch_summaries: list[str] = []  # per-instrument success lines for final notify
    try:
        try:
            await ib.connectAsync(IB_HOST, IB_PORT, clientId=IB_CLIENT_ID)
        except Exception as exc:
            msg = f"IB connection failed: {exc!r}"
            print(msg)
            discord_notify(f"❌ **backdata** IB接続失敗\n```{exc}```")
            errors.append(f"connect: {exc!r}")
            save_state(state)
            return 1

        print(f"IB connected  {IB_HOST}:{IB_PORT}  clientId={IB_CLIENT_ID}\n")

        for name, spec in INSTRUMENTS.items():
            print(f"[{name}]")
            if not ib.isConnected():
                msg = f"{name}: IB disconnected mid-run, skipping"
                print(f"  {msg}")
                errors.append(msg)
                discord_notify(f"❌ **backdata [{name}]** IB接続が途中で切断")
                print()
                continue
            try:
                summary = await update_instrument(ib, name, spec, state)
                if summary:
                    fetch_summaries.append(f"**{name}**\n{summary}")
            except Exception as exc:
                msg = f"{name}: {exc!r}"
                print(f"  ERROR {msg}")
                errors.append(msg)
                discord_notify(f"❌ **backdata [{name}]** エラー\n```{exc}```")
            print()
    finally:
        ib.disconnect()

    save_state(state)

    today = now_jst.strftime("%Y-%m-%d")
    try:
        git_push(today)
    except subprocess.CalledProcessError as exc:
        errors.append(f"git: {exc!r}")
        print(f"Git ERROR: {exc!r}")
        discord_notify(f"❌ **backdata** git push 失敗\n```{exc}```")

    done_time = datetime.now(JST).strftime("%H:%M JST")
    print(f"\n=== done {done_time} ===")

    if errors:
        print("Errors:")
        for e in errors:
            print(f"  {e}")
        return 1

    # 全て成功したら完了通知（新規バーなしでも送る）
    body = "\n".join(fetch_summaries) if fetch_summaries else "新規データなし（最新済み）"
    discord_notify(f"✅ **backdata** 更新完了 ({done_time})\n{body}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
