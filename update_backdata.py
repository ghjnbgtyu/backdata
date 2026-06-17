"""
Daily backtest data updater for MES and N225MC.

Connects to IB (TWS/Gateway must be running), fetches incremental OHLCV bars
for the current front-month contract, appends to continuous parquet files,
and pushes to GitHub.

Schedule: 08:00 JST daily via Windows Task Scheduler (run_update.bat).
IB port:  4002 (paper/gateway read-only)
"""
from __future__ import annotations

import asyncio
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
from ib_async import Future, IB, util

# ── configuration ──────────────────────────────────────────────────────────────
IB_HOST = "127.0.0.1"
IB_PORT = 4002
IB_CLIENT_ID = 499  # distinct from trading clients

REPO_DIR = Path(__file__).resolve().parent
DATA_DIR = REPO_DIR / "data"
LOG_DIR = REPO_DIR / "logs"

JST = ZoneInfo("Asia/Tokyo")

# Roll to next contract when fewer than this many days remain until expiry
ROLL_BUFFER_DAYS = 7

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

# (suffix used in filename,  IB bar size string)
TIMEFRAMES: list[tuple[str, str]] = [
    ("5_mins", "5 mins"),
    ("1_day",  "1 day"),
]

IB_PACE_SECONDS = 3.0   # wait between reqHistoricalData calls (IB pacing)


# ── contract helpers ───────────────────────────────────────────────────────────

def _parse_expiry(exp_str: str) -> datetime | None:
    """Parse YYYYMMDD or YYYYMM expiry string to UTC datetime."""
    exp_str = exp_str.strip()
    try:
        if len(exp_str) >= 8:
            return datetime.strptime(exp_str[:8], "%Y%m%d").replace(tzinfo=ZoneInfo("UTC"))
        if len(exp_str) == 6:
            y, m = int(exp_str[:4]), int(exp_str[4:])
            # approximate: first day of the following month
            if m == 12:
                return datetime(y + 1, 1, 1, tzinfo=ZoneInfo("UTC"))
            return datetime(y, m + 1, 1, tzinfo=ZoneInfo("UTC"))
    except ValueError:
        pass
    return None


async def find_front_month(ib: IB, spec: dict) -> tuple:
    """Return (qualified_contract, month_str) for the tradable front month.

    Rolls to next contract when fewer than ROLL_BUFFER_DAYS remain.
    """
    base = Future(
        symbol=spec["symbol"],
        exchange=spec["exchange"],
        currency=spec["currency"],
    )
    if spec.get("trading_class"):
        base.tradingClass = spec["trading_class"]

    details = await ib.reqContractDetailsAsync(base)
    if not details:
        raise RuntimeError(f"No contract details for {spec['symbol']} on {spec['exchange']}")

    cutoff = datetime.now(ZoneInfo("UTC")) + timedelta(days=ROLL_BUFFER_DAYS)
    candidates: list[tuple[datetime, object]] = []
    for d in details:
        c = d.contract
        exp_str = c.lastTradeDateOrContractMonth or ""
        exp_dt = _parse_expiry(exp_str)
        if exp_dt and exp_dt > cutoff:
            candidates.append((exp_dt, c))

    if not candidates:
        raise RuntimeError(
            f"No tradable front contract for {spec['symbol']} "
            f"(roll buffer {ROLL_BUFFER_DAYS} days)"
        )

    candidates.sort(key=lambda x: x[0])
    front_contract = candidates[0][1]

    qualified = await ib.qualifyContractsAsync(front_contract)
    if not qualified:
        raise RuntimeError(f"Could not qualify {spec['symbol']} front month")

    contract = qualified[0]
    month_str = contract.lastTradeDateOrContractMonth
    return contract, month_str


# ── data helpers ───────────────────────────────────────────────────────────────

def _fetch_duration(last_ts: pd.Timestamp | None) -> str:
    """Choose IB durationStr based on how stale the data is."""
    if last_ts is None:
        return "365 D"
    days = max(int((pd.Timestamp.now() - last_ts).days) + 3, 1)
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
    frame = util.df(bars)
    frame = frame.rename(columns={"date": "datetime"}).set_index("datetime")
    return frame[["open", "high", "low", "close", "volume"]]


def load_parquet(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path) if path.exists() else pd.DataFrame()


def merge_and_save(existing: pd.DataFrame, new: pd.DataFrame, path: Path) -> pd.DataFrame:
    if new.empty:
        return existing
    combined = pd.concat([existing, new]) if not existing.empty else new
    combined = combined[~combined.index.duplicated(keep="last")].sort_index()
    path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(path)
    return combined


# ── per-instrument update ──────────────────────────────────────────────────────

async def update_instrument(ib: IB, name: str, spec: dict) -> list[str]:
    """Fetch and save incremental data. Returns list of changed file names."""
    contract, month_str = await find_front_month(ib, spec)
    print(f"  front month: {month_str}  conId={contract.conId}")
    changed: list[str] = []

    for tf_name, bar_size in TIMEFRAMES:
        continuous_path = DATA_DIR / f"{name}_continuous_{tf_name}.parquet"
        contract_path   = DATA_DIR / f"{name}_{month_str}_{tf_name}.parquet"

        existing = load_parquet(continuous_path)
        last_ts  = pd.Timestamp(existing.index.max()) if not existing.empty else None
        duration = _fetch_duration(last_ts)

        print(f"  [{tf_name}] last={str(last_ts)[:16] if last_ts else 'none'}  requesting {duration}")
        new_bars = await fetch_bars(ib, contract, bar_size, duration)

        if new_bars.empty:
            print(f"  [{tf_name}] IB returned no bars")
        else:
            if last_ts is not None:
                new_bars = new_bars[new_bars.index > last_ts]

            if new_bars.empty:
                print(f"  [{tf_name}] already up to date")
            else:
                updated = merge_and_save(existing, new_bars, continuous_path)
                # also update individual contract file
                merge_and_save(load_parquet(contract_path), new_bars, contract_path)
                print(
                    f"  [{tf_name}] +{len(new_bars)} bars → "
                    f"through {str(updated.index.max())[:16]}"
                )
                changed.append(continuous_path.name)
                changed.append(contract_path.name)

        await asyncio.sleep(IB_PACE_SECONDS)

    return changed


# ── git helpers ────────────────────────────────────────────────────────────────

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

    ib = IB()
    errors: list[str] = []
    try:
        await ib.connectAsync(IB_HOST, IB_PORT, clientId=IB_CLIENT_ID)
        print(f"IB connected  {IB_HOST}:{IB_PORT}  clientId={IB_CLIENT_ID}\n")

        for name, spec in INSTRUMENTS.items():
            print(f"[{name}]")
            try:
                await update_instrument(ib, name, spec)
            except Exception as exc:
                msg = f"{name}: {exc!r}"
                print(f"  ERROR {msg}")
                errors.append(msg)
            print()
    finally:
        ib.disconnect()

    today = now_jst.strftime("%Y-%m-%d")
    try:
        git_push(today)
    except subprocess.CalledProcessError as exc:
        errors.append(f"git: {exc!r}")
        print(f"Git ERROR: {exc!r}")

    print(f"\n=== done {datetime.now(JST).strftime('%H:%M JST')} ===")
    if errors:
        print("Errors:")
        for e in errors:
            print(f"  {e}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
