"""
既存の 5_mins 連続データから 30_mins 連続データを生成する。
初回のみ実行。以降は update_backdata.py が差分追加する。
"""
from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pandas as pd

DATA_DIR = Path(__file__).resolve().parent / "data"
LOOKBACK_DAYS = 730


def resample_to_30min(df5: pd.DataFrame) -> pd.DataFrame:
    """5分足 → 30分足リサンプル（各バーの先頭時刻ベース）"""
    return (
        df5.resample("30min")
        .agg({"open": "first", "high": "max", "low": "min",
              "close": "last", "volume": "sum"})
        .dropna(subset=["open"])
    )


def bootstrap(name: str) -> None:
    src = DATA_DIR / f"{name}_continuous_5_mins.parquet"
    dst = DATA_DIR / f"{name}_continuous_30_mins.parquet"

    if not src.exists():
        print(f"  [{name}] 5_mins ファイルなし: {src}")
        return

    df5 = pd.read_parquet(src)
    df5.index = pd.to_datetime(df5.index)

    # 2年分に絞る
    cutoff = df5.index.max() - timedelta(days=LOOKBACK_DAYS)
    df5 = df5[df5.index >= cutoff]

    df30 = resample_to_30min(df5)

    if dst.exists():
        existing = pd.read_parquet(dst)
        existing.index = pd.to_datetime(existing.index)
        combined = pd.concat([existing, df30])
        combined = combined[~combined.index.duplicated(keep="last")].sort_index()
    else:
        combined = df30

    combined.to_parquet(dst)
    print(f"  [{name}] 30_mins: {len(combined)} bars  "
          f"{str(combined.index.min())[:16]} ~ {str(combined.index.max())[:16]}")
    print(f"  保存先: {dst}")


if __name__ == "__main__":
    print("=== 30分足ブートストラップ ===\n")
    for instrument in ["MES", "N225MC"]:
        bootstrap(instrument)
    print("\n完了")
