from __future__ import annotations

"""数据库增量更新脚本。

优先使用在线 API 更新，失败时回退到本地缓存，保证更新任务稳健可执行。
"""

import os
import sqlite3
from pathlib import Path

import pandas as pd

from topic10_workflow import (
    DEFAULT_STOCKS,
    create_tables,
    download_a_share_via_baostock,
    incremental_update_macro,
    normalize_stock_info,
    normalize_stock_price,
    load_cached_a_share,
    sqlite_connect,
    upsert_stock_info,
    upsert_stock_price,
    write_update_log,
)


def incremental_update_stock_from_cache(conn: sqlite3.Connection, cache_dir: Path) -> int:
    """从本地缓存执行 A 股行情增量更新。"""
    hs300, stocks_daily, stock_info_raw, _ = load_cached_a_share(cache_dir)
    stock_price_df = normalize_stock_price(hs300, stocks_daily)

    latest_by_code = pd.read_sql_query(
        "SELECT code, MAX(date) AS latest_date FROM stock_price GROUP BY code",
        conn,
    )
    latest_map = dict(zip(latest_by_code["code"], latest_by_code["latest_date"]))

    pieces = []
    for code, grp in stock_price_df.groupby("code"):
        latest = latest_map.get(code)
        if latest:
            pieces.append(grp[grp["date"] > latest].copy())
        else:
            pieces.append(grp.copy())

    if pieces:
        inc_df = pd.concat(pieces, ignore_index=True)
    else:
        inc_df = pd.DataFrame(columns=stock_price_df.columns)

    inserted = 0
    if not inc_df.empty:
        inserted = upsert_stock_price(conn, inc_df)

    stock_info_df = normalize_stock_info(stock_info_raw, DEFAULT_STOCKS.keys())
    upsert_stock_info(conn, stock_info_df)
    write_update_log(conn, "stock_price", inserted, "incremental load from local cache")
    write_update_log(conn, "stock_info", len(stock_info_df), "refresh stock_info")
    return inserted


def main() -> None:
    """脚本入口：执行宏观与 A 股增量更新。"""
    project_dir = Path(__file__).resolve().parent
    db_path = project_dir / "fin_data.db"
    cache_dir = project_dir / "cache" / "a_share"

    conn = sqlite_connect(db_path)
    try:
        create_tables(conn)

        macro_inserted = 0
        if os.environ.get("FRED_API_KEY"):
            macro_inserted = incremental_update_macro(conn)
        else:
            write_update_log(conn, "macro_data", 0, "skip macro update: FRED_API_KEY missing")

        # A 股部分优先尝试在线下载；若失败，自动回退缓存数据。
        try:
            hs300, stocks_daily, stock_info_raw, _ = download_a_share_via_baostock(
                stock_codes=[
                    "sh.600519", "sz.000858", "sz.300750", "sz.002594", "sh.601318",
                    "sh.600036", "sh.600276", "sh.600900", "sh.600309", "sz.002415",
                ],
                start_date="2010-01-01",
            )
            stock_price_df = normalize_stock_price(hs300, stocks_daily)
            stock_info_df = normalize_stock_info(stock_info_raw, DEFAULT_STOCKS.keys())

            latest_by_code = pd.read_sql_query(
                "SELECT code, MAX(date) AS latest_date FROM stock_price GROUP BY code",
                conn,
            )
            latest_map = dict(zip(latest_by_code["code"], latest_by_code["latest_date"]))
            pieces = []
            for code, grp in stock_price_df.groupby("code"):
                latest = latest_map.get(code)
                pieces.append(grp[grp["date"] > latest].copy() if latest else grp.copy())
            inc_df = pd.concat(pieces, ignore_index=True) if pieces else pd.DataFrame(columns=stock_price_df.columns)

            stock_inserted = upsert_stock_price(conn, inc_df) if not inc_df.empty else 0
            upsert_stock_info(conn, stock_info_df)
            write_update_log(conn, "stock_price", stock_inserted, "incremental load via baostock API")
            write_update_log(conn, "stock_info", len(stock_info_df), "refresh stock_info via baostock API")
        except Exception as exc:
            stock_inserted = incremental_update_stock_from_cache(conn, cache_dir)
            write_update_log(conn, "stock_price", stock_inserted, f"fallback: local cache ({type(exc).__name__})")

        print("Update completed")
        print(f"macro_data inserted: {macro_inserted}")
        print(f"stock_price inserted: {stock_inserted}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
