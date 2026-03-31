"""Topic10 主流程脚本。

功能覆盖：
1. FRED 宏观数据下载与清洗
2. baostock A 股数据下载（含限频与重试）
3. SQLite 建表、写入、增量更新
4. SQL 查询分析与主题分析
5. 数据质量检测
"""

from __future__ import annotations

import os
import time
import sqlite3
from pathlib import Path
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple
from dotenv import load_dotenv

load_dotenv()

import pandas as pd
import requests

try:
    import baostock as bs
except Exception:  # pragma: no cover
    bs = None

FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

FRED_SERIES = {
    "DGS10": "US 10Y Treasury Yield",
    "DGS2": "US 2Y Treasury Yield",
    "FEDFUNDS": "Federal Funds Rate",
    "CPIAUCSL": "US CPI (NSA)",
    "UNRATE": "US Unemployment Rate",
    "DEXCHUS": "CNY/USD Exchange Rate",
}

DEFAULT_STOCKS = {
    "sh.600519": "贵州茅台",
    "sz.000858": "五粮液",
    "sz.300750": "宁德时代",
    "sz.002594": "比亚迪",
    "sh.601318": "中国平安",
    "sh.600036": "招商银行",
    "sh.600276": "恒瑞医药",
    "sh.600900": "长江电力",
    "sh.600309": "万华化学",
    "sz.002415": "海康威视",
}

HS300_CODE = "sh.000300"


def ensure_date_str(value: pd.Series) -> pd.Series:
    """将日期序列统一转换为 YYYY-MM-DD 字符串。"""
    dt = pd.to_datetime(value, errors="coerce")
    return dt.dt.strftime("%Y-%m-%d")


def read_csv_auto(path: Path) -> pd.DataFrame:
    """自动尝试多种编码读取 CSV，兼容中文环境下常见编码。"""
    encodings = ["utf-8", "utf-8-sig", "gbk", "gb18030", "latin1"]
    last_err: Optional[Exception] = None
    for enc in encodings:
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception as exc:  # pragma: no cover
            last_err = exc
    raise RuntimeError(f"Failed to read CSV: {path}. Last error: {last_err}")


def _rs_to_df(rs) -> pd.DataFrame:
    """将 baostock 的结果集对象转换为 DataFrame。"""
    rows = []
    while (rs.error_code == "0") and rs.next():
        rows.append(rs.get_row_data())
    return pd.DataFrame(rows, columns=rs.fields)


def _safe_baostock_query(query_func, *args, max_retries: int = 3, sleep_seconds: float = 1.2, jitter: float = 0.5, **kwargs):
    """带重试机制的 baostock 查询封装。"""
    last_err = ""
    for attempt in range(1, max_retries + 1):
        try:
            rs = query_func(*args, **kwargs)
            if rs.error_code == "0":
                return True, _rs_to_df(rs), attempt, "success"
            last_err = f"{rs.error_code}: {rs.error_msg}"
        except Exception as exc:  # pragma: no cover
            last_err = repr(exc)
        if attempt < max_retries:
            time.sleep(sleep_seconds + jitter * attempt / max_retries)
    return False, pd.DataFrame(), max_retries, last_err


def download_a_share_via_baostock(
    stock_codes: Iterable[str],
    start_date: str = "2010-01-01",
    end_date: Optional[str] = None,
    sleep_seconds: float = 1.2,
    max_retries: int = 3,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """批量下载 A 股数据并返回指数、个股、基本信息、下载日志。"""
    if bs is None:
        raise RuntimeError("baostock is not installed. Run `pip install baostock` first.")

    end_date = end_date or datetime.now().strftime("%Y-%m-%d")
    lg = bs.login()
    if lg.error_code != "0":
        raise RuntimeError(f"baostock login failed: {lg.error_code} {lg.error_msg}")

    try:
        ok, hs300, _, msg = _safe_baostock_query(
            bs.query_history_k_data_plus,
            HS300_CODE,
            "date,code,open,high,low,close,volume",
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag="3",
            max_retries=max_retries,
            sleep_seconds=sleep_seconds,
        )
        if not ok:
            raise RuntimeError(f"HS300 download failed: {msg}")

        stock_frames: List[pd.DataFrame] = []
        info_rows: List[Dict] = []
        logs: List[Dict] = []

        fields = "date,code,open,high,low,close,volume"
        for code in stock_codes:
            ok, daily, tries, status = _safe_baostock_query(
                bs.query_history_k_data_plus,
                code,
                fields,
                start_date=start_date,
                end_date=end_date,
                frequency="d",
                adjustflag="3",
                max_retries=max_retries,
                sleep_seconds=sleep_seconds,
            )
            logs.append(
                {
                    "code": code,
                    "status": "success" if ok else "failed",
                    "attempts": tries,
                    "rows": len(daily),
                    "message": status,
                }
            )

            if ok and not daily.empty:
                stock_frames.append(daily)

            ok_basic, basic_df, _, _ = _safe_baostock_query(bs.query_stock_basic, code=code)
            ok_ind, ind_df, _, _ = _safe_baostock_query(bs.query_stock_industry, code=code)
            basic_row = basic_df.iloc[0].to_dict() if ok_basic and not basic_df.empty else {}
            ind_row = ind_df.iloc[0].to_dict() if ok_ind and not ind_df.empty else {}

            info_rows.append(
                {
                    "code": code,
                    "code_name": basic_row.get("code_name"),
                    "ipoDate": basic_row.get("ipoDate"),
                    "industry": ind_row.get("industry"),
                    "latest_total_market_value": None,  # baostock原生接口无稳定总市值字段
                }
            )
            time.sleep(sleep_seconds)

        stock_daily = pd.concat(stock_frames, ignore_index=True) if stock_frames else pd.DataFrame(columns=["date", "code", "open", "high", "low", "close", "volume"])
        stock_info = pd.DataFrame(info_rows)
        download_log = pd.DataFrame(logs)
        return hs300, stock_daily, stock_info, download_log
    finally:
        bs.logout()


def fetch_fred_series(
    series_id: str,
    observation_start: str = "2000-01-01",
    observation_end: Optional[str] = None,
    api_key: Optional[str] = None,
    timeout: int = 30,
) -> pd.DataFrame:
    """下载单个 FRED 序列并返回标准长表结构。"""
    key = api_key or os.environ.get("FRED_API_KEY")
    if not key:
        raise ValueError("FRED_API_KEY is required. Set it in env or pass api_key explicitly.")

    params = {
        "series_id": series_id,
        "api_key": key,
        "file_type": "json",
        "observation_start": observation_start,
    }
    if observation_end:
        params["observation_end"] = observation_end

    response = requests.get(FRED_BASE_URL, params=params, timeout=timeout)
    response.raise_for_status()
    payload = response.json()

    if "observations" not in payload:
        raise RuntimeError(f"Unexpected FRED response for {series_id}: {payload}")

    obs = pd.DataFrame(payload["observations"])
    if obs.empty:
        return pd.DataFrame(columns=["date", "series_id", "value"])

    obs = obs[["date", "value"]].copy()
    obs["series_id"] = series_id
    obs["value"] = pd.to_numeric(obs["value"], errors="coerce")
    obs["date"] = pd.to_datetime(obs["date"])
    return obs.sort_values("date").reset_index(drop=True)


def fetch_fred_macro_wide(
    series_ids: Optional[Iterable[str]] = None,
    start_date: str = "2000-01-01",
    end_date: Optional[str] = None,
    api_key: Optional[str] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """下载多个 FRED 序列并合并为宽表（原始/清洗后）。"""
    ids = list(series_ids or FRED_SERIES.keys())
    long_frames: List[pd.DataFrame] = []

    for sid in ids:
        df = fetch_fred_series(sid, observation_start=start_date, observation_end=end_date, api_key=api_key)
        if df.empty:
            continue

        series = (
            df.set_index("date")["value"]
            .sort_index()
            .resample("ME")
            .last()
        )
        out = series.to_frame(name=sid).reset_index()
        out = out.rename(columns={"date": "date"})
        long_frames.append(out)

    if not long_frames:
        raise RuntimeError("No FRED data downloaded. Check API key and series IDs.")

    merged = long_frames[0]
    for frame in long_frames[1:]:
        merged = merged.merge(frame, on="date", how="outer")

    merged = merged.sort_values("date").set_index("date")
    merged_clean = merged.copy()

    # Missing-value strategy:
    # 1) monthly alignment with month-end values
    # 2) forward fill up to 2 months for sparse daily-to-monthly series
    # 3) remaining missing values kept as NaN for transparency
    merged_clean = merged_clean.ffill(limit=2)

    merged_clean.index = merged_clean.index.strftime("%Y-%m-%d")
    merged.index = merged.index.strftime("%Y-%m-%d")
    return merged, merged_clean


def macro_wide_to_long(macro_wide: pd.DataFrame) -> pd.DataFrame:
    """将宏观宽表转换为数据库可写入的长表。"""
    long_df = (
        macro_wide.reset_index(names="date")
        .melt(id_vars="date", var_name="series_id", value_name="value")
        .dropna(subset=["value"])
    )
    long_df["date"] = ensure_date_str(long_df["date"])
    return long_df[["date", "series_id", "value"]]


def load_cached_a_share(part2_output_dir: Path) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """从本地缓存读取 A 股相关 CSV。"""
    hs300 = read_csv_auto(part2_output_dir / "hs300_index_daily.csv")
    stocks_daily = read_csv_auto(part2_output_dir / "stocks_daily_all.csv")
    stock_info = read_csv_auto(part2_output_dir / "stock_basic_info.csv")
    download_log = read_csv_auto(part2_output_dir / "download_log.csv")
    return hs300, stocks_daily, stock_info, download_log


def normalize_stock_price(hs300: pd.DataFrame, stocks_daily: pd.DataFrame) -> pd.DataFrame:
    """统一指数和个股行情字段，输出 stock_price 标准结构。"""
    hs = hs300.copy()
    hs = hs[["date", "code", "open", "high", "low", "close", "volume"]]
    hs["adj_close"] = pd.to_numeric(hs["close"], errors="coerce")

    st = stocks_daily.copy()
    st = st[["date", "code", "open", "high", "low", "close", "volume"]]
    st["adj_close"] = pd.to_numeric(st["close"], errors="coerce")

    out = pd.concat([hs, st], ignore_index=True)
    out["date"] = ensure_date_str(out["date"])

    for col in ["open", "high", "low", "close", "volume", "adj_close"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out = out.dropna(subset=["date", "code"])\
        .sort_values(["code", "date"])\
        .drop_duplicates(subset=["code", "date"], keep="last")
    return out[["code", "date", "open", "high", "low", "close", "volume", "adj_close"]]


def normalize_stock_info(stock_info_raw: pd.DataFrame, stock_codes: Iterable[str]) -> pd.DataFrame:
    """统一股票基本信息字段，输出 stock_info 标准结构。"""
    info = stock_info_raw.copy()
    info["code"] = info["code"].astype(str)

    mapped = pd.DataFrame({"code": list(stock_codes)})
    mapped = mapped.merge(info, on="code", how="left")

    mapped["name"] = mapped.get("code_name")
    mapped["industry"] = mapped.get("industry")
    mapped["list_date"] = ensure_date_str(mapped.get("ipoDate"))
    mapped["market_cap"] = pd.to_numeric(mapped.get("latest_total_market_value"), errors="coerce")

    out = mapped[["code", "name", "industry", "list_date", "market_cap"]].drop_duplicates("code")
    return out


def sqlite_connect(db_path: Path) -> sqlite3.Connection:
    """创建 SQLite 连接并设置常用性能参数。"""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def create_tables(conn: sqlite3.Connection) -> None:
    """创建项目所需数据表（若不存在则创建）。"""
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS macro_data (
            date        TEXT NOT NULL,
            series_id   TEXT NOT NULL,
            value       REAL,
            PRIMARY KEY (date, series_id)
        );

        CREATE TABLE IF NOT EXISTS stock_price (
            code        TEXT NOT NULL,
            date        TEXT NOT NULL,
            open        REAL,
            high        REAL,
            low         REAL,
            close       REAL,
            volume      REAL,
            adj_close   REAL,
            PRIMARY KEY (code, date)
        );

        CREATE TABLE IF NOT EXISTS stock_info (
            code        TEXT PRIMARY KEY,
            name        TEXT,
            industry    TEXT,
            list_date   TEXT,
            market_cap  REAL
        );

        CREATE TABLE IF NOT EXISTS update_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_time TEXT NOT NULL,
            table_name TEXT NOT NULL,
            inserted_rows INTEGER NOT NULL,
            note TEXT
        );

        CREATE TABLE IF NOT EXISTS data_quality (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            check_name TEXT NOT NULL,
            code TEXT NOT NULL,
            date TEXT,
            issue_value REAL,
            detail TEXT,
            created_at TEXT NOT NULL
        );
        """
    )
    conn.commit()


def upsert_macro_data(conn: sqlite3.Connection, macro_long: pd.DataFrame) -> int:
    """将宏观数据写入 macro_data（主键冲突时更新）。"""
    rows = list(macro_long[["date", "series_id", "value"]].itertuples(index=False, name=None))
    conn.executemany(
        """
        INSERT INTO macro_data(date, series_id, value)
        VALUES (?, ?, ?)
        ON CONFLICT(date, series_id) DO UPDATE SET value=excluded.value
        """,
        rows,
    )
    conn.commit()
    return len(rows)


def upsert_stock_price(conn: sqlite3.Connection, stock_price_df: pd.DataFrame) -> int:
    """将行情数据写入 stock_price（主键冲突时更新）。"""
    rows = list(stock_price_df[["code", "date", "open", "high", "low", "close", "volume", "adj_close"]].itertuples(index=False, name=None))
    conn.executemany(
        """
        INSERT INTO stock_price(code, date, open, high, low, close, volume, adj_close)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(code, date) DO UPDATE SET
            open=excluded.open,
            high=excluded.high,
            low=excluded.low,
            close=excluded.close,
            volume=excluded.volume,
            adj_close=excluded.adj_close
        """,
        rows,
    )
    conn.commit()
    return len(rows)


def upsert_stock_info(conn: sqlite3.Connection, stock_info_df: pd.DataFrame) -> int:
    """将股票基本信息写入 stock_info（主键冲突时更新）。"""
    rows = list(stock_info_df[["code", "name", "industry", "list_date", "market_cap"]].itertuples(index=False, name=None))
    conn.executemany(
        """
        INSERT INTO stock_info(code, name, industry, list_date, market_cap)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(code) DO UPDATE SET
            name=excluded.name,
            industry=excluded.industry,
            list_date=excluded.list_date,
            market_cap=excluded.market_cap
        """,
        rows,
    )
    conn.commit()
    return len(rows)


def write_update_log(conn: sqlite3.Connection, table_name: str, inserted_rows: int, note: str = "") -> None:
    """记录本次更新日志。"""
    conn.execute(
        "INSERT INTO update_log(run_time, table_name, inserted_rows, note) VALUES (?, ?, ?, ?)",
        (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), table_name, inserted_rows, note),
    )
    conn.commit()


def get_latest_date(conn: sqlite3.Connection, table_name: str, date_col: str = "date", where_sql: str = "", params: Tuple = ()) -> Optional[str]:
    """获取指定表（可带条件）的最新日期。"""
    sql = f"SELECT MAX({date_col}) FROM {table_name} "
    if where_sql:
        sql += f" WHERE {where_sql}"
    cur = conn.execute(sql, params)
    value = cur.fetchone()[0]
    return value


def incremental_update_macro(conn: sqlite3.Connection, api_key: Optional[str] = None) -> int:
    """按序列增量更新宏观数据，只拉取最新日期之后的数据。"""
    total_rows = 0
    for sid in FRED_SERIES.keys():
        latest = get_latest_date(conn, "macro_data", where_sql="series_id = ?", params=(sid,))
        if latest:
            start = (pd.to_datetime(latest) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            start = "2000-01-01"

        df = fetch_fred_series(sid, observation_start=start, api_key=api_key)
        if df.empty:
            continue

        series = df.set_index("date")["value"].sort_index().resample("ME").last().ffill(limit=2)
        macro_long = pd.DataFrame(
            {
                "date": series.index.strftime("%Y-%m-%d"),
                "series_id": sid,
                "value": series.values,
            }
        ).dropna(subset=["value"])

        if not macro_long.empty:
            written = upsert_macro_data(conn, macro_long)
            total_rows += written

    write_update_log(conn, "macro_data", total_rows, "incremental update")
    return total_rows


def run_data_quality_checks(conn: sqlite3.Connection) -> Dict[str, int]:
    """执行数据质量检查并写入 data_quality。"""
    conn.execute("DELETE FROM data_quality")

    price_anomaly_sql = """
    WITH x AS (
        SELECT
            code,
            date,
            open,
            high,
            low,
            close,
            adj_close,
            LAG(adj_close) OVER (PARTITION BY code ORDER BY date) AS prev_close
        FROM stock_price
        WHERE code <> 'sh.000300'
    )
    INSERT INTO data_quality(check_name, code, date, issue_value, detail, created_at)
    SELECT
        'price_anomaly' AS check_name,
        code,
        date,
        (adj_close - prev_close) / prev_close AS issue_value,
        'Absolute daily return > 20% (excluding one-price bars)' AS detail,
        DATETIME('now', 'localtime') AS created_at
    FROM x
    WHERE prev_close IS NOT NULL
      AND ABS((adj_close - prev_close) / prev_close) > 0.2
      AND NOT (open = high AND high = low AND low = close);
    """

    zero_volume_sql = """
    INSERT INTO data_quality(check_name, code, date, issue_value, detail, created_at)
    SELECT
        'zero_volume' AS check_name,
        code,
        date,
        volume AS issue_value,
        'Volume equals 0, likely suspension or data issue' AS detail,
        DATETIME('now', 'localtime') AS created_at
    FROM stock_price
    WHERE code <> 'sh.000300'
      AND COALESCE(volume, 0) = 0;
    """

    continuity_sql = """
    WITH cal AS (
        SELECT date FROM stock_price WHERE code='sh.000300'
    ),
    ranges AS (
        SELECT code, MIN(date) AS min_date, MAX(date) AS max_date
        FROM stock_price
        WHERE code <> 'sh.000300'
        GROUP BY code
    ),
    expected AS (
        SELECT r.code, c.date
        FROM ranges r
        JOIN cal c ON c.date BETWEEN r.min_date AND r.max_date
    ),
    actual AS (
        SELECT code, date
        FROM stock_price
        WHERE code <> 'sh.000300'
    )
    INSERT INTO data_quality(check_name, code, date, issue_value, detail, created_at)
    SELECT
        'date_missing_against_hs300' AS check_name,
        e.code,
        e.date,
        NULL AS issue_value,
        'Missing trading day compared with HS300 calendar' AS detail,
        DATETIME('now', 'localtime') AS created_at
    FROM expected e
    LEFT JOIN actual a
      ON a.code = e.code AND a.date = e.date
    WHERE a.date IS NULL;
    """

    conn.executescript(price_anomaly_sql)
    conn.executescript(zero_volume_sql)
    conn.executescript(continuity_sql)
    conn.commit()

    summary = pd.read_sql_query(
        "SELECT check_name, COUNT(*) AS cnt FROM data_quality GROUP BY check_name ORDER BY check_name",
        conn,
    )
    return {row["check_name"]: int(row["cnt"]) for _, row in summary.iterrows()}


def run_required_sql_queries(conn: sqlite3.Connection) -> Dict[str, pd.DataFrame]:
    """执行任务书要求的 3 个 SQL 查询。"""
    q1 = pd.read_sql_query(
        """
        -- 查询1业务含义：计算美国收益率曲线利差（10Y-2Y）月度时序
        SELECT date,
               MAX(CASE WHEN series_id='DGS10' THEN value END) -
               MAX(CASE WHEN series_id='DGS2'  THEN value END) AS spread_10_2
        FROM macro_data
        GROUP BY date
        ORDER BY date;
        """,
        conn,
    )

    q2 = pd.read_sql_query(
        """
        -- 查询2业务含义：统计每只股票每年的平均收盘价与总成交量
        SELECT code, substr(date,1,4) AS year,
               AVG(adj_close) AS avg_close,
               SUM(volume)    AS total_volume
        FROM stock_price
        GROUP BY code, year
        ORDER BY code, year;
        """,
        conn,
    )

    q3 = pd.read_sql_query(
        """
        -- 查询3业务含义：按当前数据口径筛选“货币金融服务”且上市超过10年的股票
        SELECT s.code, s.name, s.industry, s.list_date
        FROM stock_info s
        WHERE s.industry LIKE '%货币金融服务%'
          AND (julianday('now') - julianday(s.list_date)) / 365.0 > 10;
        """,
        conn,
    )

    return {"query1": q1, "query2": q2, "query3": q3}


def run_custom_sql_queries(conn: sqlite3.Connection) -> Dict[str, pd.DataFrame]:
    """执行 2 个自定义 SQL 查询。"""
    q4 = pd.read_sql_query(
        """
        WITH daily_ret AS (
            SELECT
                code,
                substr(date,1,7) AS ym,
                (adj_close - LAG(adj_close) OVER (PARTITION BY code ORDER BY date)) /
                LAG(adj_close) OVER (PARTITION BY code ORDER BY date) AS ret
            FROM stock_price
            WHERE code <> 'sh.000300'
        )
        SELECT
            code,
            ym,
            AVG(ret) AS avg_daily_return,
            SQRT(AVG(ret * ret) - AVG(ret) * AVG(ret)) AS volatility
        FROM daily_ret
        WHERE ret IS NOT NULL
        GROUP BY code, ym
        ORDER BY code, ym;
        """,
        conn,
    )

    q5 = pd.read_sql_query(
        """
        WITH stock_year AS (
            SELECT
                code,
                substr(date,1,4) AS year,
                MIN(date) AS first_date,
                MAX(date) AS last_date
            FROM stock_price
            WHERE code <> 'sh.000300'
            GROUP BY code, substr(date,1,4)
        ),
        stock_ret AS (
            SELECT
                sy.code,
                sy.year,
                (p2.adj_close - p1.adj_close) / p1.adj_close AS annual_return
            FROM stock_year sy
            JOIN stock_price p1 ON p1.code = sy.code AND p1.date = sy.first_date
            JOIN stock_price p2 ON p2.code = sy.code AND p2.date = sy.last_date
        ),
        hs_year AS (
            SELECT
                substr(date,1,4) AS year,
                MIN(date) AS first_date,
                MAX(date) AS last_date
            FROM stock_price
            WHERE code = 'sh.000300'
            GROUP BY substr(date,1,4)
        ),
        hs_ret AS (
            SELECT
                hy.year,
                (p2.adj_close - p1.adj_close) / p1.adj_close AS hs300_return
            FROM hs_year hy
            JOIN stock_price p1 ON p1.code='sh.000300' AND p1.date = hy.first_date
            JOIN stock_price p2 ON p2.code='sh.000300' AND p2.date = hy.last_date
        )
        SELECT
            sr.code,
            si.name,
            sr.year,
            sr.annual_return,
            hr.hs300_return,
            sr.annual_return - hr.hs300_return AS excess_return
        FROM stock_ret sr
        LEFT JOIN hs_ret hr ON sr.year = hr.year
        LEFT JOIN stock_info si ON sr.code = si.code
        ORDER BY sr.code, sr.year;
        """,
        conn,
    )

    return {"query4": q4, "query5": q5}


def analyze_hike_cycle_fx(conn: sqlite3.Connection) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """主题分析：识别加息/降息阶段并计算汇率平均变动。"""
    df = pd.read_sql_query(
        """
        SELECT
            m1.date,
            m1.value AS fedfunds,
            m2.value AS dexchus
        FROM macro_data m1
        JOIN macro_data m2
          ON m1.date = m2.date
        WHERE m1.series_id='FEDFUNDS'
          AND m2.series_id='DEXCHUS'
        ORDER BY m1.date;
        """,
        conn,
    )

    if df.empty:
        return df, pd.DataFrame(columns=["cycle_type", "start_date", "end_date", "avg_fx_change"])

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    df["rate_diff"] = df["fedfunds"].diff()
    df["fx_change"] = df["dexchus"].pct_change()

    def classify(x: float) -> str:
        if pd.isna(x):
            return "flat"
        if x > 0:
            return "hike"
        if x < 0:
            return "cut"
        return "flat"

    df["cycle_type"] = df["rate_diff"].map(classify)

    df["group_key"] = (df["cycle_type"] != df["cycle_type"].shift(1)).cumsum()
    cycles = (
        df.groupby(["group_key", "cycle_type"], as_index=False)
        .agg(start_date=("date", "min"), end_date=("date", "max"), avg_fx_change=("fx_change", "mean"), n_obs=("date", "count"))
    )
    cycles = cycles[cycles["cycle_type"].isin(["hike", "cut"])].copy()
    cycles["start_date"] = cycles["start_date"].dt.strftime("%Y-%m-%d")
    cycles["end_date"] = cycles["end_date"].dt.strftime("%Y-%m-%d")

    summary = (
        cycles.groupby("cycle_type", as_index=False)["avg_fx_change"]
        .mean()
        .sort_values("cycle_type")
    )

    return df, summary


def bootstrap_project_from_cache(
    project_dir: Path,
    part2_output_dir: Path,
    fred_api_key: Optional[str] = None,
    with_quality_checks: bool = True,
) -> Dict[str, int]:
    """基于本地缓存完成项目初始化建库。"""
    project_dir.mkdir(parents=True, exist_ok=True)
    db_path = project_dir / "fin_data.db"

    hs300, stocks_daily, stock_info_raw, _ = load_cached_a_share(part2_output_dir)
    stock_price_df = normalize_stock_price(hs300, stocks_daily)
    stock_info_df = normalize_stock_info(stock_info_raw, DEFAULT_STOCKS.keys())

    macro_written = 0
    macro_cache = project_dir / "cache" / "fred_macro_monthly_clean.csv"
    macro_cache.parent.mkdir(parents=True, exist_ok=True)

    if fred_api_key or os.environ.get("FRED_API_KEY"):
        _, macro_clean = fetch_fred_macro_wide(api_key=fred_api_key)
        macro_clean.to_csv(macro_cache, index=True)
        macro_long = macro_wide_to_long(macro_clean)
    elif macro_cache.exists():
        macro_clean = pd.read_csv(macro_cache)
        if "date" not in macro_clean.columns:
            raise RuntimeError("Cached macro CSV must include a date column.")
        macro_long = (
            macro_clean.melt(id_vars="date", var_name="series_id", value_name="value")
            .dropna(subset=["value"])
            [["date", "series_id", "value"]]
        )
    else:
        macro_long = pd.DataFrame(columns=["date", "series_id", "value"])

    conn = sqlite_connect(db_path)
    try:
        create_tables(conn)
        if not macro_long.empty:
            macro_written = upsert_macro_data(conn, macro_long)
            write_update_log(conn, "macro_data", macro_written, "initial load")

        stock_price_written = upsert_stock_price(conn, stock_price_df)
        stock_info_written = upsert_stock_info(conn, stock_info_df)
        write_update_log(conn, "stock_price", stock_price_written, "initial load from cache")
        write_update_log(conn, "stock_info", stock_info_written, "initial load from cache")

        quality_counts: Dict[str, int] = {}
        if with_quality_checks:
            quality_counts = run_data_quality_checks(conn)

    finally:
        conn.close()

    return {
        "macro_rows": macro_written,
        "stock_price_rows": len(stock_price_df),
        "stock_info_rows": len(stock_info_df),
        **{f"dq_{k}": v for k, v in quality_counts.items()},
    }


def main() -> None:
    """命令行入口：基于缓存初始化数据库。"""
    root = Path(__file__).resolve().parent
    cache_a_share = root / "cache" / "a_share"
    legacy_part2 = root.parent / "Part2" / "part2_output（最终结果）"

    # 优先使用项目内缓存，保证独立仓库可直接复现。
    if cache_a_share.exists():
        source_dir = cache_a_share
    elif legacy_part2.exists():
        source_dir = legacy_part2
    else:
        raise FileNotFoundError(
            "未找到 A 股缓存目录。请先准备 cache/a_share/*.csv 后再运行。"
        )

    result = bootstrap_project_from_cache(root, source_dir)
    print(result)


if __name__ == "__main__":
    main()
