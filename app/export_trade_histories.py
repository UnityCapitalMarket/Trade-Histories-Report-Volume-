# file: app/trade_fetcher.py
"""
PSEUDOCODE PLAN (ENGLISH):
1) Convert BigIntHumanReadable <-> datetime UTC (length 14/17; sentinel 0/19700101000000000 -> None).
2) Dataclass TradeRecord maps 1-to-1 with the TradeHistories table; add is_closed.
3) Two query modes:
   a) Filter mode (CLI parameters) same as previous version.
   b) RAW SQL mode: accepts "SELECT ..." from --sql or --sql-file, must return all columns as per layout, parse & export CSV.
   - Block non-SELECT statements.
   - Check for all required columns; report missing if not matched.
4) Export standard UTF-8 CSV, datetime -> ISO8601Z; or JSON lines.
5) CLI: connect to MySQL from args/env; choose one of two modes; example commands at end of file.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import mysql.connector  # pip install mysql-connector-python


# ----------------------------
# Datetime conversion helpers
# ----------------------------

def _is_epoch_sentinel(val: Optional[int | str]) -> bool:
    if val is None:
        return True
    s = str(val).strip()
    return s in {"0", "19700101000000000"}


def bigint_hr_to_datetime(value: Optional[int | str]) -> Optional[datetime]:
    if value is None:
        return None
    s = str(value).strip()
    if not s or _is_epoch_sentinel(s):
        return None
    if not s.isdigit():
        raise ValueError(f"Invalid BigIntHumanReadable value: {value}")
    if len(s) == 14:
        fmt = "%Y%m%d%H%M%S"
        dt = datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        return dt
    if len(s) == 17:
        base, ms = s[:14], s[14:]
        dt = datetime.strptime(base, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
        dt = dt.replace(microsecond=int(ms) * 1000)
        return dt
    raise ValueError(
        f"Unsupported length for BigIntHumanReadable (expect 14 or 17): {value}"
    )


def datetime_to_bigint_hr(dt: Optional[datetime]) -> Optional[int]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        aware = dt.replace(tzinfo=timezone.utc)
    else:
        aware = dt.astimezone(timezone.utc)
    ms = int(round(aware.microsecond / 1000.0))
    if ms == 1000:
        aware = aware.replace(microsecond=0)
        ms = 0
    return int(aware.strftime("%Y%m%d%H%M%S") + f"{ms:03d}")


# ----------------------------
# Data models
# ----------------------------

@dataclass
class TradeRecord:
    id: int
    trade_account_id: Optional[int]
    ticket: Optional[int]
    symbol_name: Optional[str]
    digits: Optional[int]
    type: Optional[int]
    quantity: Optional[float]
    state: Optional[int]
    open_time: Optional[datetime]
    open_price: Optional[float]
    open_rate: Optional[float]
    close_time: Optional[datetime]
    close_price: Optional[float]
    close_rate: Optional[float]
    stop_loss: Optional[float]
    take_profit: Optional[float]
    expiration: Optional[datetime]
    commission: Optional[float]
    commission_agent: Optional[float]
    swap: Optional[float]
    profit: Optional[float]
    tax: Optional[float]
    magic: Optional[int]
    comment: Optional[str]
    timestamp: Optional[datetime]

    @property
    def is_closed(self) -> bool:
        if self.open_time is None or self.close_time is None:
            return False
        return self.close_time >= self.open_time

    def to_serializable(self) -> Dict[str, Any]:
        d = asdict(self)
        for k in ("open_time", "close_time", "expiration", "timestamp"):
            v = d.get(k)
            d[k] = v.isoformat().replace("+00:00", "Z") if isinstance(v, datetime) else None
        d["is_closed"] = self.is_closed
        return d


@dataclass
class TradeQuery:
    trade_account_id: Optional[int] = None
    ticket: Optional[int] = None
    symbol: Optional[str] = None
    opened_from: Optional[datetime] = None
    opened_to: Optional[datetime] = None
    closed_from: Optional[datetime] = None
    closed_to: Optional[datetime] = None
    comment_like: Optional[str] = None
    limit: int = 100
    offset: int = 0
    order_by: str = "OpenTime"
    order_dir: str = "ASC"

    def validate(self) -> None:
        allowed_cols = {"ID", "OpenTime", "CloseTime", "TimeStamp", "Ticket"}
        if self.order_by not in allowed_cols:
            raise ValueError(f"order_by must be one of {sorted(allowed_cols)}")
        if self.order_dir not in {"ASC", "DESC"}:
            raise ValueError("order_dir must be 'ASC' or 'DESC'")
        if self.limit <= 0 or self.limit > 10000:
            raise ValueError("limit must be between 1 and 10000")
        if self.offset < 0:
            raise ValueError("offset must be >= 0")


# ----------------------------
# MySQL access
# ----------------------------

COLUMNS: Tuple[str, ...] = (
    "ID",
    "TradeAccountID",
    "Ticket",
    "SymbolName",
    "Digits",
    "Type",
    "Quantity",
    "State",
    "OpenTime",
    "OpenPrice",
    "OpenRate",
    "CloseTime",
    "ClosePrice",
    "CloseRate",
    "StopLoss",
    "TakeProfit",
    "Expiration",
    "Commission",
    "CommissionAgent",
    "Swap",
    "Profit",
    "Tax",
    "Magic",
    "Comment",
    "TimeStamp",
)

REQUIRED_SET = set(COLUMNS)


def build_where_and_params(q: TradeQuery) -> Tuple[str, List[Any]]:
    q.validate()
    clauses: List[str] = []
    params: List[Any] = []
    if q.trade_account_id is not None:
        clauses.append("TradeAccountID = %s")
        params.append(q.trade_account_id)
    if q.ticket is not None:
        clauses.append("Ticket = %s")
        params.append(q.ticket)
    if q.symbol:
        clauses.append("SymbolName = %s")
        params.append(q.symbol)
    if q.opened_from is not None:
        clauses.append("OpenTime >= %s")
        params.append(datetime_to_bigint_hr(q.opened_from))
    if q.opened_to is not None:
        clauses.append("OpenTime <= %s")
        params.append(datetime_to_bigint_hr(q.opened_to))
    if q.closed_from is not None:
        clauses.append("CloseTime >= %s")
        params.append(datetime_to_bigint_hr(q.closed_from))
    if q.closed_to is not None:
        clauses.append("CloseTime <= %s")
        params.append(datetime_to_bigint_hr(q.closed_to))
    if q.comment_like:
        clauses.append("Comment LIKE %s")
        params.append(f"%{q.comment_like}%")
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    return where, params


def row_to_trade_record(row: Dict[str, Any]) -> TradeRecord:
    return TradeRecord(
        id=int(row["ID"]),
        trade_account_id=_opt_int(row.get("TradeAccountID")),
        ticket=_opt_int(row.get("Ticket")),
        symbol_name=_opt_str(row.get("SymbolName")),
        digits=_opt_int(row.get("Digits")),
        type=_opt_int(row.get("Type")),
        quantity=_opt_float(row.get("Quantity")),
        state=_opt_int(row.get("State")),
        open_time=bigint_hr_to_datetime(row.get("OpenTime")),
        open_price=_opt_float(row.get("OpenPrice")),
        open_rate=_opt_float(row.get("OpenRate")),
        close_time=bigint_hr_to_datetime(row.get("CloseTime")),
        close_price=_opt_float(row.get("ClosePrice")),
        close_rate=_opt_float(row.get("CloseRate")),
        stop_loss=_opt_float(row.get("StopLoss")),
        take_profit=_opt_float(row.get("TakeProfit")),
        expiration=bigint_hr_to_datetime(row.get("Expiration")),
        commission=_opt_float(row.get("Commission")),
        commission_agent=_opt_float(row.get("CommissionAgent")),
        swap=_opt_float(row.get("Swap")),
        profit=_opt_float(row.get("Profit")),
        tax=_opt_float(row.get("Tax")),
        magic=_opt_int(row.get("Magic")),
        comment=_opt_str(row.get("Comment")),
        timestamp=bigint_hr_to_datetime(row.get("TimeStamp")),
    )


def _opt_int(v: Any) -> Optional[int]:
    return None if v is None else int(v)


def _opt_float(v: Any) -> Optional[float]:
    return None if v is None else float(v)


def _opt_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v)
    return s if s != "" else None


def fetch_trades(conn_params: Dict[str, Any], query: TradeQuery) -> List[TradeRecord]:
    columns_sql = ", ".join(COLUMNS)
    where_sql, params = build_where_and_params(query)
    sql = (
        f"SELECT {columns_sql} FROM TradeHistories" + where_sql +
        f" ORDER BY {query.order_by} {query.order_dir} LIMIT %s OFFSET %s"
    )
    params.extend([query.limit, query.offset])
    cnx = mysql.connector.connect(**conn_params)
    try:
        cur = cnx.cursor(dictionary=True)
        cur.execute(sql, params)
        return [row_to_trade_record(r) for r in cur.fetchall()]
    finally:
        cnx.close()


# ----------------------------
# RAW SQL mode
# ----------------------------

def _strip_leading_comments(sql: str) -> str:
    s = sql.lstrip()
    # Remove leading /* ... */ blocks and -- line comments at top only
    while s.startswith("/*"):
        end = s.find("*/")
        if end == -1:
            break
        s = s[end + 2 :].lstrip()
    while s.startswith("--"):
        nl = s.find("\n")
        s = (s[nl + 1 :] if nl != -1 else "").lstrip()
    return s


def is_select_only(sql: str) -> bool:
    s = _strip_leading_comments(sql)
    return s[:6].lower() == "select"


def fetch_trades_by_sql(conn_params: Dict[str, Any], sql: str) -> List[TradeRecord]:
    if not is_select_only(sql):
        raise ValueError("Chỉ cho phép lệnh SELECT trong --sql/--sql-file")
    cnx = mysql.connector.connect(**conn_params)
    try:
        cur = cnx.cursor(dictionary=True)
        cur.execute(sql)
        rows = cur.fetchall()
        if not rows:
            return []
        cols = {desc[0] for desc in cur.description}  # type: ignore[index]
        missing = REQUIRED_SET - cols
        if missing:
            raise ValueError(
                "Thiếu cột bắt buộc trong kết quả SELECT: " + ", ".join(sorted(missing))
            )
        return [row_to_trade_record(r) for r in rows]  # type: ignore[arg-type]
    finally:
        cnx.close()


# ----------------------------
# Output helpers
# ----------------------------

def write_csv(path: str, records: List[TradeRecord]) -> None:
    if not records:
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(TradeRecord.__annotations__.keys()) + ["is_closed"])
            writer.writeheader()
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        fieldnames = list(records[0].to_serializable().keys())
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for rec in records:
            writer.writerow(rec.to_serializable())


def write_jsonl(records: List[TradeRecord]) -> None:
    for rec in records:
        print(json.dumps(rec.to_serializable(), ensure_ascii=False))


# ----------------------------
# CLI
# ----------------------------


def _env_or_default(key: str, default: Optional[str] = None) -> Optional[str]:
    return os.getenv(key, default)


def parse_iso8601_z(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except Exception as exc:
        raise argparse.ArgumentTypeError(f"Invalid datetime: {exc}")


def main(argv: Optional[Iterable[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Fetch TradeHistories -> CSV (filter mode hoặc raw SQL mode).")

    # Connection
    parser.add_argument("--host", default=_env_or_default("DB_HOST", "localhost"))
    parser.add_argument("--port", type=int, default=int(_env_or_default("DB_PORT", "3306")))
    parser.add_argument("--user", default=_env_or_default("DB_USER", "root"))
    parser.add_argument("--password", default=_env_or_default("DB_PASSWORD"))
    parser.add_argument("--database", default=_env_or_default("DB_NAME"))

    # Mode selection
    parser.add_argument("--sql", help="Câu SELECT thô. BẮT BUỘC phải trả về đầy đủ cột theo layout.")
    parser.add_argument("--sql-file", help="Đường dẫn file .sql chứa câu SELECT.")

    # Filters (only used when --sql/--sql-file không có)
    parser.add_argument("--account-id", type=int)
    parser.add_argument("--ticket", type=int)
    parser.add_argument("--symbol")
    parser.add_argument("--opened-from", type=parse_iso8601_z)
    parser.add_argument("--opened-to", type=parse_iso8601_z)
    parser.add_argument("--closed-from", type=parse_iso8601_z)
    parser.add_argument("--closed-to", type=parse_iso8601_z)
    parser.add_argument("--comment-like")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--order-by", default="OpenTime")
    parser.add_argument("--order-dir", default="ASC", choices=["ASC", "DESC"])

    # Output
    parser.add_argument("--csv-out", required=False, help="Đường dẫn file CSV output")
    parser.add_argument("--jsonl", action="store_true", help="In JSON Lines ra stdout thay vì CSV")

    args = parser.parse_args(list(argv) if argv is not None else None)

    conn_params = {
        "host": args.host,
        "port": args.port,
        "user": args.user,
        "password": args.password,
        "database": args.database,
        "autocommit": True,
    }

    # Determine mode
    sql_text: Optional[str] = None
    if args.sql_file:
        with open(args.sql_file, "r", encoding="utf-8") as f:
            sql_text = f.read()
    elif args.sql:
        sql_text = args.sql

    if sql_text:
        records = fetch_trades_by_sql(conn_params, sql_text)
    else:
        query = TradeQuery(
            trade_account_id=args.account_id,
            ticket=args.ticket,
            symbol=args.symbol,
            opened_from=args.opened_from,
            opened_to=args.opened_to,
            closed_from=args.closed_from,
            closed_to=args.closed_to,
            comment_like=args.comment_like,
            limit=args.limit,
            offset=args.offset,
            order_by=args.order_by,
            order_dir=args.order_dir,
        )
        records = fetch_trades(conn_params, query)

    if args.jsonl or not args.csv_out:
        write_jsonl(records)
    if args.csv_out:
        write_csv(args.csv_out, records)


# ----------------------------
# Sample row for quick self-check
# ----------------------------

SAMPLE_ROW = (
    '9209', '111', '3599795', 'EURUSD', '5', '0', '0', '5', '20230209084334000', '1.07351',
    '1.07351', '20230209090257000', '1.07351', '0', '0', '0', '19700101000000000', '0', '0', '0',
    '0', '0', '3599793', 'close hedge by #3599791', '20230209090257000'
)

SAMPLE_DICT = {col: SAMPLE_ROW[i] for i, col in enumerate(COLUMNS)}

def _demo_parse_sample() -> TradeRecord:
    return row_to_trade_record(SAMPLE_DICT)


if __name__ == "__main__":
    # RAW SQL mode (khuyến nghị):
    # python app/trade_fetcher.py --host 127.0.0.1 --user root --password 123 --database mydb \
    #   --sql "SELECT ID, TradeAccountID, Ticket, SymbolName, Digits, Type, Quantity, State, OpenTime, OpenPrice, OpenRate, CloseTime, ClosePrice, CloseRate, StopLoss, TakeProfit, Expiration, Commission, CommissionAgent, Swap, Profit, Tax, Magic, Comment, TimeStamp FROM TradeHistories WHERE TradeAccountID=111" \
    #   --csv-out trades.csv
    #
    # Filter mode (không cần tự viết SQL):
    # python app/trade_fetcher.py --host 127.0.0.1 --user root --password 123 --database mydb \
    #   --account-id 111 --symbol EURUSD --opened-from 2023-02-09T00:00:00Z --opened-to 2023-02-10T00:00:00Z \
    #   --limit 50 --order-by OpenTime --order-dir ASC --csv-out trades.csv
    main()
