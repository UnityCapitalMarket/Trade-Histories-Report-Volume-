#!/usr/bin/env python3
# filepath: tools/filter_trades_csv.py
"""
Filter CSV trade records by rules:
- Remove any row where magic == 0.
- Remove any row where comment == "cancelled" or "canceled" (case-insensitive, trims leading/trailing whitespace).

Designed for large datasets (>= 20k rows) by reading in chunks.

Example:
    python filter_trades_csv.py input.csv output.csv
    # Or filter in-place (safe overwrite using a temporary file):
    python filter_trades_csv.py input.csv --inplace

Suggested columns: id, trade_account_id, ticket, symbol_name, digits, type, quantity, state,
open_time, open_price, open_rate, close_time, close_price, close_rate, stop_loss,
take_profit, expiration, commission, commission_agent, swap, profit, tax, magic,
comment, timestamp, is_closed
"""
from __future__ import annotations

import argparse
import os
import sys
import tempfile
from pathlib import Path
from typing import Optional

import pandas as pd


CANCELLED_TOKENS = {"cancelled", "canceled"}  # UK + US spelling


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Filter CSV: drop rows where magic == 0 or comment equals 'cancelled'/'canceled' (case-insensitive).",
    )
    parser.add_argument("input", type=Path, help="Input CSV file path")
    parser.add_argument(
        "output",
        type=Path,
        nargs="?",
        help="Output CSV file path (leave blank if using --inplace)",
    )
    parser.add_argument(
        "--inplace",
        action="store_true",
        help="Overwrite input file with filtered result (uses a temporary file for safety)",
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=100_000,
        help="Number of rows to read per chunk (default: 100000)",
    )
    parser.add_argument(
        "--encoding",
        type=str,
        default="utf-8",
        help="CSV file encoding (default: utf-8)",
    )
    parser.add_argument(
        "--sep",
        type=str,
        default=",",
        help="Column separator character (default: ,)",
    )
    parser.add_argument(
        "--na-values",
        dest="na_values",
        type=str,
        default=None,
        help="List of values to treat as NA, separated by | (e.g. 'NA|NaN|')",
    )
    return parser


def _normalize_comment(series: pd.Series) -> pd.Series:
    # Only normalize if column exists; otherwise, return empty string so it doesn't match
    s = series.astype("string").fillna("").str.strip().str.lower()
    return s


def _magic_is_zero(series: pd.Series) -> pd.Series:
    # NaN is not removed (comparison with 0 will be False)
    mag = pd.to_numeric(series, errors="coerce")
    return mag.eq(0)


def filter_chunk(df: pd.DataFrame) -> pd.DataFrame:
    # Protect against missing columns
    if "magic" not in df.columns:
        raise KeyError("Missing 'magic' column in CSV")
    if "comment" not in df.columns:
        # If 'comment' is missing, only apply magic == 0 condition
        magic_zero = _magic_is_zero(df["magic"]).fillna(False)
        return df.loc[~magic_zero]

    magic_zero = _magic_is_zero(df["magic"]).fillna(False)
    comments = _normalize_comment(df["comment"])  # case-insensitive
    cancelled = comments.isin(CANCELLED_TOKENS)

    mask_drop = magic_zero | cancelled
    return df.loc[~mask_drop]


def filter_csv(
    input_path: Path,
    output_path: Optional[Path] = None,
    *,
    chunksize: int = 100_000,
    encoding: str = "utf-8",
    sep: str = ",",
    na_values: Optional[str] = None,
) -> Path:
    if not input_path.exists():
        raise FileNotFoundError(f"File not found: {input_path}")

    if output_path is None:
        # If not specified, create file next to input: <name>.filtered.csv
        output_path = input_path.with_suffix("")
        output_path = output_path.with_name(output_path.name + ".filtered.csv")

    na_vals = None
    if na_values:
        na_vals = [tok for tok in na_values.split("|") if tok is not None]

    total_in = 0
    total_out = 0
    wrote_header = False

    # Open for new write
    if output_path.exists():
        output_path.unlink()

    try:
        reader = pd.read_csv(
            input_path,
            chunksize=chunksize,
            encoding=encoding,
            sep=sep,
            na_values=na_vals,
            low_memory=False,
        )
    except Exception as exc:
        raise RuntimeError(
            "Error reading CSV. Try adjusting --encoding or --sep."
        ) from exc

    for chunk in reader:
        total_in += len(chunk)
        try:
            out = filter_chunk(chunk)
        except KeyError as ke:
            # Error due to missing required column
            raise
        total_out += len(out)
        # Append to output
        out.to_csv(output_path, index=False, mode="a", header=not wrote_header)
        wrote_header = True

    print(
        f"Read {total_in} rows, wrote {total_out} rows -> {output_path}",
        file=sys.stderr,
    )
    return output_path


def inplace_filter(
    input_path: Path,
    *,
    chunksize: int,
    encoding: str,
    sep: str,
    na_values: Optional[str],
) -> Path:
    # Safely write via temporary file then replace
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_out = Path(tmpdir) / (input_path.stem + ".filtered.csv")
        out = filter_csv(
            input_path,
            tmp_out,
            chunksize=chunksize,
            encoding=encoding,
            sep=sep,
            na_values=na_values,
        )
        # Atomically replace if possible
        os.replace(out, input_path)
        print(f"Overwritten: {input_path}", file=sys.stderr)
        return input_path


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if args.inplace and args.output:
        print("Cannot use --inplace and output path at the same time.", file=sys.stderr)
        return 2
    if not args.inplace and not args.output:
        # Allow blank output -> auto-generate <name>.filtered.csv
        pass

    try:
        if args.inplace:
            inplace_filter(
                args.input,
                chunksize=args.chunksize,
                encoding=args.encoding,
                sep=args.sep,
                na_values=args.na_values,
            )
        else:
            out = filter_csv(
                args.input,
                args.output,
                chunksize=args.chunksize,
                encoding=args.encoding,
                sep=args.sep,
                na_values=args.na_values,
            )
            print(out)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
