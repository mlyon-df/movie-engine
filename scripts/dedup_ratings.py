"""dedup_ratings.py

Read a MovieLens-style ratings CSV and remove duplicate (userId,movieId)
pairs, keeping only the newest rating according to the timestamp column.

This script keeps the header and writes the deduplicated rows to an
output CSV. It performs a single-pass keeping the best per key in memory.

Usage:
    python scripts/dedup_ratings.py \
        --input movie-engine-data/raw/ml-100k/ratings.csv \
        --output movie-engine-data/processed/ml-100k/ratings_dedup.csv

Options:
 - --user-col: name of user id column (default: userId)
 - --item-col: name of item/movie id column (default: movieId)
 - --timestamp-col: name of timestamp column (default: timestamp)
 - --rating-col: name of rating value column (default: rating)
 - --keep-order: write rows in the same order as their newest timestamp occurrences (default: arbitrary) 
 
Notes:
- For very large files that don't fit in memory, consider external sort
  by (userId,movieId) and then keeping the last entry per group.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from typing import Dict, Tuple


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deduplicate ratings by (userId,movieId) keeping newest by timestamp")
    parser.add_argument("--input", required=True, help="Path to input ratings CSV")
    parser.add_argument("--output", required=True, help="Path to output deduplicated CSV")
    parser.add_argument("--user-col", default="userId", help="Column name for user id (default: userId)")
    parser.add_argument("--item-col", default="movieId", help="Column name for movie/item id (default: movieId)")
    parser.add_argument("--timestamp-col", default="timestamp", help="Column name for timestamp (default: timestamp)")
    parser.add_argument("--rating-col", default="rating", help="Column name for rating value (default: rating)")
    parser.add_argument("--keep-order", action="store_true", help="Write rows in the same order as their newest timestamp occurrences (default: arbitrary)")
    return parser.parse_args(argv)


def to_int_safe(value: str) -> int:
    """Try to convert a timestamp-like string to int; on failure, return 0.

    We return 0 as a fallback so that missing/garbled timestamps are treated as
    very old.
    """
    try:
        return int(value)
    except Exception:
        try:
            # maybe a float-like value
            return int(float(value))
        except Exception:
            # Log the bad value and return 0
            sys.stderr.write(f"Warning: could not convert timestamp value '{value}' to int; using 0\n")
            return 0


def dedup_ratings(inpath: str, outpath: str, user_col: str, item_col: str, ts_col: str, rating_col: str, keep_order: bool = False) -> Tuple[int, int]:
    """Return (kept_count, total_rows).

    This reads the whole input and keeps in memory a mapping from (user,item)
    to (timestamp, row_dict). If a duplicate is found with a newer timestamp,
    it replaces the stored row.
    """
    if not os.path.exists(inpath):
        sys.stderr.write(f"Error: input file does not exist: {inpath}\n")
        raise FileNotFoundError(inpath)

    best: Dict[Tuple[str, str], Tuple[int, dict]] = {}
    total = 0

    with open(inpath, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames is None:
            raise ValueError("Input CSV has no header")

        # It's not strictly necessary to check for the rating column, but we do it because there's no point to the exercise if there's no rating data.
        if user_col not in reader.fieldnames or item_col not in reader.fieldnames or ts_col not in reader.fieldnames or rating_col not in reader.fieldnames:
            raise ValueError(f"Missing expected columns. Available: {reader.fieldnames}")

        for row in reader:
            total += 1
            user = row.get(user_col, "")
            item = row.get(item_col, "")
            ts_raw = row.get(ts_col, "")
            ts = to_int_safe(ts_raw)

            key = (user, item)
            entry = best.get(key)
            if entry is None:
                best[key] = (ts, row)
            else:
                existing_ts, _ = entry
                # keep the row with the greater (newer) timestamp
                if ts >= existing_ts:
                    best[key] = (ts, row)

    # prepare to write
    os.makedirs(os.path.dirname(outpath), exist_ok=True)
    # choose the header from the original input; they are the same across rows
    header = None
    # best.values() is (ts,row)
    if best:
        header = list(next(iter(best.values()))[1].keys())
    else:
        # no rows -> try to read header from input
        with open(inpath, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            header = reader.fieldnames or []

    # Optionally order rows by their stored timestamp (newest first) or
    # arbitrary (dictionary order). If keep_order is True, order by timestamp
    # ascending of newest occurrence so that final file is reproducible.
    items = list(best.items())  # list of ((user,item),(ts,row))
    if keep_order:
        items.sort(key=lambda it: it[1][0])

    with open(outpath, "w", newline="", encoding="utf-8") as outfh:
        writer = csv.DictWriter(outfh, fieldnames=header)
        writer.writeheader()
        for _, (ts, row) in items:
            writer.writerow(row)

    kept = len(best)
    return kept, total


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    try:
        kept, total = dedup_ratings(args.input, args.output, args.user_col, args.item_col, args.rating_col, args.timestamp_col, args.keep_order)
        print(f"Processed {total} rows; kept {kept} unique (userId,movieId) pairs")
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
