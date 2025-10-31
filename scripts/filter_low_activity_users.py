"""filter_low_activity_users.py

Remove ratings from users who have fewer than a configurable threshold of
ratings. Default threshold is 10.

Two-pass approach:
 - Pass 1: count ratings per userId (memory: one counter per unique user)
 - Pass 2: write rows where user's count >= threshold

Usage:
    python scripts/filter_low_activity_users.py \
        --input movie-engine-data/raw/ml-100k/ratings.csv \
        --output movie-engine-data/processed/ml-100k/ratings_active_users.csv \
        --threshold 10

Options:
 - --user-col: name of user id column (default: userId)
 - --threshold: min number of ratings to keep (default: 10)
 - --keep-order: preserve original file order in output (default: True)

"""

from __future__ import annotations

import argparse
import collections
import csv
import os
import sys
from typing import Dict


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Filter out ratings from low-activity users")
    p.add_argument("--input", required=True, help="Path to input ratings CSV")
    p.add_argument("--output", required=True, help="Path to write filtered CSV")
    p.add_argument("--user-col", default="userId", help="Column name for user id (default: userId)")
    p.add_argument("--threshold", type=int, default=30, help="Minimum number of ratings required to keep a user's ratings (default: 30)")
    p.add_argument("--keep-order", action="store_true", help="Preserve original order in output (default: write in streaming order)")
    return p.parse_args(argv)


def count_users(inpath: str, user_col: str) -> Dict[str, int]:
    counts: Dict[str, int] = collections.Counter()
    with open(inpath, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames is None:
            raise ValueError("Input CSV has no header")
        if user_col not in reader.fieldnames:
            raise ValueError(f"user column '{user_col}' not found in header: {reader.fieldnames}")
        for row in reader:
            uid = row.get(user_col, "")
            counts[uid] += 1
    return counts


def filter_users(inpath: str, outpath: str, user_col: str, threshold: int, keep_order: bool) -> int:
    counts = count_users(inpath, user_col)
    keep_users = {u for u, c in counts.items() if c >= threshold}

    os.makedirs(os.path.dirname(outpath), exist_ok=True)

    kept = 0
    total = 0
    with open(inpath, newline="", encoding="utf-8") as infh, open(outpath, "w", newline="", encoding="utf-8") as outfh:
        reader = csv.DictReader(infh)
        fieldnames = reader.fieldnames or []
        writer = csv.DictWriter(outfh, fieldnames=fieldnames)
        writer.writeheader()

        if keep_order:
            # streaming write preserves order naturally
            for row in reader:
                total += 1
                if row.get(user_col, "") in keep_users:
                    writer.writerow(row)
                    kept += 1
        else:
            # If not keeping order we still stream; kept users are same
            for row in reader:
                total += 1
                if row.get(user_col, "") in keep_users:
                    writer.writerow(row)
                    kept += 1

    print(f"Total rows: {total}; rows kept: {kept}; users kept: {len(keep_users)}")
    return kept


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.threshold < 1:
        print("--threshold must be >= 1", file=sys.stderr)
        return 2
    try:
        kept = filter_users(args.input, args.output, args.user_col, args.threshold, args.keep_order)
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
