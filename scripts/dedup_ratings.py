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
import tempfile
import hashlib
import heapq
import shutil
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Tuple, List
from threading import Lock

# Progress bar utility used by multiple scripts
from progress import ProgressBar, wrap_iter


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deduplicate ratings by (userId,movieId) keeping newest by timestamp")
    parser.add_argument("--input", required=True, help="Path to input ratings CSV")
    parser.add_argument("--output", required=True, help="Path to output deduplicated CSV")
    parser.add_argument("--user-col", default="userId", help="Column name for user id (default: userId)")
    parser.add_argument("--item-col", default="movieId", help="Column name for movie/item id (default: movieId)")
    parser.add_argument("--timestamp-col", default="timestamp", help="Column name for timestamp (default: timestamp)")
    parser.add_argument("--rating-col", default="rating", help="Column name for rating value (default: rating)")
    parser.add_argument("--keep-order", action="store_true", help="Write rows in the same order as their newest timestamp occurrences (default: arbitrary)")
    parser.add_argument("--workers", type=int, default=1, help="Number of worker threads to use for partitioned processing (default: 1)")
    parser.add_argument("--byte-chunk", action="store_true", help="Partition input by byte ranges (faster) instead of hashing keys; unsafe if fields contain newlines")
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

        # Wrap the CSV reader so we show progress while scanning rows. Use a
        # context manager to ensure the final bar is drawn.
        with ProgressBar(prefix="Reading") as pbr:
            for row in wrap_iter(reader, progress=pbr):
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
        # Show progress during the write phase; we know the total number of
        # items to write so show a percentage bar.
        with ProgressBar(total=len(items), prefix="Writing") as pbw:
            for _, (ts, row) in wrap_iter(items, progress=pbw):
                writer.writerow(row)

    kept = len(best)
    return kept, total


def _partition_input(inpath: str, n_parts: int, user_col: str, item_col: str) -> Tuple[str, List[str]]:
    """Partition the CSV into n_parts files and return (tmpdir, [paths]).

    Uses md5(user|item) to pick a partition. Shows a progress bar while scanning.
    """
    tmpdir = tempfile.mkdtemp(prefix="dedup_part_")
    part_paths = [os.path.join(tmpdir, f"part_{i}.csv") for i in range(n_parts)]

    outs = [open(p, "w", newline="", encoding="utf-8") for p in part_paths]
    writers: List[csv.DictWriter] = []

    try:
        with open(inpath, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            if reader.fieldnames is None:
                raise ValueError("Input CSV has no header")

            for outfh in outs:
                w = csv.DictWriter(outfh, fieldnames=reader.fieldnames)
                w.writeheader()
                writers.append(w)

            # Use progress bar while partitioning rows
            with ProgressBar(prefix="Partitioning") as pbr:
                for row in wrap_iter(reader, progress=pbr):
                    user = row.get(user_col, "")
                    item = row.get(item_col, "")
                    key = f"{user}|{item}"
                    h = int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16) % n_parts
                    writers[h].writerow(row)

    finally:
        for outfh in outs:
            try:
                outfh.close()
            except Exception:
                pass

    return tmpdir, part_paths


def _partition_by_bytes(inpath: str, n_parts: int, user_col: str, item_col: str, ts_col: str, rating_col: str, keep_order: bool) -> Tuple[str, List[str]]:
    """Partition file into n_parts by byte ranges (header preserved).

    Returns (tmpdir, [part_paths]). Each part file will include the header line.
    This assumes rows are newline-delimited and there are no embedded newlines in fields.
    """
    tmpdir = tempfile.mkdtemp(prefix="dedup_part_")
    part_paths = [os.path.join(tmpdir, f"part_{i}.csv") for i in range(n_parts)]

    filesize = os.path.getsize(inpath)
    with open(inpath, "rb") as fh:
        header = fh.readline()  # bytes including trailing newline
        data_start = fh.tell()
        data_size = filesize - data_start

    if data_size <= 0:
        # empty data - just create empty parts with header
        for p in part_paths:
            with open(p, "wb") as out:
                out.write(header)
        return tmpdir, part_paths

    # compute byte ranges for data (exclude header)
    chunk = (data_size + n_parts - 1) // n_parts
    ranges = []
    for i in range(n_parts):
        start = data_start + i * chunk
        end = min(data_start + (i + 1) * chunk - 1, filesize - 1)
        # ensure valid range
        if start > end:
            start = end + 1
        ranges.append((start, end))
    
    # we'll process each byte-range directly into a per-range deduplicated output
    def range_rows(inpath: str, start: int, end: int, progress_update=None):
        with open(inpath, "rb") as inf:
            inf.seek(start)
            # if not at a line boundary, skip partial line
            if start != data_start:
                # read and discard partial line
                partial = inf.readline()
                if progress_update:
                    progress_update(len(partial))
            # yield header first as text (do not count header towards data progress)
            yield header.decode("utf-8")
            while True:
                pos = inf.tell()
                if pos > end:
                    break
                line = inf.readline()
                if not line:
                    break
                if progress_update:
                    progress_update(len(line))
                yield line.decode("utf-8")

    def _dedup_range_worker(idx: int, start: int, end: int, user_col: str, item_col: str, ts_col: str, rating_col: str, keep_order: bool, progress_update=None) -> str:
        outp = part_paths[idx]
        # build a csv.DictReader over the generator
        gen = range_rows(inpath, start, end, progress_update=progress_update)
        reader = csv.DictReader(gen)
        # perform dedup in-memory for this range
        best: Dict[Tuple[str, str], Tuple[int, dict]] = {}
        total_local = 0
        for row in reader:
            total_local += 1
            user = row.get(user_col, "")
            item = row.get(item_col, "")
            ts = to_int_safe(row.get(ts_col, ""))
            key = (user, item)
            entry = best.get(key)
            if entry is None or ts >= entry[0]:
                best[key] = (ts, row)

        # write out deduped rows for this partition
        os.makedirs(os.path.dirname(outp), exist_ok=True)
        header_fields = list(next(iter(best.values()))[1].keys()) if best else []
        with open(outp, "w", newline="", encoding="utf-8") as outf:
            writer = csv.DictWriter(outf, fieldnames=header_fields)
            writer.writeheader()
            items = list(best.items())
            if keep_order:
                items.sort(key=lambda it: it[1][0])
            for _, (tsv, row) in items:
                writer.writerow(row)

        return outp

    # run dedup workers in parallel reading their byte ranges
    # show aggregated progress by reporting bytes read to a shared ProgressBar
    lock = Lock()
    def make_progress_updater(pb: ProgressBar):
        def updater(n: int) -> None:
            with lock:
                pb.update(n)
        return updater

    with ProgressBar(total=data_size, prefix="Partitioning") as pbr:
        progress_updater = make_progress_updater(pbr)
        with ThreadPoolExecutor(max_workers=min(n_parts, 8)) as ex:
            futures = []
            for idx, (s, e) in enumerate(ranges):
                futures.append(ex.submit(_dedup_range_worker, idx, s, e, user_col, item_col, ts_col, rating_col, keep_order, progress_updater))
            for f in futures:
                f.result()

    return tmpdir, part_paths


def _dedup_worker(in_out: Tuple[str, str, str, str, str, str, bool]) -> str:
    """Run dedup_ratings on a single partition.

    in_out: (inpath,outpath,user_col,item_col,ts_col,rating_col,keep_order)
    Returns outpath on success.
    """
    inpath, outpath, user_col, item_col, ts_col, rating_col, keep_order = in_out
    # reuse the existing dedup implementation for the partition
    dedup_ratings(inpath, outpath, user_col, item_col, ts_col, rating_col, keep_order)
    return outpath


def _merge_partitions_sorted(out_path: str, part_out_paths: List[str], ts_col: str, header: List[str], total_rows: int | None = None) -> None:
    """K-way merge rows from partition outputs by timestamp into out_path.

    Uses a generator and wrap_iter with ProgressBar to render progress.
    """
    readers = []
    try:
        for p in part_out_paths:
            fh = open(p, newline="", encoding="utf-8")
            rdr = csv.DictReader(fh)
            readers.append((fh, rdr))

        # initialize heap
        heap: List[tuple[int, int, dict]] = []
        for idx, (fh, rdr) in enumerate(readers):
            try:
                row = next(rdr)
            except StopIteration:
                continue
            ts = to_int_safe(row.get(ts_col, ""))
            heapq.heappush(heap, (ts, idx, row))

        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        with open(out_path, "w", newline="", encoding="utf-8") as outfh:
            writer = csv.DictWriter(outfh, fieldnames=header)
            writer.writeheader()

            def gen_rows():
                # yields rows in sorted order by timestamp
                while heap:
                    ts, idx, row = heapq.heappop(heap)
                    yield row
                    fh, rdr = readers[idx]
                    try:
                        next_row = next(rdr)
                    except StopIteration:
                        continue
                    next_ts = to_int_safe(next_row.get(ts_col, ""))
                    heapq.heappush(heap, (next_ts, idx, next_row))

            # show progress if total_rows known
            if total_rows:
                with ProgressBar(total=total_rows, prefix="Merging") as pbr:
                    for row in wrap_iter(gen_rows(), progress=pbr):
                        writer.writerow(row)
            else:
                for row in gen_rows():
                    writer.writerow(row)

    finally:
        for fh, _ in readers:
            try:
                fh.close()
            except Exception:
                pass


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    try:
        workers = max(1, int(getattr(args, "workers", 1)))

        if workers == 1:
            # Single-threaded path (original behavior)
            kept, total = dedup_ratings(
                args.input,
                args.output,
                args.user_col,
                args.item_col,
                args.timestamp_col,
                args.rating_col,
                args.keep_order,
            )

        else:
            # Partition input, run per-partition dedup in threads, then merge
            if getattr(args, "byte_chunk", False):
                tmpdir, part_paths = _partition_by_bytes(args.input, workers, args.user_col, args.item_col, args.timestamp_col, args.rating_col, args.keep_order)
            else:
                tmpdir, part_paths = _partition_input(args.input, workers, args.user_col, args.item_col)
            part_outs = [os.path.join(tmpdir, f"part_out_{i}.csv") for i in range(len(part_paths))]

            worker_inputs = []
            for inpath, outpath in zip(part_paths, part_outs):
                worker_inputs.append((inpath, outpath, args.user_col, args.item_col, args.timestamp_col, args.rating_col, args.keep_order))

            # run workers using threads
            with ThreadPoolExecutor(max_workers=workers) as ex:
                futures = [ex.submit(_dedup_worker, wi) for wi in worker_inputs]
                # wait for completion and raise on errors
                for f in futures:
                    _ = f.result()

            # compute totals: kept is sum of deduped rows across partition outputs
            kept = 0
            for out in part_outs:
                with open(out, newline="", encoding="utf-8") as fh:
                    rdr = csv.reader(fh)
                    cnt = sum(1 for _ in rdr) - 1
                    kept += max(0, cnt)

            # total: compute from the original input file (exclude header)
            with open(args.input, newline="", encoding="utf-8") as fh:
                total = sum(1 for _ in fh) - 1

            # build header from first partition output (if any)
            header: List[str] = []
            if part_outs and os.path.exists(part_outs[0]):
                with open(part_outs[0], newline="", encoding="utf-8") as fh:
                    r = csv.DictReader(fh)
                    header = list(r.fieldnames) if r.fieldnames is not None else []

            # merge
            if args.keep_order:
                _merge_partitions_sorted(args.output, part_outs, args.timestamp_col, header, total_rows=kept)
            else:
                os.makedirs(os.path.dirname(args.output), exist_ok=True)
                with open(args.output, "w", newline="", encoding="utf-8") as outfh:
                    writer = csv.DictWriter(outfh, fieldnames=header)
                    writer.writeheader()
                    for p in part_outs:
                        with open(p, newline="", encoding="utf-8") as fh:
                            rdr = csv.DictReader(fh)
                            for row in rdr:
                                writer.writerow(row)

            # cleanup
            try:
                shutil.rmtree(tmpdir)
            except Exception:
                pass

        print(f"Processed {total} rows; kept {kept} unique (userId,movieId) pairs")
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
