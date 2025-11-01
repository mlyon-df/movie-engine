"""onehot_movies.py

Read MovieLens movies.csv, one-hot-encode the `genres` column, and write a new CSV.

Behavior:
- Writes the output with one-hot columns based on a pre-defined list of genres.
- Keeps all original non-genre columns (e.g., movieId, title).
- Writes 1/0 as integer flags for genre membership.

Usage:
    python scripts/onehot_movies.py \
        --input movie-engine-data/raw/ml-100k/movies.csv \
        --output movie-engine-data/processed/ml-100k/movies_onehot.csv

Options:
 - --sort-genres: sort genre columns alphabetically (default: use predefined order)
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from typing import List
from progress import ProgressBar, wrap_iter


# MovieLens predefined genre list. Keep the exact strings so CSV
# column headers match the requested names (including the "(no genres listed)").
# This must be changed if the dataset uses different genre names.
GENRE_LIST: List[str] = [
    "Action",
    "Adventure",
    "Animation",
    "Children",
    "Comedy",
    "Crime",
    "Documentary",
    "Drama",
    "Fantasy",
    "Film-Noir",
    "Horror",
    "Musical",
    "Mystery",
    "Romance",
    "Sci-Fi",
    "Thriller",
    "War",
    "Western",
    "(no genres listed)",
]


def write_onehot(input_path: str, output_path: str, genre_list: List[str]) -> int:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(input_path, newline="", encoding="utf-8") as infh, \
         open(output_path, "w", newline="", encoding="utf-8") as outfh:
        reader = csv.DictReader(infh)
        if not reader.fieldnames:
            raise ValueError("Input CSV has no header")

        other_fields = [f for f in reader.fieldnames if f != "genres"]
        fieldnames = other_fields + genre_list
        writer = csv.DictWriter(outfh, fieldnames=fieldnames)
        writer.writeheader()

        row_count = 0
        # Show progress while processing rows
        with ProgressBar(prefix="Processing") as pb:
            for row in wrap_iter(reader, progress=pb):
                out_row = {k: row.get(k, "") for k in other_fields}
                raw = row.get("genres", "")
                present = set(g.strip() for g in raw.split("|") if g.strip())
                # If the dataset explicitly uses the placeholder, treat that as the
                # only genre present so the corresponding column will be 1 and
                # others 0. Otherwise keep the parsed genres as-is.
                lowered = {g.lower() for g in present}
                if "(no genres listed)" in lowered:
                    present = {"(no genres listed)"}

                for g in genre_list:
                    out_row[g] = "1" if g in present else "0"

                writer.writerow(out_row)
                row_count += 1

    return row_count


def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="One-hot encode genres in MovieLens movies.csv")
    p.add_argument("--input", required=True, help="Path to raw movies.csv")
    p.add_argument("--output", required=True, help="Path to write processed CSV")
    p.add_argument("--sort-genres", action="store_true", help="Sort genre columns alphabetically")
    return p.parse_args(argv)


def main(argv: List[str] | None = None) -> int:
    args = parse_args(argv)
    input_path = args.input
    output_path = args.output

    if not os.path.exists(input_path):
        print(f"Input file does not exist: {input_path}", file=sys.stderr)
        return 2

    # Use the predefined genre list provided by the user. Optionally sort the
    # columns if requested (this will reorder the user-provided list).
    if args.sort_genres:
        genre_list = sorted(GENRE_LIST)
    else:
        genre_list = GENRE_LIST

    print(f"Using {len(genre_list)} genres")
    written = write_onehot(input_path, output_path, genre_list)
    print(f"Wrote {written} rows to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
