"""Simple progress bar utility for CLI scripts.

This module provides a lightweight, dependency-free ProgressBar class and a
helper `wrap_iter` generator to wrap existing iterables so scripts can show
progress without pulling in external packages.

API
- ProgressBar(total=None, prefix='', length=40, file=sys.stderr)
    - .update(n=1): advance by n and redraw
    - .set_total(total): set or change total
    - .finish(): draw final bar and newline
    - context-manager support (with ProgressBar(...) as pb:)

- wrap_iter(iterable, **progress_kwargs)
    - yields items from iterable and updates the bar on each iteration

Example:

    from scripts.progress import ProgressBar, wrap_iter

    pb = ProgressBar(total=100, prefix='Processing')
    for item in wrap_iter(my_iterable, progress=pb):
        do_work(item)
    # or use context manager
    with ProgressBar(total=100, prefix='Processing') as pb:
        for item in wrap_iter(my_iterable, progress=pb):
            do_work(item)

This file is intentionally small and portable so it can be imported by the
data-processing scripts in this repository.
"""

from __future__ import annotations

import sys
import time
from typing import Iterable, Iterator, Optional


class ProgressBar:
    """A minimal terminal progress bar.

    It writes to the provided file (defaults to stderr) and redraws the same
    line using carriage return. If total is None, it shows a counter instead
    of percentage.
    """

    def __init__(self, total: Optional[int] = None, prefix: str = "", length: int = 40, file=None):
        self.total = total
        self.prefix = prefix
        self.length = length
        self.file = file or sys.stderr
        self.start = time.time()
        self.count = 0
        self._last_drawn = ""

    def set_total(self, total: Optional[int]) -> None:
        self.total = total

    def update(self, n: int = 1) -> None:
        self.count += n
        self._draw()

    def _draw(self) -> None:
        elapsed = time.time() - self.start
        if self.total:
            frac = min(float(self.count) / float(self.total), 1.0)
            filled_len = int(round(self.length * frac))
            bar = "█" * filled_len + "-" * (self.length - filled_len)
            pct = int(frac * 100)
            s = f"{self.prefix} |{bar}| {pct:3d}% ({self.count}/{self.total}) Elapsed: {int(elapsed)}s"
        else:
            s = f"{self.prefix} {self.count} items Elapsed: {int(elapsed)}s"

        # Only rewrite if changed to reduce terminal noise
        if s != self._last_drawn:
            try:
                print(s, end="\r", file=self.file, flush=True)
            except Exception:
                # Best-effort: ignore terminal write errors
                pass
            self._last_drawn = s

    def finish(self) -> None:
        # draw final state and newline
        self._draw()
        try:
            print(file=self.file)
        except Exception:
            pass

    # Context manager support
    def __enter__(self) -> "ProgressBar":
        self.start = time.time()
        self.count = 0
        self._last_drawn = ""
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.finish()


def wrap_iter(iterable: Iterable, progress: Optional[ProgressBar] = None, total: Optional[int] = None, prefix: str = "") -> Iterator:
    """Yield items from iterable and update the progress bar.

    If a ProgressBar instance is provided via `progress`, it is used. If not,
    a temporary ProgressBar is created using `total` and `prefix`.
    """
    own = False
    pb = progress
    if pb is None:
        pb = ProgressBar(total=total, prefix=prefix)
        own = True

    try:
        for item in iterable:
            yield item
            pb.update(1)
    finally:
        if own:
            pb.finish()
