#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable, List

import diskcache as dc


def _default_cache_dirs() -> List[Path]:
    service_root = Path(__file__).resolve().parents[1]
    repo_root = service_root.parent
    return [
        repo_root / ".diskcache_pending_invocations",
        repo_root / ".diskcache_pending_processing_requests",
    ]


def _print_cache(cache_dir: Path) -> None:
    print(f"\n=== Cache: {cache_dir} ===")
    if not cache_dir.exists():
        print("Directory not found.")
        return

    with dc.Cache(str(cache_dir)) as cache:
        keys = sorted(list(cache))
        print(f"Entries: {len(keys)}")
        if not keys:
            return

        for key in keys:
            value = cache.get(key)
            print(f"- key: {key}")
            print(f"  value: {json.dumps(value, ensure_ascii=False, default=str)}")


def main(cache_dirs: Iterable[Path]) -> None:
    for cache_dir in cache_dirs:
        _print_cache(cache_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Read extractloadlivedata diskcache entries."
    )
    parser.add_argument(
        "--cache-dir",
        action="append",
        default=[],
        help="Cache directory path. Repeat to pass multiple directories.",
    )
    args = parser.parse_args()

    if args.cache_dir:
        dirs = [Path(value).expanduser().resolve() for value in args.cache_dir]
    else:
        dirs = _default_cache_dirs()

    main(dirs)
