#!/usr/bin/env python
"""CLI entry point to run the memo ingestion pipeline."""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from memos import MemoConfig, MemoState, MemoWorkflow


def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the INDOT memo ingestion workflow")
    parser.add_argument("--config", type=Path, default=Path("references/memos/config.json"))
    parser.add_argument("--state", type=Path, default=Path("references/memos/state.json"))
    parser.add_argument("--no-notify", action="store_true", help="Disable email notification even if configured")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    setup_logging(args.verbose)

    config = MemoConfig.load(args.config)
    state = MemoState.load(args.state)

    workflow = MemoWorkflow(config, state)
    result = workflow.run(notify=not args.no_notify)

    summary = {
        "fetched": result.fetched_count,
        "downloaded": [record.memo_id for record in result.downloaded],
        "downloaded_count": result.downloaded_count,
        "parsed": [memo.memo_id for memo in result.parsed],
        "parsed_count": result.parsed_count,
        "failed_parse_count": result.failed_parse_count,
        "notified": result.notified,
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
