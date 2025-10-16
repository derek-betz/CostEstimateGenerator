#!/usr/bin/env python
"""Check for memo approvals and update index ready for commit."""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from memos import MemoConfig, MemoState
from memos.approval import ApprovalChecker
from memos.indexer import MemoIndexer


def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Process memo approvals and update index")
    parser.add_argument("--config", type=Path, default=Path("references/memos/config.json"))
    parser.add_argument("--state", type=Path, default=Path("references/memos/state.json"))
    parser.add_argument("--index", type=Path, default=Path("references/memos/index.json"))
    parser.add_argument("--memo", dest="memo_ids", action="append", help="Specific memo IDs to check (default: all pending)")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    setup_logging(args.verbose)

    config = MemoConfig.load(args.config)
    state = MemoState.load(args.state)
    checker = ApprovalChecker(config)

    pending = [memo_id for memo_id, record in state.memos.items() if record.summary_path and not record.approved]
    if args.memo_ids:
        pending = [memo_id for memo_id in pending if memo_id in args.memo_ids]

    if not pending:
        logging.info("No pending memos for approval")
        return

    results = checker.check(pending)

    approved_records = []
    for result in results:
        record = state.memos.get(result.memo_id)
        if not record:
            continue
        if result.approved:
            logging.info("Memo %s approved by %s", result.memo_id, result.approver)
            record.approved = True
            record.approved_at = result.approved_at.isoformat() if result.approved_at else None
            approved_records.append(record)
        else:
            logging.info("Memo %s not yet approved", result.memo_id)

    if approved_records:
        indexer = MemoIndexer(args.index)
        indexer.update(approved_records)
        state.save()
        logging.info("Updated state and index for %d memo(s)", len(approved_records))


if __name__ == "__main__":
    main()
