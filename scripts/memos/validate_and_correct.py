#!/usr/bin/env python
"""CLI to validate and optionally correct memo digests against source PDFs."""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
import csv
from datetime import datetime

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from costest.memos import MemoConfig, MemoState
from costest.memos.validator import MemoValidator


def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Validate memo digests against PDFs and optionally correct")
    p.add_argument("--config", type=Path, default=Path("references/memos/config.json"))
    p.add_argument("--state", type=Path, default=Path("references/memos/state.json"))
    p.add_argument("--use-ai", action="store_true", help="Enable AI cross-check for additional highlight fixes")
    p.add_argument("--model", default="gpt-4o-mini", help="Model to use with --use-ai (OpenAI)")
    p.add_argument("--apply", action="store_true", help="Write corrected JSON/MD when differences are found")
    p.add_argument("--limit", type=int, default=0, help="Limit the number of memos to validate (0 = all)")
    p.add_argument("--verbose", action="store_true")
    p.add_argument(
        "--audit-csv",
        nargs="?",
        const=str(Path("outputs") / "pay_items_delta_audit.csv"),
        default=None,
        help=(
            "Optional path to write a CSV audit of pay item deltas and changed fields. "
            "If provided without a value, defaults to outputs/pay_items_delta_audit.csv"
        ),
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    setup_logging(args.verbose)

    config = MemoConfig.load(args.config)
    state = MemoState.load(args.state)
    validator = MemoValidator(config, state)

    diffs = validator.validate_and_correct(
        use_ai=args.use_ai,
        model=args.model,
        dry_run=not args.apply,
        limit=(args.limit or None),
    )

    changed = [d for d in diffs if d.changed]
    fields_changed = {d.memo_id: d.fields_changed for d in changed}
    payitem_changes = {
        d.memo_id: {
            "added": d.pay_items_added or [],
            "removed": d.pay_items_removed or [],
        }
        for d in diffs
        if (d.pay_items_added or d.pay_items_removed)
    }
    summary = {
        "validated": len(diffs),
        "changed": len(changed),
        "changed_memos": [d.memo_id for d in changed],
        "fields_changed": fields_changed,
        "pay_items_delta": payitem_changes,
        "notes": {d.memo_id: d.notes for d in diffs if d.notes},
    }
    print(json.dumps(summary, indent=2))

    # Optional CSV audit export
    if args.audit_csv:
        out_path = Path(args.audit_csv)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        # Build rows from diffs
        rows = []
        timestamp = datetime.now().isoformat(timespec="seconds")
        for d in diffs:
            added = d.pay_items_added or []
            removed = d.pay_items_removed or []
            rows.append(
                {
                    "timestamp": timestamp,
                    "memo_id": d.memo_id,
                    "changed": "yes" if d.changed else "no",
                    "fields_changed": ";".join(d.fields_changed or []),
                    "added_count": len(added),
                    "removed_count": len(removed),
                    "added_codes": ";".join(added),
                    "removed_codes": ";".join(removed),
                }
            )

        with out_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "timestamp",
                    "memo_id",
                    "changed",
                    "fields_changed",
                    "added_count",
                    "removed_count",
                    "added_codes",
                    "removed_codes",
                ],
            )
            writer.writeheader()
            writer.writerows(rows)

        print(f"\nWrote CSV audit to: {out_path}")


if __name__ == "__main__":
    main()
