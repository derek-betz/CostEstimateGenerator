"""Helper script to run the default cost estimate pipeline."""
from __future__ import annotations

import argparse

from costest.cli import main


def _parse_args(argv: list[str] | None = None) -> tuple[argparse.Namespace, list[str]]:
    parser = argparse.ArgumentParser(description="Run the cost estimate pipeline")
    parser.add_argument(
        "--apply-dm23-21",
        action="store_true",
        help="Enable HMA remapping + transitional adders per INDOT DM 23-21.",
    )
    return parser.parse_known_args(argv)


if __name__ == "__main__":  # pragma: no cover
    args, remaining = _parse_args()
    forward_args: list[str] = list(remaining)
    if args.apply_dm23_21:
        forward_args.append("--apply-dm23-21")
    raise SystemExit(main(forward_args))
