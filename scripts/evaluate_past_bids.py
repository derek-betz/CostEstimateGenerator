from __future__ import annotations

import argparse
from pathlib import Path
import sys
import pandas as pd


def main() -> int:
    # Ensure project src is on path for runtime imports
    sys.path.append(str(Path(__file__).resolve().parents[1] / 'src'))
    from costest.eval import evaluate_contract, summarize_errors  # type: ignore
    ap = argparse.ArgumentParser(description="Batch evaluate CostEstimateGenerator against past bid PDFs")
    ap.add_argument("pdf_folder", type=Path, help="Folder containing bid tab PDFs")
    ap.add_argument("--work-dir", type=Path, default=Path("outputs/eval"), help="Working directory for generated files and reports")
    ap.add_argument("--glob", default="*.pdf", help="Glob pattern for PDFs")
    args = ap.parse_args()

    pdfs = sorted(args.pdf_folder.glob(args.glob))
    if not pdfs:
        print(f"No PDFs found in {args.pdf_folder} matching {args.glob}")
        return 1

    args.work_dir.mkdir(parents=True, exist_ok=True)

    all_rows = []
    for pdf in pdfs:
        print(f"Evaluating {pdf.name}...")
        try:
            merged, out_dir = evaluate_contract(pdf, args.work_dir)
        except Exception as ex:
            print(f"Failed {pdf.name}: {ex}")
            continue
        merged["CONTRACT"] = pdf.stem
        all_rows.append(merged)
        # Save per-contract comparison
        merged.to_csv(out_dir / "comparison.csv", index=False)

    if not all_rows:
        print("No evaluations completed")
        return 2

    results = pd.concat(all_rows, ignore_index=True)
    summary = summarize_errors(results.to_dict("records"))
    results.to_csv(args.work_dir / "all_comparisons.csv", index=False)
    pd.DataFrame([summary]).to_csv(args.work_dir / "summary_metrics.csv", index=False)
    print("Summary:", summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
