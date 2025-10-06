# AI Recommendations: Improving CostEstimateGenerator Accuracy

Based on the benchmarking-loop summary (13,266 items; RMSE ~$12.9k; MAPE ~32.2%; ALT usage 0%), here are targeted, actionable improvements.

## 1) Data selection and weighting
- Contract-type filter: add project-type/letting-family filters to avoid mixing dissimilar jobs.
- Regional weighting: weight items by district proximity first; fall back to state-level only when necessary.
- Temporal decay: exponentially decay weights beyond 24 months; cap at 36 months.

## 2) Robust statistics and outlier defense
- Switch to median or 20% trimmed mean for the unit price estimator; retain mean only when CV < 0.2.
- Apply Tukey IQR or MAD-based outlier rejection per item category before aggregation.
- Use bootstrap to compute confidence intervals for unit prices.

## 3) Sparse-data fallback (activate Alternate-Seek)
- Enable Alternate-Seek when DATA_POINTS_USED < 3 or description similarity < threshold.
- Similarity model: TF-IDF or sentence embeddings on description/spec; use top-k similar items to derive price.
- Calibrate fallback ratios using historical backtests from the benchmarking PDFs.

## 4) Bias detection and correction
- Maintain per-item residuals from benchmarking; learn correction factors (e.g., ridge regression on features: qty scale, district, month, spec family) and apply as a post-estimate adjustment.
- Cross-validate correction on held-out contracts; include guardrails to avoid overfitting (regularization and min-support thresholds).

## 5) Pipeline safeguards and UX
- Guardrails: if estimated price deviates >200% from median of last 24 months, flag and propose alternative sources.
- Confidence surfacing: compute and display CI-based confidence bands in the Excel output next to CONFIDENCE.
- Diagnostics: write a compact per-item diagnostics CSV (sources, filtered counts, outliers removed, chosen estimator, final price).

## 6) Continuous benchmarking and drift watch
- Schedule quarterly re-benchmarks using the sandbox evaluator; log MAPE/RMSE trends.
- Add a lightweight dashboard (CSV-to-HTML) to visualize error by item family and district over time.

---

### Implementation priorities (sequenced)
1. Robust statistics + outlier rejection (median/trimmed mean + IQR/MAD) with CI and confidence integration.
2. Regional/temporal weighting and contract-type filters in data selection.
3. Activate Alternate-Seek with description similarity and calibrated ratios.
4. Bias correction from residual modeling with cross-validation.
5. Guardrails + diagnostics outputs; quarterly benchmarking automation.

These steps are incremental and measurable with the existing benchmarking-loop, allowing you to verify accuracy gains (MAPE/RMSE) after each change.