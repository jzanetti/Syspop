from os import makedirs
from os.path import exists, join
from itertools import combinations
import numpy as np
import pandas as pd
from scipy import stats
from scipy.spatial.distance import jensenshannon

from syspop.confidential.report import _format_excel_workbook


def _is_continuous_series(s: pd.Series) -> bool:
    return pd.api.types.is_numeric_dtype(s) and not pd.api.types.is_bool_dtype(s)


def _cramers_v(x: pd.Series, y: pd.Series) -> float:
    ct = pd.crosstab(x.astype(str), y.astype(str))
    if ct.shape[0] < 2 or ct.shape[1] < 2:
        return 0.0
    n = float(ct.to_numpy().sum())
    if n <= 0:
        return 0.0
    chi2 = float(stats.chi2_contingency(ct, correction=False)[0])
    min_dim = min(ct.shape[0] - 1, ct.shape[1] - 1)
    return float(np.sqrt(max(0.0, chi2 / (n * min_dim))))


def _correlation_ratio(categories: pd.Series, values: pd.Series) -> float:
    df_tmp = pd.DataFrame({"cat": categories.astype(str), "val": pd.to_numeric(values, errors="coerce")}).dropna()
    if len(df_tmp) < 2:
        return 0.0
    overall_mean = df_tmp["val"].mean()
    ss_total = float(((df_tmp["val"] - overall_mean) ** 2).sum())
    if ss_total <= 0.0:
        return 0.0
    grouped = df_tmp.groupby("cat")["val"]
    ss_between = float((grouped.count() * ((grouped.mean() - overall_mean) ** 2)).sum())
    return float(np.sqrt(np.clip(ss_between / ss_total, 0.0, 1.0)))


def _contingency_tvd(x_raw: pd.Series, y_raw: pd.Series, x_syn: pd.Series, y_syn: pd.Series) -> float:
    p_raw = pd.crosstab(x_raw.astype(str), y_raw.astype(str), normalize=True)
    p_syn = pd.crosstab(x_syn.astype(str), y_syn.astype(str), normalize=True)
    all_idx = p_raw.index.union(p_syn.index)
    all_cols = p_raw.columns.union(p_syn.columns)
    p_raw = p_raw.reindex(index=all_idx, columns=all_cols, fill_value=0.0)
    p_syn = p_syn.reindex(index=all_idx, columns=all_cols, fill_value=0.0)
    return float(0.5 * np.abs(p_raw.to_numpy() - p_syn.to_numpy()).sum())


def build_metric_guide_df() -> pd.DataFrame:
    """Reference guide explaining every standard synthetic data verification metric."""
    rows = [
        ("1_Overall_Summary", "High-level accuracy summary across 1D marginals, 2D correlations, and hierarchy residuals."),
        ("2_Continuous_Fidelity", "1D marginal accuracy for numeric columns (mean, std, median, IQR, KS, Wasserstein, JS distance)."),
        ("3_Categorical_Summary", "1D marginal accuracy for categorical columns (coverage, TVD, KL, JS distance, Chi-square)."),
        ("4_Category_Proportions", "Category-by-category raw vs. synthetic counts and proportions."),
        ("5_Pairwise_Correlations", "2D joint accuracy: Pearson/Spearman (cont-cont), Cramer's V & 2D TVD (cat-cat), Correlation Ratio eta & binned 2D TVD (cont-cat)."),
        ("6_Hierarchy_Residuals", "MAE residuals between target hierarchy aggregates and synthetic microdata (when diagnostics provided)."),
        ("KS Statistic / KS Similarity", "Two-sample Kolmogorov-Smirnov max CDF difference D in [0, 1] (0 = identical). KS Similarity = 1 - D (1 = identical)."),
        ("Wasserstein Distance", "1D Earth Mover's Distance between raw and synthetic distributions in original units (0 = identical)."),
        ("JS Distance (base 2)", "Jensen-Shannon distance sqrt(JSD) in [0, 1] using base-2 log (0 = identical, 1 = disjoint)."),
        ("TVD / TVD Similarity", "Total Variation Distance = 0.5 * sum(|p_raw - p_syn|) in [0, 1] (0 = identical). TVD Similarity = 1 - TVD."),
        ("KL Divergence", "Smoothed Kullback-Leibler divergence D_KL(P_raw || P_syn) >= 0 (0 = identical)."),
        ("Chi-Square (2xK)", "Two-sample Pearson Chi-square homogeneity test comparing raw and synthetic category counts."),
        ("Pearson r / Spearman rho", "Linear and rank correlation coefficients in [-1, 1]; Correlation Similarity = 1 - |r_raw - r_syn| / 2."),
        ("Cramer's V", "Association strength between two categorical variables in [0, 1] (0 = independent, 1 = deterministic)."),
        ("Correlation Ratio (eta)", "ANOVA effect-size measure in [0, 1] showing variance of a continuous column explained by a categorical column."),
    ]
    return pd.DataFrame(rows, columns=["Sheet / Metric", "Definition & Interpretation"])


def evaluate_synthetic_data(
    raw_df: pd.DataFrame,
    syn_df: pd.DataFrame,
    columns: list or None = None,
    diagnostics: dict or None = None,
) -> dict:
    """
    Compute standard statistical fidelity and correlation metrics comparing
    `syn_df` against `raw_df`.
    """
    if columns is None:
        eval_cols = [c for c in raw_df.columns if c in syn_df.columns]
    else:
        eval_cols = [c for c in columns if c in raw_df.columns and c in syn_df.columns]

    cont_cols = [c for c in eval_cols if _is_continuous_series(raw_df[c]) and _is_continuous_series(syn_df[c])]
    cat_cols = [c for c in eval_cols if c not in cont_cols]

    # 1. Continuous 1D Fidelity
    cont_rows = []
    for col in cont_cols:
        r_vals = pd.to_numeric(raw_df[col], errors="coerce").dropna().to_numpy(dtype=float)
        s_vals = pd.to_numeric(syn_df[col], errors="coerce").dropna().to_numpy(dtype=float)
        if len(r_vals) == 0 or len(s_vals) == 0:
            continue

        r_mean, s_mean = float(np.mean(r_vals)), float(np.mean(s_vals))
        r_std = float(np.std(r_vals, ddof=1)) if len(r_vals) > 1 else 0.0
        s_std = float(np.std(s_vals, ddof=1)) if len(s_vals) > 1 else 0.0
        r_med, s_med = float(np.median(r_vals)), float(np.median(s_vals))
        r_iqr = float(np.percentile(r_vals, 75) - np.percentile(r_vals, 25))
        s_iqr = float(np.percentile(s_vals, 75) - np.percentile(s_vals, 25))

        ks_res = stats.ks_2samp(r_vals, s_vals)
        ks_stat = float(ks_res.statistic)
        ks_pval = float(ks_res.pvalue)
        w_dist = float(stats.wasserstein_distance(r_vals, s_vals))

        combined = np.concatenate([r_vals, s_vals])
        if np.all(combined == combined[0]):
            js_dist = 0.0
        else:
            edges = np.histogram_bin_edges(combined, bins="fd")
            if len(edges) - 1 < 10:
                edges = np.linspace(float(combined.min()), float(combined.max()), 11)
            elif len(edges) - 1 > 50:
                edges = np.linspace(float(combined.min()), float(combined.max()), 51)
            p_hist, _ = np.histogram(r_vals, bins=edges)
            q_hist, _ = np.histogram(s_vals, bins=edges)
            p_prob = p_hist / max(p_hist.sum(), 1)
            q_prob = q_hist / max(q_hist.sum(), 1)
            js_dist = float(jensenshannon(p_prob, q_prob, base=2.0))

        cont_rows.append(
            {
                "column": col,
                "raw_count": int(len(r_vals)),
                "syn_count": int(len(s_vals)),
                "raw_mean": r_mean,
                "syn_mean": s_mean,
                "abs_mean_diff": abs(r_mean - s_mean),
                "rel_mean_diff_pct": (abs(r_mean - s_mean) / abs(r_mean) * 100.0) if r_mean != 0 else np.nan,
                "raw_std": r_std,
                "syn_std": s_std,
                "abs_std_diff": abs(r_std - s_std),
                "rel_std_diff_pct": (abs(r_std - s_std) / abs(r_std) * 100.0) if r_std != 0 else np.nan,
                "raw_median": r_med,
                "syn_median": s_med,
                "raw_iqr": r_iqr,
                "syn_iqr": s_iqr,
                "ks_statistic": ks_stat,
                "ks_pvalue": ks_pval,
                "ks_similarity": 1.0 - ks_stat,
                "wasserstein_distance": w_dist,
                "js_distance": js_dist,
            }
        )
    cont_df = pd.DataFrame(cont_rows)

    # 2. Categorical 1D Fidelity
    cat_summary_rows = []
    cat_prop_rows = []
    for col in cat_cols:
        r_ser = raw_df[col].dropna().astype(str)
        s_ser = syn_df[col].dropna().astype(str)
        r_cnts = r_ser.value_counts()
        s_cnts = s_ser.value_counts()
        all_cats = sorted(set(r_cnts.index).union(set(s_cnts.index)))
        if not all_cats:
            continue

        r_vec = np.array([float(r_cnts.get(k, 0)) for k in all_cats], dtype=float)
        s_vec = np.array([float(s_cnts.get(k, 0)) for k in all_cats], dtype=float)
        r_prob = r_vec / max(r_vec.sum(), 1.0)
        s_prob = s_vec / max(s_vec.sum(), 1.0)

        tvd = float(0.5 * np.abs(r_prob - s_prob).sum())
        js_dist = float(jensenshannon(r_prob, s_prob, base=2.0))
        eps = 1e-10
        r_smooth = (r_prob + eps) / (r_prob + eps).sum()
        s_smooth = (s_prob + eps) / (s_prob + eps).sum()
        kl_div = float(np.sum(r_smooth * np.log(r_smooth / s_smooth)))

        non_zero_cols = (r_vec + s_vec) > 0
        if non_zero_cols.sum() >= 2:
            chi2_stat, chi2_pval, _, _ = stats.chi2_contingency(
                np.vstack([r_vec[non_zero_cols], s_vec[non_zero_cols]]), correction=False
            )
        else:
            chi2_stat, chi2_pval = 0.0, 1.0

        raw_cats = set(r_cnts.index)
        syn_cats = set(s_cnts.index)
        coverage = float(len(raw_cats & syn_cats) / len(raw_cats)) if raw_cats else 1.0

        cat_summary_rows.append(
            {
                "column": col,
                "raw_unique": int(len(raw_cats)),
                "syn_unique": int(len(syn_cats)),
                "category_coverage": coverage,
                "tvd": tvd,
                "tvd_similarity": 1.0 - tvd,
                "kl_divergence": kl_div,
                "js_distance": js_dist,
                "chi2_statistic": float(chi2_stat),
                "chi2_pvalue": float(chi2_pval),
            }
        )

        for idx, k in enumerate(all_cats):
            cat_prop_rows.append(
                {
                    "column": col,
                    "category": k,
                    "raw_count": int(r_vec[idx]),
                    "syn_count": int(s_vec[idx]),
                    "raw_proportion": float(r_prob[idx]),
                    "syn_proportion": float(s_prob[idx]),
                    "abs_diff": float(abs(r_prob[idx] - s_prob[idx])),
                }
            )
    cat_summary_df = pd.DataFrame(cat_summary_rows)
    cat_props_df = pd.DataFrame(cat_prop_rows)

    # 3. Pairwise 2D Correlations & Joint Fidelity
    pair_rows = []
    for c1, c2 in combinations(eval_cols, 2):
        c1_cont = c1 in cont_cols
        c2_cont = c2 in cont_cols
        if c1_cont and c2_cont:
            r_pair = raw_df[[c1, c2]].apply(pd.to_numeric, errors="coerce").dropna()
            s_pair = syn_df[[c1, c2]].apply(pd.to_numeric, errors="coerce").dropna()
            if len(r_pair) >= 2 and r_pair[c1].std() > 0 and r_pair[c2].std() > 0:
                pr_raw = float(stats.pearsonr(r_pair[c1], r_pair[c2]).statistic)
                sr_raw = float(stats.spearmanr(r_pair[c1], r_pair[c2]).statistic)
            else:
                pr_raw, sr_raw = np.nan, np.nan
            if len(s_pair) >= 2 and s_pair[c1].std() > 0 and s_pair[c2].std() > 0:
                pr_syn = float(stats.pearsonr(s_pair[c1], s_pair[c2]).statistic)
                sr_syn = float(stats.spearmanr(s_pair[c1], s_pair[c2]).statistic)
            else:
                pr_syn, sr_syn = np.nan, np.nan

            p_diff = abs(pr_raw - pr_syn) if (np.isfinite(pr_raw) and np.isfinite(pr_syn)) else np.nan
            s_diff = abs(sr_raw - sr_syn) if (np.isfinite(sr_raw) and np.isfinite(sr_syn)) else np.nan
            corr_sim = (1.0 - p_diff / 2.0) if np.isfinite(p_diff) else np.nan
            pair_rows.append(
                {
                    "feature_1": c1,
                    "feature_2": c2,
                    "pair_type": "continuous-continuous",
                    "metric_name": "Pearson r / Spearman rho",
                    "raw_value": pr_raw,
                    "syn_value": pr_syn,
                    "abs_diff": p_diff,
                    "spearman_raw": sr_raw,
                    "spearman_syn": sr_syn,
                    "spearman_abs_diff": s_diff,
                    "contingency_tvd": np.nan,
                    "similarity_score": corr_sim,
                }
            )
        elif (not c1_cont) and (not c2_cont):
            v_raw = _cramers_v(raw_df[c1], raw_df[c2])
            v_syn = _cramers_v(syn_df[c1], syn_df[c2])
            tvd_2d = _contingency_tvd(raw_df[c1], raw_df[c2], syn_df[c1], syn_df[c2])
            pair_rows.append(
                {
                    "feature_1": c1,
                    "feature_2": c2,
                    "pair_type": "categorical-categorical",
                    "metric_name": "Cramer's V & 2D TVD",
                    "raw_value": v_raw,
                    "syn_value": v_syn,
                    "abs_diff": abs(v_raw - v_syn),
                    "spearman_raw": np.nan,
                    "spearman_syn": np.nan,
                    "spearman_abs_diff": np.nan,
                    "contingency_tvd": tvd_2d,
                    "similarity_score": 1.0 - tvd_2d,
                }
            )
        else:
            cat_c, cont_c = (c1, c2) if not c1_cont else (c2, c1)
            eta_raw = _correlation_ratio(raw_df[cat_c], raw_df[cont_c])
            eta_syn = _correlation_ratio(syn_df[cat_c], syn_df[cont_c])
            # Binned 2D contingency TVD using shared quantile edges of continuous column
            r_num = pd.to_numeric(raw_df[cont_c], errors="coerce")
            s_num = pd.to_numeric(syn_df[cont_c], errors="coerce")
            combined_num = pd.concat([r_num, s_num]).dropna()
            if len(combined_num) >= 2 and combined_num.nunique() > 1:
                _, q_edges = pd.qcut(combined_num, q=min(10, combined_num.nunique()), retbins=True, duplicates="drop")
                r_bin = pd.cut(r_num, bins=q_edges, include_lowest=True).astype(str)
                s_bin = pd.cut(s_num, bins=q_edges, include_lowest=True).astype(str)
                tvd_2d = _contingency_tvd(raw_df[cat_c], r_bin, syn_df[cat_c], s_bin)
            else:
                tvd_2d = 0.0
            pair_rows.append(
                {
                    "feature_1": c1,
                    "feature_2": c2,
                    "pair_type": "continuous-categorical",
                    "metric_name": "Correlation Ratio (eta) & Binned 2D TVD",
                    "raw_value": eta_raw,
                    "syn_value": eta_syn,
                    "abs_diff": abs(eta_raw - eta_syn),
                    "spearman_raw": np.nan,
                    "spearman_syn": np.nan,
                    "spearman_abs_diff": np.nan,
                    "contingency_tvd": tvd_2d,
                    "similarity_score": 1.0 - tvd_2d,
                }
            )
    pairs_df = pd.DataFrame(pair_rows)

    # 4. Optional Hierarchy Residuals
    res_rows = []
    if diagnostics and isinstance(diagnostics.get("residuals"), dict):
        for lvl, res_map in diagnostics["residuals"].items():
            for metric_k, mae_val in res_map.items():
                res_rows.append({"level": lvl, "metric": metric_k, "mae_residual": float(mae_val)})
    residuals_df = pd.DataFrame(res_rows)

    # 5. Overall Summary
    summary_rows = [
        {"Metric": "Raw Records", "Value": int(len(raw_df))},
        {"Metric": "Synthetic Records", "Value": int(len(syn_df))},
        {"Metric": "Continuous Columns Evaluated", "Value": int(len(cont_df))},
        {"Metric": "Categorical Columns Evaluated", "Value": int(len(cat_summary_df))},
        {"Metric": "Pairwise Feature Pairs Evaluated", "Value": int(len(pairs_df))},
    ]
    if len(cont_df) > 0:
        summary_rows.extend(
            [
                {"Metric": "Mean Continuous KS Statistic (lower=better)", "Value": float(cont_df["ks_statistic"].mean())},
                {"Metric": "Mean Continuous KS Similarity (1-KS, higher=better)", "Value": float(cont_df["ks_similarity"].mean())},
                {"Metric": "Mean Continuous JS Distance (lower=better)", "Value": float(cont_df["js_distance"].mean())},
            ]
        )
    if len(cat_summary_df) > 0:
        summary_rows.extend(
            [
                {"Metric": "Mean Categorical TVD (lower=better)", "Value": float(cat_summary_df["tvd"].mean())},
                {"Metric": "Mean Categorical TVD Similarity (1-TVD, higher=better)", "Value": float(cat_summary_df["tvd_similarity"].mean())},
                {"Metric": "Mean Categorical JS Distance (lower=better)", "Value": float(cat_summary_df["js_distance"].mean())},
                {"Metric": "Mean Category Coverage", "Value": float(cat_summary_df["category_coverage"].mean())},
            ]
        )
    if len(pairs_df) > 0:
        valid_sim = pairs_df["similarity_score"].dropna()
        if len(valid_sim) > 0:
            summary_rows.append(
                {"Metric": "Mean Pairwise Joint Similarity (higher=better)", "Value": float(valid_sim.mean())}
            )
    overall_df = pd.DataFrame(summary_rows)

    return {
        "metric_guide": build_metric_guide_df(),
        "overall_summary": overall_df,
        "continuous_fidelity": cont_df,
        "categorical_summary": cat_summary_df,
        "category_proportions": cat_props_df,
        "pairwise_correlations": pairs_df,
        "hierarchy_residuals": residuals_df,
    }


def export_synthetic_verification_report(
    raw_df: pd.DataFrame,
    syn_df: pd.DataFrame,
    output_dir: str,
    columns: list or None = None,
    diagnostics: dict or None = None,
    filename: str = "synthetic_verification_report.xlsx",
) -> str:
    """
    Evaluate synthetic data accuracy against raw data using standard statistical
    metrics and export a formatted multi-sheet Excel report.
    """
    if not exists(output_dir):
        makedirs(output_dir)

    results = evaluate_synthetic_data(
        raw_df=raw_df,
        syn_df=syn_df,
        columns=columns,
        diagnostics=diagnostics,
    )
    excel_path = join(output_dir, filename)
    sheets = {
        "0_Metric_Guide": results["metric_guide"],
        "1_Overall_Summary": results["overall_summary"],
        "2_Continuous_Fidelity": results["continuous_fidelity"],
        "3_Categorical_Summary": results["categorical_summary"],
        "4_Category_Proportions": results["category_proportions"],
        "5_Pairwise_Correlations": results["pairwise_correlations"],
    }
    if len(results["hierarchy_residuals"]) > 0:
        sheets["6_Hierarchy_Residuals"] = results["hierarchy_residuals"]

    with pd.ExcelWriter(excel_path) as writer:
        for sheet_name, df_sheet in sheets.items():
            df_sheet.to_excel(writer, sheet_name=sheet_name[:31], index=False)
        _format_excel_workbook(writer)

    return excel_path
