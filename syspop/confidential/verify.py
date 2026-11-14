from os import makedirs
from os.path import exists, join
from itertools import combinations
import numpy as np
import pandas as pd
from scipy import stats
from scipy.spatial.distance import jensenshannon

from syspop.confidential.config import resolve_verification_config
from syspop.confidential.report import _format_excel_workbook
from syspop.postp.vis import compute_idi_safe_1d_bins, compute_idi_safe_2d_bins


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


DEFAULT_VERIFICATION_THRESHOLDS = {
    "max_ks_statistic": 0.20,
    "max_tvd": 0.15,
    "min_category_coverage": 0.90,
    "min_similarity_score": 0.80,
}


def evaluate_synthetic_data(
    raw_df: pd.DataFrame,
    syn_df: pd.DataFrame,
    columns: list or None = None,
    diagnostics: dict or None = None,
    thresholds: dict or None = None,
) -> dict:
    """
    Compute standard statistical fidelity and correlation metrics comparing
    `syn_df` against `raw_df`, with pass/check status against `thresholds`.
    """
    active_thresh = dict(DEFAULT_VERIFICATION_THRESHOLDS)
    if thresholds:
        active_thresh.update(thresholds)
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

        r_nz = r_vec[r_vec > 0]
        s_nz = s_vec[s_vec > 0]
        cat_summary_rows.append(
            {
                "column": col,
                "raw_total_count": int(r_vec.sum()),
                "syn_total_count": int(s_vec.sum()),
                "min_raw_cell_count": int(r_nz.min()) if len(r_nz) > 0 else 0,
                "min_syn_cell_count": int(s_nz.min()) if len(s_nz) > 0 else 0,
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
        f1_raw_cnt = int(raw_df[c1].notna().sum())
        f1_syn_cnt = int(syn_df[c1].notna().sum())
        f2_raw_cnt = int(raw_df[c2].notna().sum())
        f2_syn_cnt = int(syn_df[c2].notna().sum())
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
                    "feature_1_raw_count": f1_raw_cnt,
                    "feature_1_syn_count": f1_syn_cnt,
                    "feature_2": c2,
                    "feature_2_raw_count": f2_raw_cnt,
                    "feature_2_syn_count": f2_syn_cnt,
                    "raw_count": int(len(r_pair)),
                    "syn_count": int(len(s_pair)),
                    "min_raw_cell_count": np.nan,
                    "min_syn_cell_count": np.nan,
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
            r_pair = raw_df[[c1, c2]].dropna()
            s_pair = syn_df[[c1, c2]].dropna()
            r_ct = pd.crosstab(r_pair[c1].astype(str), r_pair[c2].astype(str)).to_numpy()
            s_ct = pd.crosstab(s_pair[c1].astype(str), s_pair[c2].astype(str)).to_numpy()
            r_nz_2d = r_ct[r_ct > 0]
            s_nz_2d = s_ct[s_ct > 0]
            v_raw = _cramers_v(raw_df[c1], raw_df[c2])
            v_syn = _cramers_v(syn_df[c1], syn_df[c2])
            tvd_2d = _contingency_tvd(raw_df[c1], raw_df[c2], syn_df[c1], syn_df[c2])
            pair_rows.append(
                {
                    "feature_1": c1,
                    "feature_1_raw_count": f1_raw_cnt,
                    "feature_1_syn_count": f1_syn_cnt,
                    "feature_2": c2,
                    "feature_2_raw_count": f2_raw_cnt,
                    "feature_2_syn_count": f2_syn_cnt,
                    "raw_count": int(len(r_pair)),
                    "syn_count": int(len(s_pair)),
                    "min_raw_cell_count": int(r_nz_2d.min()) if len(r_nz_2d) > 0 else 0,
                    "min_syn_cell_count": int(s_nz_2d.min()) if len(s_nz_2d) > 0 else 0,
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
            r_pair = pd.DataFrame({"cat": raw_df[cat_c], "val": pd.to_numeric(raw_df[cont_c], errors="coerce")}).dropna()
            s_pair = pd.DataFrame({"cat": syn_df[cat_c], "val": pd.to_numeric(syn_df[cont_c], errors="coerce")}).dropna()
            r_grp_cnts = r_pair["cat"].astype(str).value_counts()
            s_grp_cnts = s_pair["cat"].astype(str).value_counts()
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
                    "feature_1_raw_count": f1_raw_cnt,
                    "feature_1_syn_count": f1_syn_cnt,
                    "feature_2": c2,
                    "feature_2_raw_count": f2_raw_cnt,
                    "feature_2_syn_count": f2_syn_cnt,
                    "raw_count": int(len(r_pair)),
                    "syn_count": int(len(s_pair)),
                    "min_raw_cell_count": int(r_grp_cnts.min()) if len(r_grp_cnts) > 0 else 0,
                    "min_syn_cell_count": int(s_grp_cnts.min()) if len(s_grp_cnts) > 0 else 0,
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
                res_rows.append(
                    {
                        "level": lvl,
                        "metric": metric_k,
                        "raw_count": int(len(raw_df)),
                        "syn_count": int(len(syn_df)),
                        "mae_residual": float(mae_val),
                    }
                )
    residuals_df = pd.DataFrame(res_rows)

    if len(cont_df) > 0:
        cont_df["max_ks_threshold"] = active_thresh["max_ks_statistic"]
        cont_df["status"] = np.where(
            cont_df["ks_statistic"] <= active_thresh["max_ks_statistic"], "PASS", "CHECK"
        )
    if len(cat_summary_df) > 0:
        cat_summary_df["max_tvd_threshold"] = active_thresh["max_tvd"]
        cat_summary_df["min_coverage_threshold"] = active_thresh["min_category_coverage"]
        cat_summary_df["status"] = np.where(
            (cat_summary_df["tvd"] <= active_thresh["max_tvd"])
            & (cat_summary_df["category_coverage"] >= active_thresh["min_category_coverage"]),
            "PASS",
            "CHECK",
        )
    if len(pairs_df) > 0:
        pairs_df["min_similarity_threshold"] = active_thresh["min_similarity_score"]
        pairs_df["status"] = np.where(
            pairs_df["similarity_score"].fillna(1.0) >= active_thresh["min_similarity_score"],
            "PASS",
            "CHECK",
        )

    # 5. Overall Summary
    n_raw, n_syn = int(len(raw_df)), int(len(syn_df))
    summary_rows = [
        {"Metric": "Raw Records", "raw_count": n_raw, "syn_count": n_syn, "Value": n_raw, "Threshold": "", "Status": "INFO"},
        {"Metric": "Synthetic Records", "raw_count": n_raw, "syn_count": n_syn, "Value": n_syn, "Threshold": "", "Status": "INFO"},
        {"Metric": "Continuous Columns Evaluated", "raw_count": n_raw, "syn_count": n_syn, "Value": int(len(cont_df)), "Threshold": "", "Status": "INFO"},
        {"Metric": "Categorical Columns Evaluated", "raw_count": n_raw, "syn_count": n_syn, "Value": int(len(cat_summary_df)), "Threshold": "", "Status": "INFO"},
        {"Metric": "Pairwise Feature Pairs Evaluated", "raw_count": n_raw, "syn_count": n_syn, "Value": int(len(pairs_df)), "Threshold": "", "Status": "INFO"},
    ]
    if len(cont_df) > 0:
        mean_ks = float(cont_df["ks_statistic"].mean())
        summary_rows.extend(
            [
                {
                    "Metric": "Mean Continuous KS Statistic (lower=better)",
                    "raw_count": n_raw,
                    "syn_count": n_syn,
                    "Value": mean_ks,
                    "Threshold": f"<= {active_thresh['max_ks_statistic']:.2f}",
                    "Status": "PASS" if mean_ks <= active_thresh["max_ks_statistic"] else "CHECK",
                },
                {
                    "Metric": "Mean Continuous KS Similarity (1-KS, higher=better)",
                    "raw_count": n_raw,
                    "syn_count": n_syn,
                    "Value": float(cont_df["ks_similarity"].mean()),
                    "Threshold": f">= {1.0 - active_thresh['max_ks_statistic']:.2f}",
                    "Status": "PASS" if mean_ks <= active_thresh["max_ks_statistic"] else "CHECK",
                },
                {
                    "Metric": "Mean Continuous JS Distance (lower=better)",
                    "raw_count": n_raw,
                    "syn_count": n_syn,
                    "Value": float(cont_df["js_distance"].mean()),
                    "Threshold": "",
                    "Status": "INFO",
                },
            ]
        )
    if len(cat_summary_df) > 0:
        mean_tvd = float(cat_summary_df["tvd"].mean())
        mean_cov = float(cat_summary_df["category_coverage"].mean())
        summary_rows.extend(
            [
                {
                    "Metric": "Mean Categorical TVD (lower=better)",
                    "raw_count": n_raw,
                    "syn_count": n_syn,
                    "Value": mean_tvd,
                    "Threshold": f"<= {active_thresh['max_tvd']:.2f}",
                    "Status": "PASS" if mean_tvd <= active_thresh["max_tvd"] else "CHECK",
                },
                {
                    "Metric": "Mean Categorical TVD Similarity (1-TVD, higher=better)",
                    "raw_count": n_raw,
                    "syn_count": n_syn,
                    "Value": float(cat_summary_df["tvd_similarity"].mean()),
                    "Threshold": f">= {1.0 - active_thresh['max_tvd']:.2f}",
                    "Status": "PASS" if mean_tvd <= active_thresh["max_tvd"] else "CHECK",
                },
                {
                    "Metric": "Mean Categorical JS Distance (lower=better)",
                    "raw_count": n_raw,
                    "syn_count": n_syn,
                    "Value": float(cat_summary_df["js_distance"].mean()),
                    "Threshold": "",
                    "Status": "INFO",
                },
                {
                    "Metric": "Mean Category Coverage",
                    "raw_count": n_raw,
                    "syn_count": n_syn,
                    "Value": mean_cov,
                    "Threshold": f">= {active_thresh['min_category_coverage']:.2f}",
                    "Status": "PASS" if mean_cov >= active_thresh["min_category_coverage"] else "CHECK",
                },
            ]
        )
    if len(pairs_df) > 0:
        valid_sim = pairs_df["similarity_score"].dropna()
        if len(valid_sim) > 0:
            mean_pair_sim = float(valid_sim.mean())
            summary_rows.append(
                {
                    "Metric": "Mean Pairwise Joint Similarity (higher=better)",
                    "raw_count": n_raw,
                    "syn_count": n_syn,
                    "Value": mean_pair_sim,
                    "Threshold": f">= {active_thresh['min_similarity_score']:.2f}",
                    "Status": "PASS" if mean_pair_sim >= active_thresh["min_similarity_score"] else "CHECK",
                }
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
    thresholds: dict or None = None,
    figure_paths: list or None = None,
    filename: str = "synthetic_verification_report.xlsx",
    config_or_path=None,
) -> str:
    """
    Evaluate synthetic data accuracy against raw data using standard statistical
    metrics (with pass/check thresholds), optionally embed verification PNG figures,
    and export a formatted multi-sheet Excel report.
    """
    if not exists(output_dir):
        makedirs(output_dir)

    if thresholds is None and config_or_path is not None:
        thresholds = resolve_verification_config(config_or_path).get("thresholds")

    results = evaluate_synthetic_data(
        raw_df=raw_df,
        syn_df=syn_df,
        columns=columns,
        diagnostics=diagnostics,
        thresholds=thresholds,
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

    valid_figs = [p for p in (figure_paths or []) if p and exists(p)]

    with pd.ExcelWriter(excel_path) as writer:
        for sheet_name, df_sheet in sheets.items():
            df_sheet.to_excel(writer, sheet_name=sheet_name[:31], index=False)
        if valid_figs:
            fig_idx_df = pd.DataFrame(
                [{"figure_number": i + 1, "file_path": p} for i, p in enumerate(valid_figs)]
            )
            fig_idx_df.to_excel(writer, sheet_name="Verification_Figures", index=False)
        _format_excel_workbook(writer)

        if valid_figs:
            try:
                from openpyxl.drawing.image import Image as XLImage

                ws_fig = writer.sheets["Verification_Figures"]
                cur_row = len(valid_figs) + 3
                for p in valid_figs:
                    img = XLImage(p)
                    # Scale down wide figures neatly while preserving aspect ratio
                    if img.width and img.width > 900:
                        scale = 900.0 / float(img.width)
                        img.width = 900
                        img.height = int(img.height * scale)
                    ws_fig.add_image(img, f"A{cur_row}")
                    cur_row += max(int((img.height or 300) / 20) + 3, 18)
            except Exception:
                pass

    return excel_path


def export_verification_figures_counts(
    raw_df: pd.DataFrame,
    syn_df: pd.DataFrame,
    columns: list,
    output_dir: str,
    focus_column: str or None = None,
    min_count: int or None = None,
    filename: str = "verification_figures_counts.xlsx",
    config_or_path=None,
) -> str:
    """
    Export an Excel workbook (`verification_figures_counts.xlsx`) recording the exact
    underlying raw and synthetic record counts used in every bin, bar, group, and
    2D density cell of the verification figures (`comparison_raw_vs_synthetic.png`
    and `comparison_{col1}_{col2}.png`).
    """
    if min_count is None:
        verif_cfg = resolve_verification_config(config_or_path) if config_or_path is not None else {}
        min_count = int(verif_cfg.get("min_count", 6))

    if not exists(output_dir):
        makedirs(output_dir)

    if isinstance(columns, str):
        columns = [columns]
    eval_cols = [c for c in columns if c in raw_df.columns and c in syn_df.columns]

    summary_rows = []
    hist_1d_rows = []
    cat_1d_rows = []
    cat_cat_2d_rows = []
    cat_cont_2d_rows = []
    cont_cont_2d_rows = []

    # 1. 1D comparison figure counts (matching vis.plot_comparison)
    min_1d_raw = None
    min_1d_syn = None
    for col in eval_cols:
        if _is_continuous_series(raw_df[col]) and _is_continuous_series(syn_df[col]):
            r_vals = pd.to_numeric(raw_df[col], errors="coerce").dropna().to_numpy(dtype=float)
            s_vals = pd.to_numeric(syn_df[col], errors="coerce").dropna().to_numpy(dtype=float)
            if len(r_vals) == 0 or len(s_vals) == 0:
                continue
            bins = compute_idi_safe_1d_bins(r_vals, s_vals, min_bin_count=min_count)
            r_cnts, _ = np.histogram(r_vals, bins=bins)
            s_cnts, _ = np.histogram(s_vals, bins=bins)
            r_tot = max(int(r_cnts.sum()), 1)
            s_tot = max(int(s_cnts.sum()), 1)
            for b_i in range(len(r_cnts)):
                rc = int(r_cnts[b_i])
                sc = int(s_cnts[b_i])
                if rc > 0:
                    min_1d_raw = rc if min_1d_raw is None else min(min_1d_raw, rc)
                if sc > 0:
                    min_1d_syn = sc if min_1d_syn is None else min(min_1d_syn, sc)
                hist_1d_rows.append(
                    {
                        "figure_file": "comparison_raw_vs_synthetic.png",
                        "column": col,
                        "bin_lower": float(bins[b_i]),
                        "bin_upper": float(bins[b_i + 1]),
                        "raw_count": rc,
                        "syn_count": sc,
                        "raw_proportion": rc / r_tot,
                        "syn_proportion": sc / s_tot,
                        "min_required_count": int(min_count),
                        "idi_status": "PASS" if (rc == 0 or rc >= min_count) else "CHECK",
                    }
                )
        else:
            r_ser = raw_df[col].dropna().astype(str)
            s_ser = syn_df[col].dropna().astype(str)
            r_cnts = r_ser.value_counts()
            s_cnts = s_ser.value_counts()
            cats = sorted(set(r_cnts.index).union(set(s_cnts.index)))
            r_tot = max(int(len(r_ser)), 1)
            s_tot = max(int(len(s_ser)), 1)
            for k in cats:
                rc = int(r_cnts.get(k, 0))
                sc = int(s_cnts.get(k, 0))
                is_suppressed = 0 < rc < min_count
                if not is_suppressed and rc > 0:
                    min_1d_raw = rc if min_1d_raw is None else min(min_1d_raw, rc)
                if sc > 0:
                    min_1d_syn = sc if min_1d_syn is None else min(min_1d_syn, sc)
                cat_1d_rows.append(
                    {
                        "figure_file": "comparison_raw_vs_synthetic.png",
                        "column": col,
                        "category": k,
                        "raw_count": rc,
                        "syn_count": sc,
                        "raw_proportion": np.nan if is_suppressed else (rc / r_tot),
                        "syn_proportion": sc / s_tot,
                        "min_required_count": int(min_count),
                        "idi_status": "SUPPRESSED" if is_suppressed else "PASS",
                    }
                )

    summary_rows.append(
        {
            "figure_file": "comparison_raw_vs_synthetic.png",
            "figure_type": "1D_comparison",
            "features": ", ".join(eval_cols),
            "raw_total_count": int(len(raw_df)),
            "syn_total_count": int(len(syn_df)),
            "min_nonzero_raw_count": min_1d_raw if min_1d_raw is not None else 0,
            "min_nonzero_syn_count": min_1d_syn if min_1d_syn is not None else 0,
        }
    )

    # 2. 2D cross-comparison figure counts (matching vis.plot_cross_comparison)
    if focus_column is not None and focus_column in eval_cols:
        pairs = [(focus_column, c) for c in eval_cols if c != focus_column]
    else:
        pairs = list(combinations(eval_cols, 2))

    for col1, col2 in pairs:
        c1_cont = _is_continuous_series(raw_df[col1]) and _is_continuous_series(syn_df[col1])
        c2_cont = _is_continuous_series(raw_df[col2]) and _is_continuous_series(syn_df[col2])
        fig_name = f"comparison_{col1}_{col2}.png"

        if (not c1_cont) and (not c2_cont):
            r_pair = raw_df[[col1, col2]].dropna()
            s_pair = syn_df[[col1, col2]].dropna()
            r_ct = pd.crosstab(r_pair[col1].astype(str), r_pair[col2].astype(str))
            s_ct = pd.crosstab(s_pair[col1].astype(str), s_pair[col2].astype(str))
            all_v1 = sorted(set(r_ct.index).union(set(s_ct.index)))
            all_v2 = sorted(set(r_ct.columns).union(set(s_ct.columns)))
            r_ct = r_ct.reindex(index=all_v1, columns=all_v2, fill_value=0)
            s_ct = s_ct.reindex(index=all_v1, columns=all_v2, fill_value=0)
            r_tot = max(int(len(r_pair)), 1)
            s_tot = max(int(len(s_pair)), 1)
            min_r, min_s = None, None
            for v1 in all_v1:
                for v2 in all_v2:
                    rc = int(r_ct.loc[v1, v2])
                    sc = int(s_ct.loc[v1, v2])
                    if rc == 0 and sc == 0:
                        continue
                    is_suppressed = 0 < rc < min_count
                    if not is_suppressed and rc > 0:
                        min_r = rc if min_r is None else min(min_r, rc)
                    if sc > 0:
                        min_s = sc if min_s is None else min(min_s, sc)
                    cat_cat_2d_rows.append(
                        {
                            "figure_file": fig_name,
                            "feature_1": col1,
                            "feature_2": col2,
                            "value_1": v1,
                            "value_2": v2,
                            "raw_count": rc,
                            "syn_count": sc,
                            "raw_proportion": np.nan if is_suppressed else (rc / r_tot),
                            "syn_proportion": sc / s_tot,
                            "min_required_count": int(min_count),
                            "idi_status": "SUPPRESSED" if is_suppressed else "PASS",
                        }
                    )
            summary_rows.append(
                {
                    "figure_file": fig_name,
                    "figure_type": "2D_categorical_categorical",
                    "features": f"{col1}, {col2}",
                    "raw_total_count": int(len(r_pair)),
                    "syn_total_count": int(len(s_pair)),
                    "min_nonzero_raw_count": min_r if min_r is not None else 0,
                    "min_nonzero_syn_count": min_s if min_s is not None else 0,
                }
            )
        elif (not c1_cont) or (not c2_cont):
            cat_c, cont_c = (col1, col2) if not c1_cont else (col2, col1)
            r_sub = pd.DataFrame({"cat": raw_df[cat_c].astype(str), "val": pd.to_numeric(raw_df[cont_c], errors="coerce")}).dropna()
            s_sub = pd.DataFrame({"cat": syn_df[cat_c].astype(str), "val": pd.to_numeric(syn_df[cont_c], errors="coerce")}).dropna()
            cats = sorted(set(r_sub["cat"]).union(set(s_sub["cat"])))
            min_r, min_s = None, None
            for k in cats:
                rv = r_sub.loc[r_sub["cat"] == k, "val"]
                sv = s_sub.loc[s_sub["cat"] == k, "val"]
                rc, sc = int(len(rv)), int(len(sv))
                is_suppressed = 0 < rc < min_count
                if not is_suppressed and rc > 0:
                    min_r = rc if min_r is None else min(min_r, rc)
                if sc > 0:
                    min_s = sc if min_s is None else min(min_s, sc)
                cat_cont_2d_rows.append(
                    {
                        "figure_file": fig_name,
                        "cat_feature": cat_c,
                        "cont_feature": cont_c,
                        "category": k,
                        "raw_count": rc,
                        "syn_count": sc,
                        "raw_mean": np.nan if is_suppressed else (float(rv.mean()) if rc > 0 else np.nan),
                        "syn_mean": float(sv.mean()) if sc > 0 else np.nan,
                        "raw_std": np.nan if is_suppressed else (float(rv.std()) if rc > 1 else np.nan),
                        "syn_std": float(sv.std()) if sc > 1 else np.nan,
                        "raw_median": np.nan if is_suppressed else (float(rv.median()) if rc > 0 else np.nan),
                        "syn_median": float(sv.median()) if sc > 0 else np.nan,
                        "min_required_count": int(min_count),
                        "idi_status": "SUPPRESSED" if is_suppressed else "PASS",
                    }
                )
            summary_rows.append(
                {
                    "figure_file": fig_name,
                    "figure_type": "2D_categorical_continuous",
                    "features": f"{col1}, {col2}",
                    "raw_total_count": int(len(r_sub)),
                    "syn_total_count": int(len(s_sub)),
                    "min_nonzero_raw_count": min_r if min_r is not None else 0,
                    "min_nonzero_syn_count": min_s if min_s is not None else 0,
                }
            )
        else:
            rx = pd.to_numeric(raw_df[col1], errors="coerce").to_numpy(dtype=float)
            ry = pd.to_numeric(raw_df[col2], errors="coerce").to_numpy(dtype=float)
            sx = pd.to_numeric(syn_df[col1], errors="coerce").to_numpy(dtype=float)
            sy = pd.to_numeric(syn_df[col2], errors="coerce").to_numpy(dtype=float)
            rmask = ~np.isnan(rx) & ~np.isnan(ry)
            smask = ~np.isnan(sx) & ~np.isnan(sy)
            rx, ry = rx[rmask], ry[rmask]
            sx, sy = sx[smask], sy[smask]
            if len(rx) == 0 or len(sx) == 0:
                continue
            x_bins, y_bins = compute_idi_safe_2d_bins(rx, ry, sx, sy, min_bin_count=min_count)
            h_raw_cnt, _, _ = np.histogram2d(rx, ry, bins=[x_bins, y_bins])
            h_syn_cnt, _, _ = np.histogram2d(sx, sy, bins=[x_bins, y_bins])
            h_raw_den, _, _ = np.histogram2d(rx, ry, bins=[x_bins, y_bins], density=True)
            h_syn_den, _, _ = np.histogram2d(sx, sy, bins=[x_bins, y_bins], density=True)
            min_r, min_s = None, None
            for xi in range(len(x_bins) - 1):
                for yi in range(len(y_bins) - 1):
                    rc = int(h_raw_cnt[xi, yi])
                    sc = int(h_syn_cnt[xi, yi])
                    if rc == 0 and sc == 0:
                        continue
                    is_suppressed = 0 < rc < min_count
                    if not is_suppressed and rc > 0:
                        min_r = rc if min_r is None else min(min_r, rc)
                    if sc > 0:
                        min_s = sc if min_s is None else min(min_s, sc)
                    cont_cont_2d_rows.append(
                        {
                            "figure_file": fig_name,
                            "feature_1": col1,
                            "feature_2": col2,
                            "x_bin_lower": float(x_bins[xi]),
                            "x_bin_upper": float(x_bins[xi + 1]),
                            "y_bin_lower": float(y_bins[yi]),
                            "y_bin_upper": float(y_bins[yi + 1]),
                            "raw_count": rc,
                            "syn_count": sc,
                            "raw_density": np.nan if is_suppressed else float(h_raw_den[xi, yi]),
                            "syn_density": float(h_syn_den[xi, yi]),
                            "min_required_count": int(min_count),
                            "idi_status": "SUPPRESSED" if is_suppressed else "PASS",
                        }
                    )
            summary_rows.append(
                {
                    "figure_file": fig_name,
                    "figure_type": "2D_continuous_density",
                    "features": f"{col1}, {col2}",
                    "raw_total_count": int(len(rx)),
                    "syn_total_count": int(len(sx)),
                    "min_nonzero_raw_count": min_r if min_r is not None else 0,
                    "min_nonzero_syn_count": min_s if min_s is not None else 0,
                }
            )

    excel_path = join(output_dir, filename)
    sheets = {
        "0_Figures_Summary": pd.DataFrame(summary_rows),
        "1D_Histograms": pd.DataFrame(hist_1d_rows),
        "1D_Categories": pd.DataFrame(cat_1d_rows),
        "2D_Cat_Cat": pd.DataFrame(cat_cat_2d_rows),
        "2D_Cat_Cont": pd.DataFrame(cat_cont_2d_rows),
        "2D_Cont_Cont_Density": pd.DataFrame(cont_cont_2d_rows),
    }
    with pd.ExcelWriter(excel_path) as writer:
        for sheet_name, df_sheet in sheets.items():
            df_sheet.to_excel(writer, sheet_name=sheet_name[:31], index=False)
        _format_excel_workbook(writer)

    return excel_path
