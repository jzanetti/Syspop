from os import makedirs
from os.path import basename, dirname, exists, join
from itertools import combinations
import numpy as np
import pandas as pd
from scipy import stats

from syspop.confidential.aggregate import build_unit_hierarchy_table
from syspop.confidential.config import (
    load_config,
    resolve_data_structure,
    resolve_verification_config,
)
from syspop.confidential.report import _format_excel_workbook
from syspop.postp.vis import (
    _is_categorical,
    compute_idi_safe_1d_bins,
    compute_idi_safe_2d_bins,
)


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
    df_tmp = pd.DataFrame(
        {"cat": categories.astype(str), "val": pd.to_numeric(values, errors="coerce")}
    ).dropna()
    if len(df_tmp) < 2:
        return 0.0
    overall_mean = df_tmp["val"].mean()
    ss_total = float(((df_tmp["val"] - overall_mean) ** 2).sum())
    if ss_total <= 0.0:
        return 0.0
    grouped = df_tmp.groupby("cat")["val"]
    ss_between = float((grouped.count() * ((grouped.mean() - overall_mean) ** 2)).sum())
    return float(np.sqrt(np.clip(ss_between / ss_total, 0.0, 1.0)))


def _contingency_tvd(
    x_raw: pd.Series, y_raw: pd.Series, x_syn: pd.Series, y_syn: pd.Series
) -> float:
    p_raw = pd.crosstab(x_raw.astype(str), y_raw.astype(str), normalize=True)
    p_syn = pd.crosstab(x_syn.astype(str), y_syn.astype(str), normalize=True)
    all_idx = p_raw.index.union(p_syn.index)
    all_cols = p_raw.columns.union(p_syn.columns)
    p_raw = p_raw.reindex(index=all_idx, columns=all_cols, fill_value=0.0)
    p_syn = p_syn.reindex(index=all_idx, columns=all_cols, fill_value=0.0)
    return float(0.5 * np.abs(p_raw.to_numpy() - p_syn.to_numpy()).sum())


def build_metric_guide_df() -> pd.DataFrame:
    """Reference guide explaining the synthetic data verification report."""
    rows = [
        (
            "1_Numeric_Summary",
            "1D numeric comparison across hierarchy levels: raw vs. synthetic count, mean, std, and median.",
        ),
        (
            "2_Category_Summary",
            "1D category and discrete-count comparison across hierarchy levels: raw vs. synthetic counts.",
        ),
        (
            "3_Pairwise_Correlations",
            "2D association comparison: Pearson r (numeric-numeric), Cramer's V (cat-cat), Correlation Ratio eta (cat-numeric).",
        ),
        (
            "status",
            "PASS = raw_count >= min_count (or 0); SUPPRESSED = raw_count is below the configured min_count threshold.",
        ),
    ]
    return pd.DataFrame(rows, columns=["Sheet / Column", "Description"])


def evaluate_synthetic_data(
    raw_df: pd.DataFrame,
    syn_df: pd.DataFrame,
    columns: list or None = None,
    diagnostics: dict or None = None,
    min_count: int = 6,
) -> dict:
    """
    Compute basic statistical comparison metrics (mean, std, median, category
    counts, and pairwise correlations) between `syn_df` and `raw_df`,
    with minimum-count checks (`min_count`).
    """
    if columns is None:
        eval_cols = [c for c in raw_df.columns if c in syn_df.columns]
    else:
        eval_cols = [c for c in columns if c in raw_df.columns and c in syn_df.columns]

    cont_cols = [
        c
        for c in eval_cols
        if _is_continuous_series(raw_df[c]) and _is_continuous_series(syn_df[c])
    ]
    cat_cols = [c for c in eval_cols if c not in cont_cols]
    # Also include discrete low-cardinality integer/count columns (e.g. family_num, individual_num) in category counts
    discrete_cat_cols = list(cat_cols)
    for col in cont_cols:
        r_num = pd.to_numeric(raw_df[col], errors="coerce").dropna()
        if (
            len(r_num) > 0
            and _is_categorical(r_num)
            and np.all(np.isclose(r_num, np.round(r_num)))
        ):
            discrete_cat_cols.append(col)

    # 1. Numeric 1D Summary (count, mean, std, median)
    cont_rows = []
    for col in cont_cols:
        r_vals = (
            pd.to_numeric(raw_df[col], errors="coerce").dropna().to_numpy(dtype=float)
        )
        s_vals = (
            pd.to_numeric(syn_df[col], errors="coerce").dropna().to_numpy(dtype=float)
        )
        if len(r_vals) == 0 or len(s_vals) == 0:
            continue

        rc, sc = int(len(r_vals)), int(len(s_vals))
        r_mean, s_mean = float(np.mean(r_vals)), float(np.mean(s_vals))
        r_std = float(np.std(r_vals, ddof=1)) if rc > 1 else 0.0
        s_std = float(np.std(s_vals, ddof=1)) if sc > 1 else 0.0
        r_med, s_med = float(np.median(r_vals)), float(np.median(s_vals))
        is_low = rc < min_count

        cont_rows.append(
            {
                "column": col,
                "raw_count": rc,
                "syn_count": sc,
                "raw_mean": np.nan if is_low else r_mean,
                "syn_mean": s_mean,
                "mean_diff": np.nan if is_low else (s_mean - r_mean),
                "raw_std": np.nan if is_low else r_std,
                "syn_std": s_std,
                "raw_median": np.nan if is_low else r_med,
                "syn_median": s_med,
                "status": "SUPPRESSED" if is_low else "PASS",
            }
        )
    cont_df = pd.DataFrame(cont_rows)

    # 2. Categorical / Discrete 1D Summary (counts only, with small-cell status)
    cat_rows = []
    for col in discrete_cat_cols:
        if col in cont_cols:
            r_ser = (
                pd.to_numeric(raw_df[col], errors="coerce")
                .dropna()
                .round()
                .astype(int)
                .astype(str)
            )
            s_ser = (
                pd.to_numeric(syn_df[col], errors="coerce")
                .dropna()
                .round()
                .astype(int)
                .astype(str)
            )
        else:
            r_ser = raw_df[col].dropna().astype(str)
            s_ser = syn_df[col].dropna().astype(str)
        r_cnts = r_ser.value_counts()
        s_cnts = s_ser.value_counts()
        all_cats = sorted(set(r_cnts.index).union(set(s_cnts.index)))
        if not all_cats:
            continue

        for k in all_cats:
            rc = int(r_cnts.get(k, 0))
            sc = int(s_cnts.get(k, 0))
            is_suppressed = 0 < rc < min_count
            cat_rows.append(
                {
                    "column": col,
                    "category": k,
                    "raw_count": rc,
                    "syn_count": sc,
                    "status": "SUPPRESSED" if is_suppressed else "PASS",
                }
            )
    cat_df = pd.DataFrame(cat_rows)

    # 3. Pairwise 2D Correlations (basic association metric per pair type)
    pair_rows = []
    for c1, c2 in combinations(eval_cols, 2):
        c1_cont = c1 in cont_cols
        c2_cont = c2 in cont_cols
        if c1_cont and c2_cont:
            r_pair = raw_df[[c1, c2]].apply(pd.to_numeric, errors="coerce").dropna()
            s_pair = syn_df[[c1, c2]].apply(pd.to_numeric, errors="coerce").dropna()
            rc, sc = int(len(r_pair)), int(len(s_pair))
            is_low = rc < min_count
            if not is_low and rc >= 2 and r_pair[c1].std() > 0 and r_pair[c2].std() > 0:
                v_raw = float(stats.pearsonr(r_pair[c1], r_pair[c2]).statistic)
            else:
                v_raw = np.nan
            if sc >= 2 and s_pair[c1].std() > 0 and s_pair[c2].std() > 0:
                v_syn = float(stats.pearsonr(s_pair[c1], s_pair[c2]).statistic)
            else:
                v_syn = np.nan
            pair_type, metric_name = "numeric-numeric", "Pearson r"
        elif (not c1_cont) and (not c2_cont):
            r_pair = raw_df[[c1, c2]].dropna()
            s_pair = syn_df[[c1, c2]].dropna()
            rc, sc = int(len(r_pair)), int(len(s_pair))
            is_low = rc < min_count
            v_raw = np.nan if is_low else _cramers_v(raw_df[c1], raw_df[c2])
            v_syn = _cramers_v(syn_df[c1], syn_df[c2])
            pair_type, metric_name = "categorical-categorical", "Cramer's V"
        else:
            cat_c, cont_c = (c1, c2) if not c1_cont else (c2, c1)
            r_pair = pd.DataFrame(
                {
                    "cat": raw_df[cat_c],
                    "val": pd.to_numeric(raw_df[cont_c], errors="coerce"),
                }
            ).dropna()
            s_pair = pd.DataFrame(
                {
                    "cat": syn_df[cat_c],
                    "val": pd.to_numeric(syn_df[cont_c], errors="coerce"),
                }
            ).dropna()
            rc, sc = int(len(r_pair)), int(len(s_pair))
            is_low = rc < min_count
            v_raw = (
                np.nan if is_low else _correlation_ratio(raw_df[cat_c], raw_df[cont_c])
            )
            v_syn = _correlation_ratio(syn_df[cat_c], syn_df[cont_c])
            pair_type, metric_name = "categorical-numeric", "Correlation Ratio (eta)"

        pair_rows.append(
            {
                "feature_1": c1,
                "feature_2": c2,
                "pair_type": pair_type,
                "metric": metric_name,
                "raw_count": rc,
                "syn_count": sc,
                "raw_value": v_raw,
                "syn_value": v_syn,
                "status": "SUPPRESSED" if is_low else "PASS",
            }
        )
    pairs_df = pd.DataFrame(pair_rows)

    return {
        "metric_guide": build_metric_guide_df(),
        "numeric_summary": cont_df,
        "category_summary": cat_df,
        "pairwise_correlations": pairs_df,
    }


def build_verification_level_datasets(
    raw_df: pd.DataFrame,
    syn_df: pd.DataFrame,
    config_or_path=None,
    columns: list or None = None,
) -> dict:
    """
    Build comparable `(raw_df, syn_df, eval_columns, plot_columns)` dictionaries for
    each configured hierarchy level (e.g., `individual`, `family`, `household`).

    Higher levels (`family`, `household`) are rolled up from `raw_df` and `syn_df`
    using the exact same `build_unit_hierarchy_table` logic as Step 2 aggregation.
    """
    if columns is None:
        base_cols = [c for c in raw_df.columns if c in syn_df.columns]
    else:
        col_list = [columns] if isinstance(columns, str) else list(columns)
        base_cols = [c for c in col_list if c in raw_df.columns and c in syn_df.columns]

    cfg = load_config(config_or_path)
    data_struct = resolve_data_structure(raw_df, cfg)
    hierarchy = data_struct["hierarchy"]
    id_columns = data_struct["id_columns"]
    features_meta = data_struct["features"]
    hierarchy_counts = data_struct.get("hierarchy_counts", {})

    # Restrict features_meta to requested columns (if specified) while preserving types/agg
    if columns is not None:
        active_meta = {k: v for k, v in features_meta.items() if k in base_cols}
    else:
        id_set = {id_columns.get(lvl) for lvl in hierarchy if id_columns.get(lvl)}
        active_meta = {
            k: v for k, v in features_meta.items() if k in base_cols and k not in id_set
        }

    base_lvl = hierarchy[0] if hierarchy else "individual"
    ind_cols = [c for c in base_cols if c in raw_df.columns and c in syn_df.columns]
    level_datasets = {
        base_lvl: {
            "raw_df": raw_df,
            "syn_df": syn_df,
            "eval_columns": ind_cols,
            "plot_columns": ind_cols,
        }
    }

    # Build shared categorical mappings across raw & synthetic so higher-level tables align
    cat_mappings = {}
    for feat_name, spec in active_meta.items():
        if (
            spec.get("type") == "categorical"
            and feat_name in raw_df.columns
            and feat_name in syn_df.columns
        ):
            cats = sorted(
                set(raw_df[feat_name].fillna("Missing").astype(str)).union(
                    set(syn_df[feat_name].fillna("Missing").astype(str))
                )
            )
            cat_mappings[feat_name] = {
                "categories": cats,
                "columns": [f"{feat_name}_{c}" for c in cats],
            }
    cat_one_hot_cols = [
        col_lbl for m in cat_mappings.values() for col_lbl in m["columns"]
    ]

    raw_sources = {base_lvl: raw_df}
    syn_sources = {base_lvl: syn_df}

    for lvl_idx in range(1, len(hierarchy)):
        lvl = hierarchy[lvl_idx]
        child_lvl = hierarchy[lvl_idx - 1]
        lvl_id_col = id_columns.get(lvl, f"{lvl}_id")
        if lvl_id_col not in raw_df.columns or lvl_id_col not in syn_df.columns:
            continue

        group_cols = []
        for anc_lvl in reversed(hierarchy[lvl_idx:]):
            anc_col = id_columns.get(anc_lvl, f"{anc_lvl}_id")
            if anc_col in raw_df.columns and anc_col in syn_df.columns and anc_col not in group_cols:
                group_cols.append(anc_col)
        if not group_cols:
            group_cols = [lvl_id_col]

        child_id_col = id_columns.get(child_lvl, f"{child_lvl}_id") if lvl_idx > 1 else None
        if child_id_col is not None and (child_id_col not in raw_df.columns or child_id_col not in syn_df.columns):
            child_id_col = None
        child_count_name = f"{child_lvl}_num"

        raw_lvl_df, lvl_meta = build_unit_hierarchy_table(
            df=raw_df,
            group_id_cols=group_cols,
            features_meta=active_meta,
            cat_mappings=cat_mappings,
            child_id_col=child_id_col,
            child_count_col=child_count_name,
            hierarchy_counts=hierarchy_counts,
            level_name=lvl,
            source_tables_by_level=raw_sources,
        )
        syn_lvl_df, _ = build_unit_hierarchy_table(
            df=syn_df,
            group_id_cols=group_cols,
            features_meta=active_meta,
            cat_mappings=cat_mappings,
            child_id_col=child_id_col,
            child_count_col=child_count_name,
            hierarchy_counts=hierarchy_counts,
            level_name=lvl,
            source_tables_by_level=syn_sources,
        )
        raw_sources[lvl] = raw_lvl_df
        syn_sources[lvl] = syn_lvl_df

        lvl_plot_cols = [
            c
            for c, spec in lvl_meta.items()
            if spec.get("type") == "continuous"
            and c in raw_lvl_df.columns
            and c in syn_lvl_df.columns
        ]
        lvl_eval_cols = lvl_plot_cols + [
            c
            for c in cat_one_hot_cols
            if c in raw_lvl_df.columns and c in syn_lvl_df.columns
        ]
        level_datasets[lvl] = {
            "raw_df": raw_lvl_df,
            "syn_df": syn_lvl_df,
            "eval_columns": lvl_eval_cols,
            "plot_columns": lvl_plot_cols,
        }

    return level_datasets


def _infer_figure_level_and_label(fig_path: str, valid_levels: list or None = None) -> tuple:
    """Extract `(level, grouped_label)` from a verification figure path."""
    parent_dir = dirname(fig_path)
    parent_name = basename(parent_dir)
    grandparent_name = basename(dirname(parent_dir))
    fname = basename(fig_path)
    if grandparent_name == "figures" or (valid_levels and parent_name in valid_levels):
        return parent_name, f"{parent_name}/{fname}"
    base_lvl = valid_levels[0] if valid_levels else "unit"
    return base_lvl, fname


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
    include_hierarchy: bool = True,
    min_count: int or None = None,
) -> str:
    """
    Export a concise, IDI-checker-friendly Excel verification report comparing
    synthetic data against raw data across all configured hierarchy levels
    (basic stats: mean, std, median, category proportions, pairwise correlations,
    and IDI min_count checks).
    """
    if not exists(output_dir):
        makedirs(output_dir)

    if min_count is None:
        verif_cfg = (
            resolve_verification_config(config_or_path)
            if config_or_path is not None
            else {}
        )
        min_count = int(verif_cfg.get("min_count", 6))

    if include_hierarchy:
        level_datasets = build_verification_level_datasets(
            raw_df=raw_df,
            syn_df=syn_df,
            config_or_path=config_or_path,
            columns=columns,
        )
    else:
        eval_cols = (
            [c for c in raw_df.columns if c in syn_df.columns]
            if columns is None
            else [
                c
                for c in ([columns] if isinstance(columns, str) else columns)
                if c in raw_df.columns and c in syn_df.columns
            ]
        )
        level_datasets = {
            "individual": {
                "raw_df": raw_df,
                "syn_df": syn_df,
                "eval_columns": eval_cols,
                "plot_columns": eval_cols,
            }
        }

    num_parts = []
    cat_parts = []
    pair_parts = []
    metric_guide_df = build_metric_guide_df()

    for lvl, spec in level_datasets.items():
        res = evaluate_synthetic_data(
            raw_df=spec["raw_df"],
            syn_df=spec["syn_df"],
            columns=spec["eval_columns"],
            min_count=min_count,
        )
        for target_list, key in [
            (num_parts, "numeric_summary"),
            (cat_parts, "category_summary"),
            (pair_parts, "pairwise_correlations"),
        ]:
            df_part = res[key]
            if len(df_part) > 0:
                df_part = df_part.copy()
                df_part.insert(0, "level", lvl)
                target_list.append(df_part)

    excel_path = join(output_dir, filename)
    sheets = {
        "0_Guide": metric_guide_df,
        "1_Numeric_Summary": (
            pd.concat(num_parts, ignore_index=True) if num_parts else pd.DataFrame()
        ),
        "2_Category_Summary": (
            pd.concat(cat_parts, ignore_index=True) if cat_parts else pd.DataFrame()
        ),
        "3_Pairwise_Correlations": (
            pd.concat(pair_parts, ignore_index=True) if pair_parts else pd.DataFrame()
        ),
    }

    with pd.ExcelWriter(excel_path) as writer:
        for sheet_name, df_sheet in sheets.items():
            df_sheet.to_excel(writer, sheet_name=sheet_name[:31], index=False)
        _format_excel_workbook(writer)

    return excel_path


def export_verification_figures_counts(
    raw_df: pd.DataFrame,
    syn_df: pd.DataFrame,
    columns: list or None,
    output_dir: str,
    focus_column: str or None = None,
    min_count: int or None = None,
    filename: str = "verification_figures_counts.xlsx",
    config_or_path=None,
    include_hierarchy: bool = True,
) -> str:
    """
    Export an Excel workbook (`verification_figures_counts.xlsx`) recording the exact
    underlying raw and synthetic record counts used in every bin, bar, group, and
    2D density cell of the verification figures across all hierarchy levels.
    """
    if min_count is None:
        verif_cfg = (
            resolve_verification_config(config_or_path)
            if config_or_path is not None
            else {}
        )
        min_count = int(verif_cfg.get("min_count", 6))

    if not exists(output_dir):
        makedirs(output_dir)

    if include_hierarchy:
        level_datasets = build_verification_level_datasets(
            raw_df=raw_df,
            syn_df=syn_df,
            config_or_path=config_or_path,
            columns=columns,
        )
    else:
        col_list = (
            [c for c in raw_df.columns if c in syn_df.columns]
            if columns is None
            else ([columns] if isinstance(columns, str) else list(columns))
        )
        eval_cols = [c for c in col_list if c in raw_df.columns and c in syn_df.columns]
        level_datasets = {
            "individual": {
                "raw_df": raw_df,
                "syn_df": syn_df,
                "eval_columns": eval_cols,
                "plot_columns": eval_cols,
            }
        }

    use_grouped = len(level_datasets) > 1 or config_or_path is not None

    hist_1d_rows = []
    cat_1d_rows = []
    cat_cat_2d_rows = []
    cat_cont_2d_rows = []
    cont_cont_2d_rows = []

    for lvl, spec in level_datasets.items():
        lvl_raw = spec["raw_df"]
        lvl_syn = spec["syn_df"]
        eval_cols = spec["plot_columns"]
        if not eval_cols:
            continue

        fig_1d_name = (
            f"{lvl}/1d_distributions.png"
            if use_grouped
            else "comparison_raw_vs_synthetic.png"
        )

        # 1. 1D comparison figure counts (matching vis.plot_comparison)
        for col in eval_cols:
            if not _is_categorical(lvl_raw[col]):
                r_vals = (
                    pd.to_numeric(lvl_raw[col], errors="coerce")
                    .dropna()
                    .to_numpy(dtype=float)
                )
                s_vals = (
                    pd.to_numeric(lvl_syn[col], errors="coerce")
                    .dropna()
                    .to_numpy(dtype=float)
                )
                if len(r_vals) == 0 or len(s_vals) == 0:
                    continue
                bins = compute_idi_safe_1d_bins(r_vals, s_vals, min_bin_count=min_count)
                r_cnts, _ = np.histogram(r_vals, bins=bins)
                s_cnts, _ = np.histogram(s_vals, bins=bins)
                for b_i in range(len(r_cnts)):
                    rc = int(r_cnts[b_i])
                    sc = int(s_cnts[b_i])
                    hist_1d_rows.append(
                        {
                            "level": lvl,
                            "figure_file": fig_1d_name,
                            "column": col,
                            "bin_lower": float(bins[b_i]),
                            "bin_upper": float(bins[b_i + 1]),
                            "raw_count": rc,
                            "syn_count": sc,
                            "status": (
                                "PASS" if (rc == 0 or rc >= min_count) else "SUPPRESSED"
                            ),
                        }
                    )
            else:
                r_ser = lvl_raw[col].dropna().astype(str)
                s_ser = lvl_syn[col].dropna().astype(str)
                r_cnts = r_ser.value_counts()
                s_cnts = s_ser.value_counts()
                cats = sorted(set(r_cnts.index).union(set(s_cnts.index)))
                for k in cats:
                    rc = int(r_cnts.get(k, 0))
                    sc = int(s_cnts.get(k, 0))
                    is_suppressed = 0 < rc < min_count
                    cat_1d_rows.append(
                        {
                            "level": lvl,
                            "figure_file": fig_1d_name,
                            "column": col,
                            "category": k,
                            "raw_count": rc,
                            "syn_count": sc,
                            "status": "SUPPRESSED" if is_suppressed else "PASS",
                        }
                    )

        # 2. 2D cross-comparison figure counts (matching vis.plot_cross_comparison)
        if focus_column is not None and focus_column in eval_cols:
            pairs = [(focus_column, c) for c in eval_cols if c != focus_column]
        else:
            pairs = list(combinations(eval_cols, 2))

        for col1, col2 in pairs:
            c1_cont = not _is_categorical(lvl_raw[col1])
            c2_cont = not _is_categorical(lvl_raw[col2])
            if use_grouped:
                if (not c1_cont) and (not c2_cont):
                    prefix = "2d_cat_cat"
                elif (not c1_cont) or (not c2_cont):
                    prefix = "2d_cat_cont"
                else:
                    prefix = "2d_cont_cont"
                fig_name = f"{lvl}/{prefix}_{col1}_vs_{col2}.png"
            else:
                fig_name = f"comparison_{col1}_{col2}.png"

            if (not c1_cont) and (not c2_cont):
                r_pair = lvl_raw[[col1, col2]].dropna()
                s_pair = lvl_syn[[col1, col2]].dropna()
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
                        cat_cat_2d_rows.append(
                            {
                                "level": lvl,
                                "figure_file": fig_name,
                                "feature_1": col1,
                                "feature_2": col2,
                                "value_1": v1,
                                "value_2": v2,
                                "raw_count": rc,
                                "syn_count": sc,
                                "status": "SUPPRESSED" if is_suppressed else "PASS",
                            }
                        )
            elif (not c1_cont) or (not c2_cont):
                cat_c, cont_c = (col1, col2) if not c1_cont else (col2, col1)
                r_sub = pd.DataFrame(
                    {
                        "cat": lvl_raw[cat_c].astype(str),
                        "val": pd.to_numeric(lvl_raw[cont_c], errors="coerce"),
                    }
                ).dropna()
                s_sub = pd.DataFrame(
                    {
                        "cat": lvl_syn[cat_c].astype(str),
                        "val": pd.to_numeric(lvl_syn[cont_c], errors="coerce"),
                    }
                ).dropna()
                cats = sorted(set(r_sub["cat"]).union(set(s_sub["cat"])))
                for k in cats:
                    rv = r_sub.loc[r_sub["cat"] == k, "val"]
                    sv = s_sub.loc[s_sub["cat"] == k, "val"]
                    rc, sc = int(len(rv)), int(len(sv))
                    is_suppressed = 0 < rc < min_count
                    cat_cont_2d_rows.append(
                        {
                            "level": lvl,
                            "figure_file": fig_name,
                            "cat_feature": cat_c,
                            "cont_feature": cont_c,
                            "cat_value": k,
                            "cont_raw_mean": (
                                np.nan
                                if is_suppressed
                                else (float(rv.mean()) if rc > 0 else np.nan)
                            ),
                            "cont_syn_mean": float(sv.mean()) if sc > 0 else np.nan,
                            "cont_raw_std": (
                                np.nan
                                if is_suppressed
                                else (float(rv.std()) if rc > 1 else np.nan)
                            ),
                            "cont_syn_std": float(sv.std()) if sc > 1 else np.nan,
                            "cont_raw_median": (
                                np.nan
                                if is_suppressed
                                else (float(rv.median()) if rc > 0 else np.nan)
                            ),
                            "cont_syn_median": float(sv.median()) if sc > 0 else np.nan,
                            "raw_count": rc,
                            "syn_count": sc,
                            "status": "SUPPRESSED" if is_suppressed else "PASS",
                        }
                    )
            else:
                rx = pd.to_numeric(lvl_raw[col1], errors="coerce").to_numpy(dtype=float)
                ry = pd.to_numeric(lvl_raw[col2], errors="coerce").to_numpy(dtype=float)
                sx = pd.to_numeric(lvl_syn[col1], errors="coerce").to_numpy(dtype=float)
                sy = pd.to_numeric(lvl_syn[col2], errors="coerce").to_numpy(dtype=float)
                rmask = ~np.isnan(rx) & ~np.isnan(ry)
                smask = ~np.isnan(sx) & ~np.isnan(sy)
                rx, ry = rx[rmask], ry[rmask]
                sx, sy = sx[smask], sy[smask]
                if len(rx) == 0 or len(sx) == 0:
                    continue
                x_bins, y_bins = compute_idi_safe_2d_bins(
                    rx, ry, sx, sy, min_bin_count=min_count
                )
                h_raw_cnt, _, _ = np.histogram2d(rx, ry, bins=[x_bins, y_bins])
                h_syn_cnt, _, _ = np.histogram2d(sx, sy, bins=[x_bins, y_bins])
                h_raw_den, _, _ = np.histogram2d(
                    rx, ry, bins=[x_bins, y_bins], density=True
                )
                h_syn_den, _, _ = np.histogram2d(
                    sx, sy, bins=[x_bins, y_bins], density=True
                )
                for xi in range(len(x_bins) - 1):
                    for yi in range(len(y_bins) - 1):
                        rc = int(h_raw_cnt[xi, yi])
                        sc = int(h_syn_cnt[xi, yi])
                        if rc == 0 and sc == 0:
                            continue
                        is_suppressed = 0 < rc < min_count
                        cont_cont_2d_rows.append(
                            {
                                "level": lvl,
                                "figure_file": fig_name,
                                "feature_1": col1,
                                "feature_2": col2,
                                "bin_lower_1": float(x_bins[xi]),
                                "bin_upper_1": float(x_bins[xi + 1]),
                                "bin_lower_2": float(y_bins[yi]),
                                "bin_upper_2": float(y_bins[yi + 1]),
                                "raw_density": (
                                    np.nan
                                    if is_suppressed
                                    else float(h_raw_den[xi, yi])
                                ),
                                "syn_density": float(h_syn_den[xi, yi]),
                                "raw_count": rc,
                                "syn_count": sc,
                                "status": "SUPPRESSED" if is_suppressed else "PASS",
                            }
                        )

    excel_path = join(output_dir, filename)
    sheets = {
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
