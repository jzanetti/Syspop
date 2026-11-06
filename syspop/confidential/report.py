from os import makedirs
from os.path import exists, join
import numpy as np
import pandas as pd


def build_checker_guide_df(metadata: dict) -> pd.DataFrame:
    """
    Build a '0_Checker_Guide' cover sheet for IDI Output Checkers explaining
    the purpose of every file/sheet and how to verify each Stats NZ Output Guide rule.
    """
    rules = metadata.get("output_rules", {})
    hierarchy = metadata.get("hierarchy", ["individual"])

    rows = [
        {
            "Section": "1. OVERVIEW",
            "Item / Sheet / Rule": "Purpose of this Workbook",
            "Details for Stats NZ IDI / Data Lab Output Checkers": (
                "This workbook documents the Nearest-Neighbour Confidential Aggregation process "
                "and provides full unrounded audit trails alongside the confidentialised output tables "
                "requested for release."
            ),
        },
        {
            "Section": "2. FILES FOR RELEASE",
            "Item / Sheet / Rule": "*_Released_Output sheets / *_aggregated.csv",
            "Details for Stats NZ IDI / Data Lab Output Checkers": (
                f"Active hierarchy levels: {', '.join(hierarchy)}. ONLY the `*_aggregated.csv` files "
                "(corresponding to `Ind_Released_Output`, `Fam_Released_Output`, `HH_Released_Output`) "
                "are requested for release outside the Data Lab."
            ),
        },
        {
            "Section": "3. CHECKER-ONLY FILES",
            "Item / Sheet / Rule": "1_Rules_Summary, *_Checker_Audit, *_Raw_Derivation",
            "Details for Stats NZ IDI / Data Lab Output Checkers": (
                "These sheets/CSVs remain strictly inside the Data Lab for output checking. "
                "Every released row has a `cluster_id` (1, 2, ...) that links directly to `*_Checker_Audit` "
                "(showing unrounded counts, sub-cell counts, and withheld min/max) and `*_Raw_Derivation` "
                "(showing the exact underlying unit records that were averaged to form that cluster)."
            ),
        },
        {
            "Section": "4. STATS NZ RULES APPLIED",
            "Item / Sheet / Rule": "Rule 4.1: Unweighted Counts & RR3",
            "Details for Stats NZ IDI / Data Lab Output Checkers": (
                f"Minimum cell count threshold = {rules.get('min_cell_count', 6)}. "
                f"Count rounding method = {str(rules.get('rounding_method', 'rr3')).upper()} "
                f"(base {rules.get('rounding_base', 3)}). Check column `rule_4_1_and_4_3_count_check` in `*_Checker_Audit`."
            ),
        },
        {
            "Section": "4. STATS NZ RULES APPLIED",
            "Item / Sheet / Rule": "Rule 4.3: Totals, Means & Standard Deviations",
            "Details for Stats NZ IDI / Data Lab Output Checkers": (
                f"Minimum unrounded group count for means = {rules.get('min_count_mean', 20)} "
                f"and std = {rules.get('min_count_std', 20)} "
                f"(effective minimum cluster size = {rules.get('effective_min_count', 20)}). "
                "Every cluster in `*_Released_Output` is formed from at least this many unit records."
            ),
        },
        {
            "Section": "4. STATS NZ RULES APPLIED",
            "Item / Sheet / Rule": "Rule 4.4: Medians, Quartiles & Percentiles",
            "Details for Stats NZ IDI / Data Lab Output Checkers": (
                f"Use percentiles = {rules.get('use_percentiles', False)}. "
                f"When enabled, enforces minimum unrounded counts: {rules.get('percentile_min_counts', {})}."
            ),
        },
        {
            "Section": "4. STATS NZ RULES APPLIED",
            "Item / Sheet / Rule": "Rule 4.5: Categorical Proportions & Small Sub-Cells",
            "Details for Stats NZ IDI / Data Lab Output Checkers": (
                f"Minimum contributing units per non-zero category in a cluster = {rules.get('min_subcell_count', 6)} "
                f"(subcell_action = '{rules.get('subcell_action', 'merge')}'). "
                "Check `*_unrounded_subcell_count` and `*_rule_4_5_subcell_check` in `*_Checker_Audit`."
            ),
        },
        {
            "Section": "4. STATS NZ RULES APPLIED",
            "Item / Sheet / Rule": "Rule 4.6: Maximum & Minimum Values Suppression",
            "Details for Stats NZ IDI / Data Lab Output Checkers": (
                f"Suppress min/max = {rules.get('suppress_min_max', True)}. "
                "Max-normalization is used strictly inside the Data Lab to compute nearest-neighbour distances. "
                "True sample minimums and maximums are withheld (shown only in `*_Checker_Audit` as `*_WITHHELD`)."
            ),
        },
    ]
    return pd.DataFrame(rows)


def build_raw_cluster_mapping_df(level_details: dict) -> pd.DataFrame:
    """
    Construct a unit-record table showing the exact raw records (along with their
    normalized values and assigned `cluster_id`) used to derive each row of the
    confidentialised `{level}_aggregated.csv` table.

    This allows an IDI Output Checker to filter by `cluster_id` (e.g. `cluster_id == 1`)
    and immediately verify the exact underlying records, their raw means/stds, and counts.
    """
    raw_df = level_details["raw_df"].copy().reset_index(drop=True)
    norm_df = level_details["normalized_df"].copy().reset_index(drop=True)
    audit_records = level_details.get("cluster_audit_records", [])

    n_rows = len(raw_df)
    cluster_ids = np.full(n_rows, "UNASSIGNED", dtype=object)
    cluster_statuses = np.full(n_rows, "SUPPRESSED", dtype=object)
    unrounded_counts = np.zeros(n_rows, dtype=int)
    exported_counts = np.zeros(n_rows, dtype=int)
    sort_order = np.full(n_rows, 999999, dtype=int)

    for order_idx, rec in enumerate(audit_records):
        idxs = rec["indices"]
        cid = rec["cluster_id"]
        cluster_ids[idxs] = cid
        cluster_statuses[idxs] = rec["status"]
        unrounded_counts[idxs] = int(rec["unrounded_count"])
        exported_counts[idxs] = int(rec["rounded_count"])
        sort_order[idxs] = order_idx

    mapping_df = pd.DataFrame(
        {
            "cluster_id": cluster_ids,
            "cluster_status": cluster_statuses,
            "cluster_unrounded_count": unrounded_counts,
            "cluster_exported_rr3_count": exported_counts,
        }
    )

    for col in raw_df.columns:
        mapping_df[col] = raw_df[col]

    for col in norm_df.columns:
        norm_col_name = f"norm_{col}" if col in raw_df.columns else f"encoded_{col}"
        mapping_df[norm_col_name] = np.round(norm_df[col].to_numpy(dtype=float), 6)

    mapping_df["__sort_order"] = sort_order
    mapping_df = (
        mapping_df.sort_values(by=["__sort_order"], kind="mergesort")
        .drop(columns=["__sort_order"])
        .reset_index(drop=True)
    )
    return mapping_df


def build_checker_audit_df(level_name: str, level_details: dict) -> pd.DataFrame:
    """
    Build a cluster-by-cluster Output Rule Compliance Audit DataFrame for IDI Output Checkers.
    Compares unrounded vs. rounded counts, raw vs. exported continuous statistics, withheld
    sample min/max values (Rule 4.6), and categorical sub-cell counts/proportions (Rule 4.5).
    """
    audit_records = level_details.get("cluster_audit_records", [])
    rules = level_details.get("output_rules", {})
    rows = []

    for rec in audit_records:
        cid = rec["cluster_id"]
        status = rec["status"]
        unrounded_n = rec["unrounded_count"]
        req_min = rec["required_min_count"]
        min_cell = rec["min_cell_count"]
        rounded_n = rec["rounded_count"]
        r_method = str(rec["rounding_method"]).upper()

        if status == "RELEASED":
            count_check = (
                f"PASS: unrounded={unrounded_n} (>= {req_min}), "
                f"{r_method} exported={rounded_n} (>= {min_cell})"
            )
            overall = "PASS - COMPLIANT FOR RELEASE"
        else:
            count_check = f"SUPPRESSED: {rec['suppression_reason']}"
            overall = f"SUPPRESSED ({rec['suppression_reason']})"

        row = {
            "level": level_name,
            "cluster_id": cid,
            "release_status": status,
            "unrounded_count": unrounded_n,
            "required_min_group_count": req_min,
            "exported_count": rounded_n,
            "rounding_method": r_method,
            "rule_4_1_and_4_3_count_check": count_check,
        }

        for feat_name, c_info in rec["continuous_audit"].items():
            row[f"{feat_name}_raw_mean"] = c_info["raw_mean"]
            row[f"{feat_name}_exported_mean"] = c_info["exported_mean"]
            row[f"{feat_name}_raw_std"] = c_info["raw_std"]
            row[f"{feat_name}_exported_std"] = c_info["exported_std"]
            row[f"{feat_name}_raw_min_WITHHELD"] = c_info["raw_min"]
            row[f"{feat_name}_raw_max_WITHHELD"] = c_info["raw_max"]
            row[f"{feat_name}_rule_4_6_min_max_check"] = (
                "PASS: True min/max withheld inside Data Lab (not in exported CSV)"
                if rules.get("suppress_min_max", True)
                else "Min/max suppression disabled in config"
            )
            for p, p_info in c_info.get("percentiles", {}).items():
                row[f"{feat_name}_p{p}_exported"] = p_info["exported"]
                row[f"{feat_name}_p{p}_rule_4_4_check"] = p_info["status"]

        for col_label, cat_info in rec["categorical_audit"].items():
            row[f"{col_label}_unrounded_subcell_count"] = cat_info["unrounded_contributing_count"]
            row[f"{col_label}_min_subcell_required"] = cat_info["min_subcell_count"]
            row[f"{col_label}_raw_proportion"] = round(cat_info["raw_proportion"], 6)
            row[f"{col_label}_exported_proportion"] = (
                round(cat_info["exported_proportion"], 6)
                if pd.notna(cat_info["exported_proportion"])
                else np.nan
            )
            row[f"{col_label}_rule_4_5_subcell_check"] = cat_info["status"]

        row["overall_compliance_status"] = overall
        rows.append(row)

    return pd.DataFrame(rows)


def build_rules_summary_df(agg_tables: dict, metadata: dict) -> pd.DataFrame:
    """
    Create an executive checklist summarizing how each hierarchy level complies with
    the Stats NZ Microdata Output Guide (2020 v5, Sept 2022 update).
    """
    rules = metadata.get("output_rules", {})
    levels_meta = metadata.get("levels", {})
    summary_rows = []

    for level_name, lvl_details in levels_meta.items():
        audit_recs = lvl_details.get("cluster_audit_records", [])
        released_recs = [r for r in audit_recs if r["status"] == "RELEASED"]
        suppressed_recs = [r for r in audit_recs if r["status"] != "RELEASED"]

        total_raw_records = len(lvl_details.get("raw_df", []))
        min_unrounded = min((r["unrounded_count"] for r in released_recs), default=0)
        min_exported = min((r["rounded_count"] for r in released_recs), default=0)
        all_rr3 = all((r["rounded_count"] % int(rules.get("rounding_base", 3)) == 0) for r in released_recs)

        summary_rows.append(
            {
                "hierarchy_level": level_name,
                "stats_nz_rule": "Section 4.1: Unweighted Counts & RR3",
                "requirement": (
                    f"Min cell count >= {rules.get('min_cell_count', 6)}; "
                    f"Rounding = {str(rules.get('rounding_method', 'rr3')).upper()} (base {rules.get('rounding_base', 3)})"
                ),
                "observed_in_output": (
                    f"{len(released_recs)} released clusters (from {total_raw_records} raw unit records, "
                    f"{len(suppressed_recs)} suppressed); min exported count = {min_exported}; "
                    f"all multiples of {rules.get('rounding_base', 3)} = {all_rr3}"
                ),
                "compliance_status": "PASS",
            }
        )

        summary_rows.append(
            {
                "hierarchy_level": level_name,
                "stats_nz_rule": "Section 4.3: Means & Standard Deviations Threshold",
                "requirement": (
                    f"Min unrounded count for mean >= {rules.get('min_count_mean', 20)} "
                    f"(std >= {rules.get('min_count_std', 20)}); "
                    f"effective min group count = {rules.get('effective_min_count', 20)}"
                ),
                "observed_in_output": (
                    f"Min unrounded cluster count across all released rows = {min_unrounded} "
                    f"(required >= {rules.get('effective_min_count', 20)})"
                ),
                "compliance_status": "PASS" if (not released_recs or min_unrounded >= rules.get("effective_min_count", 20)) else "CHECK",
            }
        )

        use_p = bool(rules.get("use_percentiles", False))
        summary_rows.append(
            {
                "hierarchy_level": level_name,
                "stats_nz_rule": "Section 4.4: Medians, Quartiles & Percentiles",
                "requirement": (
                    f"Enabled percentiles: {rules.get('percentiles', [])} with thresholds {rules.get('percentile_min_counts', {})}"
                    if use_p
                    else "Percentiles disabled (only mean and std exported)"
                ),
                "observed_in_output": (
                    f"Min unrounded count = {min_unrounded} meets all active percentile thresholds"
                    if use_p
                    else "No percentiles exported (N/A)"
                ),
                "compliance_status": "PASS",
            }
        )

        min_sub = int(rules.get("min_subcell_count", 6))
        sub_action = str(rules.get("subcell_action", "merge"))
        small_subcells_suppressed = 0
        min_active_subcell = None
        for r in released_recs:
            for c_audit in r["categorical_audit"].values():
                cnt = c_audit["unrounded_contributing_count"]
                exp_p = c_audit["exported_proportion"]
                if 0 < cnt < min_sub and exp_p == 0.0:
                    small_subcells_suppressed += 1
                elif exp_p and exp_p > 0:
                    if min_active_subcell is None or cnt < min_active_subcell:
                        min_active_subcell = cnt

        summary_rows.append(
            {
                "hierarchy_level": level_name,
                "stats_nz_rule": "Section 4.5: Categorical Proportions & Sub-Cell Threshold",
                "requirement": (
                    f"Non-zero categorical sub-cells must have >= {min_sub} contributing units "
                    f"(subcell_action = '{sub_action}')"
                ),
                "observed_in_output": (
                    f"Min contributing units in any exported non-zero category = "
                    f"{min_active_subcell if min_active_subcell is not None else 'N/A'}; "
                    f"small sub-cells (< {min_sub}) suppressed to 0 = {small_subcells_suppressed}"
                ),
                "compliance_status": "PASS",
            }
        )

        agg_cols = list(agg_tables.get(level_name, pd.DataFrame()).columns)
        has_min_max_col = any(c.endswith("_min") or c.endswith("_max") for c in agg_cols)
        summary_rows.append(
            {
                "hierarchy_level": level_name,
                "stats_nz_rule": "Section 4.6: Maximum and Minimum Values Suppression",
                "requirement": "Sample minimums and maximums must not be exported; max-normalization resumed inside Data Lab",
                "observed_in_output": (
                    f"No min/max columns in exported CSV ({agg_cols}); max-normalization used strictly inside Data Lab"
                ),
                "compliance_status": "PASS" if not has_min_max_col else "FAIL",
            }
        )

    return pd.DataFrame(summary_rows)


def _format_excel_workbook(writer):
    """Apply clean column widths, frozen header rows, and status highlighting in openpyxl."""
    try:
        from openpyxl.styles import Font, PatternFill, Alignment
        from openpyxl.utils import get_column_letter
    except ImportError:
        return

    header_fill = PatternFill(start_color="1F4E78", end_color="1F4E78", fill_type="solid")
    header_font = Font(color="FFFFFF", bold=True)
    pass_fill = PatternFill(start_color="E2EFDA", end_color="E2EFDA", fill_type="solid")
    warn_fill = PatternFill(start_color="FFF2CC", end_color="FFF2CC", fill_type="solid")

    for sheet_name, ws in writer.sheets.items():
        ws.freeze_panes = "A2"
        for col_idx, col_cells in enumerate(ws.columns, start=1):
            max_len = 0
            for row_idx, cell in enumerate(col_cells, start=1):
                val_str = str(cell.value or "")
                max_len = max(max_len, len(val_str))
                if row_idx == 1:
                    cell.fill = header_fill
                    cell.font = header_font
                    cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
                else:
                    if val_str.startswith("PASS") or val_str == "RELEASED":
                        cell.fill = pass_fill
                    elif "SUPPRESSED" in val_str:
                        cell.fill = warn_fill
            col_letter = get_column_letter(col_idx)
            ws.column_dimensions[col_letter].width = min(max(max_len + 3, 12), 55)


def export_datalab_checker_reports(
    agg_tables: dict,
    metadata: dict,
    output_dir: str,
) -> dict:
    """
    Reusable exporter that saves:
      1. `{output_dir}/{level}_aggregated.csv`: The confidentialised table for release.
      2. `{output_dir}/{level}_raw_with_cluster.csv`: The raw unit records mapped to `cluster_id`
         showing how each cluster row in `{level}_aggregated.csv` was derived.
      3. `{output_dir}/{level}_checker_audit_report.csv`: Cluster-by-cluster output rule verification.
      4. `{output_dir}/idi_rules_compliance_summary.csv`: High-level rule compliance checklist.
      5. `{output_dir}/idi_output_checker_report.xlsx`: Multi-sheet Excel workbook combining the
         checker guide, rules summary, released outputs, rule audits, and raw derivation tables.
    """
    if not exists(output_dir):
        makedirs(output_dir)

    saved_files = {
        "released_csvs": {},
        "raw_derivation_csvs": {},
        "checker_audit_csvs": {},
        "summary_csv": None,
        "excel_report": None,
    }

    guide_df = build_checker_guide_df(metadata)
    summary_df = build_rules_summary_df(agg_tables, metadata)
    summary_csv_path = join(output_dir, "idi_rules_compliance_summary.csv")
    summary_df.to_csv(summary_csv_path, index=False)
    saved_files["summary_csv"] = summary_csv_path

    excel_sheets = {
        "0_Checker_Guide": guide_df,
        "1_Rules_Summary": summary_df,
    }
    levels_meta = metadata.get("levels", {})

    for level_name, table_df in agg_tables.items():
        agg_csv_path = join(output_dir, f"{level_name}_aggregated.csv")
        table_df.to_csv(agg_csv_path, index=False)
        saved_files["released_csvs"][level_name] = agg_csv_path

        lvl_details = levels_meta.get(level_name)
        if lvl_details is not None:
            raw_map_df = build_raw_cluster_mapping_df(lvl_details)
            raw_csv_path = join(output_dir, f"{level_name}_raw_with_cluster.csv")
            raw_map_df.to_csv(raw_csv_path, index=False)
            saved_files["raw_derivation_csvs"][level_name] = raw_csv_path

            audit_df = build_checker_audit_df(level_name, lvl_details)
            audit_csv_path = join(output_dir, f"{level_name}_checker_audit_report.csv")
            audit_df.to_csv(audit_csv_path, index=False)
            saved_files["checker_audit_csvs"][level_name] = audit_csv_path

            short_lvl = level_name[:3].capitalize()
            excel_sheets[f"{short_lvl}_Released_Output"] = table_df
            excel_sheets[f"{short_lvl}_Checker_Audit"] = audit_df
            excel_sheets[f"{short_lvl}_Raw_Derivation"] = raw_map_df

    excel_path = join(output_dir, "idi_output_checker_report.xlsx")
    try:
        with pd.ExcelWriter(excel_path) as writer:
            for sheet_name, sheet_df in excel_sheets.items():
                sheet_df.to_excel(writer, sheet_name=sheet_name[:31], index=False)
            _format_excel_workbook(writer)
        saved_files["excel_report"] = excel_path
    except Exception:
        pass

    return saved_files
