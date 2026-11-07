from os import makedirs
from os.path import exists, join
import numpy as np
import pandas as pd


def build_checker_guide_df(metadata: dict) -> pd.DataFrame:
    """
    Build a concise '0_Checker_Guide' cover sheet explaining each sheet and configured rule.
    """
    rules = metadata.get("output_rules", {})
    hierarchy = metadata.get("hierarchy", ["individual"])
    r_base = rules.get("rounding_base", None)
    rounding_str = f"Base {r_base}" if (r_base is not None and int(r_base) > 1) else "Off (exact counts)"

    rows = [
        {
            "Sheet / Item": "1_Summary",
            "Description": (
                f"Overview of cluster counts, rounding ({rounding_str}), "
                f"and sub-cell thresholds across hierarchy levels ({', '.join(hierarchy)})."
            ),
        },
        {
            "Sheet / Item": "*_Released_Output (*_aggregated.csv)",
            "Description": "Confidentialised cluster tables requested for release outside the Data Lab.",
        },
        {
            "Sheet / Item": "*_Checker_Audit (*_checker_audit_report.csv)",
            "Description": (
                "Vertically structured audit table (one row per cluster and feature/category) "
                "comparing raw counts/values against configured thresholds and released values."
            ),
        },
        {
            "Sheet / Item": "*_Raw_Derivation (*_raw_with_cluster.csv)",
            "Description": (
                "Unit-record mapping showing the exact raw records and normalized values "
                "assigned to each cluster_id."
            ),
        },
        {
            "Sheet / Item": "Configured Thresholds",
            "Description": (
                f"min_group_count={rules.get('min_group_count', 20)}, "
                f"min_cell_count={rules.get('min_cell_count', 6)}, "
                f"rounding_base={rounding_str}, "
                f"subcell_action='{rules.get('subcell_action', 'reassign_or_merge')}', "
                f"leftover_action='{rules.get('leftover_action', 'assign_nearest')}'."
            ),
        },
    ]
    return pd.DataFrame(rows)


def build_raw_cluster_mapping_df(level_details: dict) -> pd.DataFrame:
    """
    Construct a unit-record table showing the exact raw records (along with their
    normalized values and assigned `cluster_id`) used to derive each row of `{level}_aggregated.csv`.
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
            "cluster_raw_count": unrounded_counts,
            "cluster_released_count": exported_counts,
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
    Build a vertically structured audit DataFrame where each row corresponds to
    one (cluster_id, feature, metric_or_category) entry.
    Columns remain fixed regardless of how many features exist in the dataset.
    """
    audit_records = level_details.get("cluster_audit_records", [])
    rows = []

    for rec in audit_records:
        cid = rec["cluster_id"]
        status = rec["status"]
        unrounded_n = int(rec["unrounded_count"])
        req_min = int(rec["min_group_count"])
        rounded_n = int(rec["rounded_count"])

        # 1. Cluster total count row
        rows.append(
            {
                "level": level_name,
                "cluster_id": cid,
                "feature": "cluster_count",
                "metric_or_category": "total_units",
                "raw_count": unrounded_n,
                "min_required": req_min,
                "raw_value": float(unrounded_n),
                "released_value": float(rounded_n) if status == "RELEASED" else np.nan,
                "status": "PASS" if status == "RELEASED" else "SUPPRESSED",
            }
        )

        # 2. Continuous features (mean and std rows)
        for feat_name, c_info in rec["continuous_audit"].items():
            f_cnt = int(c_info.get("raw_count", unrounded_n))
            f_status = "PASS" if status == "RELEASED" else "SUPPRESSED"
            rows.append(
                {
                    "level": level_name,
                    "cluster_id": cid,
                    "feature": feat_name,
                    "metric_or_category": "mean",
                    "raw_count": f_cnt,
                    "min_required": int(c_info.get("min_group_count", req_min)),
                    "raw_value": round(float(c_info["raw_mean"]), 6) if pd.notna(c_info["raw_mean"]) else np.nan,
                    "released_value": (
                        round(float(c_info["exported_mean"]), 6)
                        if pd.notna(c_info["exported_mean"])
                        else np.nan
                    ),
                    "status": f_status,
                }
            )
            rows.append(
                {
                    "level": level_name,
                    "cluster_id": cid,
                    "feature": feat_name,
                    "metric_or_category": "std",
                    "raw_count": f_cnt,
                    "min_required": int(c_info.get("min_group_count", req_min)),
                    "raw_value": round(float(c_info["raw_std"]), 6) if pd.notna(c_info["raw_std"]) else np.nan,
                    "released_value": (
                        round(float(c_info["exported_std"]), 6)
                        if pd.notna(c_info["exported_std"])
                        else np.nan
                    ),
                    "status": f_status,
                }
            )

        # 3. Categorical features (one row per category)
        for col_label, cat_info in rec["categorical_audit"].items():
            rows.append(
                {
                    "level": level_name,
                    "cluster_id": cid,
                    "feature": cat_info["feature"],
                    "metric_or_category": cat_info.get("category", col_label),
                    "raw_count": int(cat_info["unrounded_contributing_count"]),
                    "min_required": int(cat_info["min_cell_count"]),
                    "raw_value": round(float(cat_info["raw_proportion"]), 6),
                    "released_value": (
                        round(float(cat_info["exported_proportion"]), 6)
                        if pd.notna(cat_info["exported_proportion"])
                        else np.nan
                    ),
                    "status": cat_info["status"],
                }
            )

    return pd.DataFrame(rows)


def build_rules_summary_df(agg_tables: dict, metadata: dict) -> pd.DataFrame:
    """
    Create a concise summary table with one row per hierarchy level.
    """
    rules = metadata.get("output_rules", {})
    levels_meta = metadata.get("levels", {})
    r_base = rules.get("rounding_base", None)
    rounding_label = int(r_base) if (r_base is not None and int(r_base) > 1) else "Off"
    min_cell = int(rules.get("min_cell_count", 6))
    sub_action = str(rules.get("subcell_action", "reassign_or_merge"))
    req_min = int(rules.get("min_group_count", 20))

    summary_rows = []
    for level_name, lvl_details in levels_meta.items():
        audit_recs = lvl_details.get("cluster_audit_records", [])
        released_recs = [r for r in audit_recs if r["status"] == "RELEASED"]
        suppressed_recs = [r for r in audit_recs if r["status"] != "RELEASED"]

        total_raw_records = len(lvl_details.get("raw_df", []))
        min_unrounded = min((r["unrounded_count"] for r in released_recs), default=0)
        min_exported = min((r["rounded_count"] for r in released_recs), default=0)

        min_active_subcell = None
        for r in released_recs:
            for c_audit in r["categorical_audit"].values():
                cnt = c_audit["unrounded_contributing_count"]
                exp_p = c_audit["exported_proportion"]
                if exp_p is not None and pd.notna(exp_p) and exp_p > 0:
                    if min_active_subcell is None or cnt < min_active_subcell:
                        min_active_subcell = cnt

        summary_rows.append(
            {
                "hierarchy_level": level_name,
                "raw_units": total_raw_records,
                "released_clusters": len(released_recs),
                "suppressed_clusters": len(suppressed_recs),
                "min_raw_cluster_count": min_unrounded,
                "min_group_count": req_min,
                "min_released_count": min_exported,
                "rounding_base": rounding_label,
                "min_nonzero_cell_count": min_active_subcell if min_active_subcell is not None else "N/A",
                "min_cell_count": min_cell,
                "subcell_action": sub_action,
                "status": "PASS" if (not released_recs or min_unrounded >= req_min) else "CHECK",
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
      2. `{output_dir}/{level}_raw_with_cluster.csv`: The raw unit records mapped to `cluster_id`.
      3. `{output_dir}/{level}_checker_audit_report.csv`: Vertically structured cluster/feature audit table.
      4. `{output_dir}/idi_rules_compliance_summary.csv`: Concise level-by-level compliance summary.
      5. `{output_dir}/idi_output_checker_report.xlsx`: Multi-sheet Excel workbook.
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
        "1_Summary": summary_df,
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
