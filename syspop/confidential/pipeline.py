from os import listdir, makedirs
from os.path import exists, join
import pandas as pd
from syspop.confidential.aggregate import aggregate_hierarchy
from syspop.confidential.config import load_config, resolve_data_structure, resolve_verification_config
from syspop.confidential.report import export_datalab_checker_reports
from syspop.confidential.synthesize import create_synthetic_dataset
from syspop.confidential.verify import (
    export_synthetic_verification_report,
    export_verification_figures_counts,
)
from syspop.postp.vis import plot_hierarchical_verification


def run_aggregation(
    df: pd.DataFrame,
    config_or_path=None,
    output_dir: str or None = None,
    weight_df: pd.DataFrame or None = None,
) -> tuple:
    """
    Execute Step 1 & Step 2 (Inside Data Lab):
    Aggregate unit-record microdata across configured hierarchy levels (individual,
    family, household) following confidentiality rules, with optional parallel `weight_df`.

    If `output_dir` is provided, automatically exports:
      - `{level}_aggregated.csv` (confidentialised table for release)
      - `{level}_raw_with_cluster.csv` (raw unit records mapped to `cluster_id`)
      - `{level}_checker_audit_report.csv` (cluster-by-cluster rule compliance audit)
      - `idi_rules_compliance_summary.csv` (executive summary of rules)
      - `idi_output_checker_report.xlsx` (consolidated multi-sheet Excel report for checkers)
    """
    agg_tables, metadata = aggregate_hierarchy(
        df, config_or_path=config_or_path, return_metadata=True, weight_df=weight_df
    )

    if output_dir is not None:
        saved_files = export_datalab_checker_reports(
            agg_tables=agg_tables,
            metadata=metadata,
            output_dir=output_dir,
        )
        metadata["exported_files"] = saved_files

    return agg_tables, metadata


def run_synthesis(
    agg_tables_or_dir: dict or pd.DataFrame or str,
    config_or_path=None,
    metadata: dict or None = None,
    output_dir: str or None = None,
    output_filename: str = "synthetic_unit_records.parquet",
) -> tuple:
    """
    Execute Step 3 (Outside Data Lab):
    Recreate the synthetic unit-record dataset from confidentialised aggregated
    tables (or a directory containing `{level}_aggregated.csv` files).
    """
    if isinstance(agg_tables_or_dir, str):
        agg_tables = {}
        excel_path = join(agg_tables_or_dir, "idi_output_checker_report.xlsx")
        cfg_hier = (
            load_config(config_or_path).get("data_structure", {}).get("hierarchy")
            if config_or_path is not None
            else None
        )
        candidate_levels = (
            [str(h).lower() for h in cfg_hier]
            if cfg_hier
            else [
                f[: -len("_aggregated.csv")]
                for f in sorted(listdir(agg_tables_or_dir))
                if f.endswith("_aggregated.csv")
            ]
        )
        for level_name in candidate_levels:
            csv_path = join(agg_tables_or_dir, f"{level_name}_aggregated.csv")
            if exists(csv_path):
                try:
                    df_lvl = pd.read_csv(csv_path)
                    if len(df_lvl) > 0:
                        agg_tables[level_name] = df_lvl
                except pd.errors.EmptyDataError:
                    pass
        if not agg_tables and exists(excel_path):
            xl = pd.ExcelFile(excel_path)
            if not candidate_levels:
                if "1_Summary" in xl.sheet_names:
                    sum_df = pd.read_excel(xl, sheet_name="1_Summary")
                    if "hierarchy_level" in sum_df.columns:
                        candidate_levels = [str(h).lower() for h in sum_df["hierarchy_level"].tolist()]
                if not candidate_levels:
                    candidate_levels = [
                        s[: -len("_Released_Output")].lower()
                        for s in xl.sheet_names
                        if s.endswith("_Released_Output")
                    ]
            for level_name in candidate_levels:
                sheet_name = f"{level_name}_Released_Output"[:31]
                if sheet_name not in xl.sheet_names:
                    sheet_name = f"{level_name[:3].capitalize()}_Released_Output"
                if sheet_name in xl.sheet_names:
                    df_lvl = pd.read_excel(xl, sheet_name=sheet_name)
                    if len(df_lvl) > 0:
                        if (
                            "rounded_total_count" in df_lvl.columns
                            and "count" not in df_lvl.columns
                        ):
                            df_lvl = df_lvl.rename(
                                columns={"rounded_total_count": "count"}
                            )
                        elif (
                            "rounded_count" in df_lvl.columns
                            and "count" not in df_lvl.columns
                        ):
                            df_lvl = df_lvl.rename(columns={"rounded_count": "count"})
                        elif (
                            "unrounded_total_count" in df_lvl.columns
                            and "count" not in df_lvl.columns
                        ):
                            df_lvl = df_lvl.rename(
                                columns={"unrounded_total_count": "count"}
                            )
                        elif (
                            "unrounded_count" in df_lvl.columns
                            and "count" not in df_lvl.columns
                        ):
                            df_lvl = df_lvl.rename(columns={"unrounded_count": "count"})
                        drop_cols = [
                            c
                            for c in df_lvl.columns
                            if c.startswith("unrounded_") or c.startswith("rounded_")
                        ]
                        if drop_cols:
                            df_lvl = df_lvl.drop(columns=drop_cols)
                        agg_tables[level_name] = df_lvl
        if not agg_tables:
            raise FileNotFoundError(
                f"No non-empty `idi_output_checker_report.xlsx` or `*_aggregated.csv` files found in directory '{agg_tables_or_dir}'."
            )
    else:
        agg_tables = agg_tables_or_dir

    syn_df, diagnostics = create_synthetic_dataset(
        agg_tables=agg_tables,
        config_or_path=config_or_path,
        metadata=metadata,
        return_diagnostics=True,
    )

    if output_dir is not None and len(syn_df) > 0:
        if not exists(output_dir):
            makedirs(output_dir)
        out_path = join(output_dir, output_filename)
        if out_path.endswith(".csv"):
            syn_df.to_csv(out_path, index=False)
        else:
            syn_df.to_parquet(out_path, index=False)

    return syn_df, diagnostics


def run_confidential_synthetic_pipeline(
    df: pd.DataFrame,
    config_or_path=None,
    output_dir: str or None = None,
    output_filename: str = "synthetic_unit_records.parquet",
    weight_df: pd.DataFrame or None = None,
) -> tuple:
    """
    End-to-end execution of Step 1 (Unit-Record Nearest-Neighbour Aggregation),
    Step 2 (Hierarchical Aggregation), and Step 3 (Synthetic Unit-Record Recreation).

    Returns:
        tuple: `(synthetic_df, agg_tables, diagnostics)`
    """
    agg_tables, metadata = run_aggregation(
        df=df, config_or_path=config_or_path, output_dir=output_dir, weight_df=weight_df
    )
    syn_df, diagnostics = run_synthesis(
        agg_tables_or_dir=agg_tables,
        config_or_path=config_or_path,
        metadata=metadata,
        output_dir=output_dir,
        output_filename=output_filename,
    )
    return syn_df, agg_tables, diagnostics


def run_verif(
    raw_df: pd.DataFrame,
    syn_df: pd.DataFrame,
    output_dir: str,
    config_or_path=None,
    diagnostics: dict or None = None,
    columns: list or None = None,
) -> dict:
    """Run hierarchical verification plots, accuracy report, and figure counts export."""
    if columns is not None:
        feature_cols = list(columns)
    else:
        verif_cfg = resolve_verification_config(config_or_path) if config_or_path is not None else {}
        cfg_cols = verif_cfg.get("columns")
        if cfg_cols:
            feature_cols = [c for c in cfg_cols if c in raw_df.columns and c in syn_df.columns]
        else:
            cfg = load_config(config_or_path)
            id_set = set(resolve_data_structure(raw_df, cfg)["id_columns"].values())
            feature_cols = [
                c for c in raw_df.columns if c in syn_df.columns and c not in id_set
            ]
    fig_bundle = plot_hierarchical_verification(
        raw_df=raw_df,
        syn_df=syn_df,
        columns=feature_cols,
        output_dir=output_dir,
        config_or_path=config_or_path,
    )
    verif_excel = export_synthetic_verification_report(
        raw_df=raw_df,
        syn_df=syn_df,
        output_dir=output_dir,
        columns=feature_cols,
        diagnostics=diagnostics,
        config_or_path=config_or_path,
    )
    figs_counts_excel = export_verification_figures_counts(
        raw_df=raw_df,
        syn_df=syn_df,
        columns=feature_cols,
        output_dir=output_dir,
        config_or_path=config_or_path,
    )
    return {
        "figures": fig_bundle,
        "verification_report": verif_excel,
        "figures_counts": figs_counts_excel,
    }
