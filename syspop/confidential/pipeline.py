from os import makedirs
from os.path import exists, join
import pandas as pd
from syspop.confidential.aggregate import aggregate_hierarchy
from syspop.confidential.report import export_datalab_checker_reports
from syspop.confidential.synthesize import create_synthetic_dataset


def run_datalab_aggregation(
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


def run_outside_synthesis(
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
        for level_name in ("individual", "family", "household"):
            csv_path = join(agg_tables_or_dir, f"{level_name}_aggregated.csv")
            if exists(csv_path):
                try:
                    df_lvl = pd.read_csv(csv_path)
                    if len(df_lvl) > 0:
                        agg_tables[level_name] = df_lvl
                except pd.errors.EmptyDataError:
                    pass
        if not agg_tables:
            raise FileNotFoundError(
                f"No non-empty `*_aggregated.csv` files found in directory '{agg_tables_or_dir}'."
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
    agg_tables, metadata = run_datalab_aggregation(
        df=df, config_or_path=config_or_path, output_dir=output_dir, weight_df=weight_df
    )
    syn_df, diagnostics = run_outside_synthesis(
        agg_tables_or_dir=agg_tables,
        config_or_path=config_or_path,
        metadata=metadata,
        output_dir=output_dir,
        output_filename=output_filename,
    )
    return syn_df, agg_tables, diagnostics
