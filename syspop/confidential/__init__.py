from syspop.confidential.config import load_config, resolve_data_structure
from syspop.confidential.rules import (
    rr3_round,
    apply_count_rounding,
    confidentialise_category_proportions,
)
from syspop.confidential.aggregate import (
    add_correlation_features,
    prepare_encoded_features,
    compute_pairwise_distances,
    nearest_neighbour_grouping,
    aggregate_unit_records,
    aggregate_hierarchy,
)
from syspop.confidential.report import (
    build_raw_cluster_mapping_df,
    build_checker_audit_df,
    build_rules_summary_df,
    export_datalab_checker_reports,
)
from syspop.confidential.synthesize import (
    recreate_individuals_from_agg,
    create_synthetic_dataset,
)
from syspop.confidential.pipeline import (
    run_aggregation,
    run_synthesis,
    run_confidential_synthetic_pipeline,
    run_verif,
)
from syspop.confidential.verify import (
    evaluate_synthetic_data,
    export_synthetic_verification_report,
    export_verification_figures_counts,
)

__all__ = [
    "load_config",
    "resolve_data_structure",
    "rr3_round",
    "apply_count_rounding",
    "confidentialise_category_proportions",
    "add_correlation_features",
    "prepare_encoded_features",
    "compute_pairwise_distances",
    "nearest_neighbour_grouping",
    "aggregate_unit_records",
    "aggregate_hierarchy",
    "build_raw_cluster_mapping_df",
    "build_checker_audit_df",
    "build_rules_summary_df",
    "export_datalab_checker_reports",
    "recreate_individuals_from_agg",
    "create_synthetic_dataset",
    "run_aggregation",
    "run_synthesis",
    "run_verif",
    "run_confidential_synthetic_pipeline",
    "evaluate_synthetic_data",
    "export_synthetic_verification_report",
    "export_verification_figures_counts",
]
