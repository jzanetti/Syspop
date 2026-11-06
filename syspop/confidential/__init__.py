from syspop.confidential.config import load_config, resolve_data_structure
from syspop.confidential.rules import (
    rr3_round,
    apply_count_rounding,
    get_percentile_min_count,
    confidentialise_category_proportions,
)
from syspop.confidential.aggregate import (
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
    run_datalab_aggregation,
    run_outside_synthesis,
    run_confidential_synthetic_pipeline,
)

__all__ = [
    "load_config",
    "resolve_data_structure",
    "rr3_round",
    "apply_count_rounding",
    "get_percentile_min_count",
    "confidentialise_category_proportions",
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
    "run_datalab_aggregation",
    "run_outside_synthesis",
    "run_confidential_synthetic_pipeline",
]
