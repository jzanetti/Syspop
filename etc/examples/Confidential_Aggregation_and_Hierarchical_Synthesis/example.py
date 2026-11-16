import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))


from syspop.confidential import run_aggregation, run_synthesis, run_verif
from syspop.utils.create_sample_microdata import generate_hhld_complex_microdata

cfg_path = "etc/examples/Confidential_Aggregation_and_Hierarchical_Synthesis/example.yml"
export_dir = "./output/complex_exported_tables"
syn_output_dir = "./output/complex_synthetic"

# Step 1: Creating a pseudo confidential microdata table with
#         hierarchical structure and continuous/categorical features
raw_df, weight_df = generate_hhld_complex_microdata(
    seed=2026, n_hhld=500, create_hhld_weight=True
)

# Step 2: Creating confidentialised aggregated tables and exporting them to CSV/Excel files
agg_tables, metadata = run_aggregation(
    df=raw_df, weight_df=weight_df, config_or_path=cfg_path, output_dir=export_dir
)

# Step 3: Creating synthetic unit-record dataset using the exported tables and synthesis config
syn_df, diagnostics = run_synthesis(
    agg_tables_or_dir=export_dir,
    config_or_path=cfg_path,
    output_dir=syn_output_dir,
    output_filename="complex_synthetic_population.parquet",
)

# Step 4: Running hierarchical verification plots, accuracy report, and figure counts export
run_verif(
    raw_df=raw_df,
    syn_df=syn_df,
    output_dir=syn_output_dir,
    config_or_path=cfg_path,
    diagnostics=diagnostics,
)
