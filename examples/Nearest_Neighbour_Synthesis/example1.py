import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

import numpy as np
import pandas as pd
from syspop.confidential import (
    compute_pairwise_distances,
    aggregate_unit_records,
    create_synthetic_dataset,
    run_confidential_synthetic_pipeline,
)


def run_docx_walkthrough():
    print("=" * 70)
    print("PART 1: Walkthrough of Examples in Synthetic_Data_Production_Algorithm.docx")
    print("=" * 70)

    # ------------------------------------------------------------------
    # Step 1.1: 3-Person Distance & Aggregation Example from the Document
    # ------------------------------------------------------------------
    df_step1 = pd.DataFrame(
        {
            "person_id": [1, 2, 3],
            "age": [30, 40, 50],
            "sex": ["Male", "Male", "Female"],
        }
    )

    cfg_unweighted = {
        "output_rules": {
            "min_group_count": 2,
            "min_cell_count": 2,
            "rounding_base": None,
            "subcell_action": "none",
            "leftover_action": "suppress",
        },
        "data_structure": {
            "id_columns": {"individual": "person_id"},
            "features": {
                "age": {"type": "continuous", "weight": 1.0},
                "sex": {"type": "categorical", "weight": 1.0},
            },
        },
    }

    dist_matrix = compute_pairwise_distances(df_step1, cfg_unweighted)
    print("\n1.1 Pairwise distances (unweighted):")
    print(f"  d(Person 1, Person 2) = {dist_matrix[0, 1]:.2f} (expected 0.04)")
    print(f"  d(Person 1, Person 3) = {dist_matrix[0, 2]:.2f} (expected 1.16)")
    print(f"  d(Person 2, Person 3) = {dist_matrix[1, 2]:.2f} (expected 1.04)")

    # Weighted example: weight of age = 3
    cfg_weighted = {
        "output_rules": cfg_unweighted["output_rules"],
        "data_structure": {
            "id_columns": {"individual": "person_id"},
            "features": {
                "age": {"type": "continuous", "weight": 3.0},
                "sex": {"type": "categorical", "weight": 1.0},
            },
        },
    }
    dist_matrix_w = compute_pairwise_distances(df_step1, cfg_weighted)
    print(f"  d(Person 2, Person 3) with age weight=3: {dist_matrix_w[1, 2]:.2f} (expected 1.12)")

    agg_suppressed = aggregate_unit_records(df_step1, cfg_unweighted)
    print("\n1.2 Aggregated table (min_count=2, leftover suppressed):")
    print(agg_suppressed)

    # Assign leftover Person 3 to nearest group
    cfg_assign = dict(cfg_unweighted)
    cfg_assign["output_rules"] = dict(cfg_unweighted["output_rules"], leftover_action="assign_nearest")
    agg_assigned = aggregate_unit_records(df_step1, cfg_assign)
    print("\n1.3 Aggregated table (min_count=2, leftover assigned to nearest group):")
    print(agg_assigned)

    # ------------------------------------------------------------------
    # Step 3: Recreate Synthetic Dataset from the 3 Tables in the Document
    # ------------------------------------------------------------------
    ind_table = pd.DataFrame(
        {
            "age": [35, 40],
            "Age std": [5, 6],
            "Ave income": [200, 170],
            "Income std": [0.3, 0.4],
            "Male": [0.7, 0.4],
            "Female": [0.3, 0.6],
            "count": [30, 25],
        }
    )
    fam_table = pd.DataFrame(
        {
            "age": [36, 41],
            "Age std": [4, 5],
            "Ave income": [230, 250],
            "Income std": [0.1, 0.15],
            "Male": [0.4, 0.7],
            "Female": [0.6, 0.3],
            "Individual number": [4, 2],
            "count": [10, 12],
        }
    )
    hh_table = pd.DataFrame(
        {
            "age": [47, 43],
            "Age std": [4, 5],
            "Ave income": [310, 280],
            "Income std": [0.3, 0.2],
            "Male": [0.6, 0.5],
            "Female": [0.4, 0.5],
            "Family number": [3, 2],
            "count": [6, 9],
        }
    )

    syn_from_docx, diag = create_synthetic_dataset(
        {"individual": ind_table, "family": fam_table, "household": hh_table},
        config_or_path={"synthesis": {"random_seed": 42}},
        return_diagnostics=True,
    )
    print("\n1.4 Recreated hierarchical synthetic dataset from Step 3 tables (first 10 rows):")
    print(syn_from_docx.head(10))
    print(f"Total synthetic individuals: {len(syn_from_docx)}, "
          f"Families: {syn_from_docx['family_id'].nunique()}, "
          f"Households: {syn_from_docx['household_id'].nunique()}")
    print("Residual diagnostics (MAE):", diag["residuals"])


def run_automated_pipeline_example():
    print("\n" + "=" * 70)
    print("PART 2: End-to-End Automated Pipeline with Configuration File")
    print("=" * 70)

    rng = np.random.default_rng(123)
    # Generate 240 unit-record individuals across 80 families and 40 households
    records = []
    person_id = 1
    family_id = 1
    for hh_id in range(1, 41):
        n_fams_in_hh = rng.choice([1, 2, 3], p=[0.3, 0.5, 0.2])
        base_hh_income = rng.normal(65000, 15000)
        for _ in range(n_fams_in_hh):
            n_inds_in_fam = rng.choice([2, 3, 4], p=[0.4, 0.4, 0.2])
            base_fam_age = rng.normal(38, 10)
            for _ in range(n_inds_in_fam):
                records.append(
                    {
                        "household_id": hh_id,
                        "family_id": family_id,
                        "person_id": person_id,
                        "age": max(1.0, round(rng.normal(base_fam_age, 8), 1)),
                        "income": max(0.0, round(rng.normal(base_hh_income, 8000), 2)),
                        "sex": rng.choice(["Male", "Female"], p=[0.48, 0.52]),
                    }
                )
                person_id += 1
            family_id += 1

    raw_microdata = pd.DataFrame(records)
    print(f"\nRaw unit-record microdata: {len(raw_microdata)} individuals, "
          f"{raw_microdata['family_id'].nunique()} families, "
          f"{raw_microdata['household_id'].nunique()} households.")

    syn_df, agg_tables, diagnostics = run_confidential_synthetic_pipeline(
        df=raw_microdata,
        config_or_path="etc/sample_data/sample_confidential_cfg.yml",
        output_dir="./output",
        output_filename="confidential_synthetic_population.parquet",
    )

    print("\nConfidentialised Individual Aggregated Table (IDI rules, min_count=20, RR3):")
    print(agg_tables["individual"])
    print("\nConfidentialised Family Aggregated Table:")
    print(agg_tables["family"])
    print("\nConfidentialised Household Aggregated Table:")
    print(agg_tables["household"])
    print("\nRecreated Synthetic Unit-Record Dataset (first 10 rows):")
    print(syn_df.head(10))
    print("\nHierarchy Residual Errors (MAE):", diagnostics["residuals"])


def run_complex_multi_feature_example():
    """
    PART 3: Complex multi-feature example with continuous features (age, income, saving)
    and categorical features (sex, occupation, location) across Individual -> Family -> Household.
    See also `examples/Nearest_Neighbour_Synthesis/example2.py`.
    """
    from examples.Nearest_Neighbour_Synthesis.example2 import run_complex_example

    print("\n")
    run_complex_example()


if __name__ == "__main__":
    run_docx_walkthrough()
    run_automated_pipeline_example()
    run_complex_multi_feature_example()
