import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

import numpy as np
import pandas as pd
from syspop.confidential import (
    run_datalab_aggregation,
    run_outside_synthesis,
)
from syspop.postp.vis import plot_distribution, plot_comparison, plot_cross_comparison


def generate_complex_microdata(seed: int = 2026) -> pd.DataFrame:
    """
    Generate a realistic confidential unit-record microdata table with:
      - Hierarchy IDs: household_id, family_id, person_id
      - Continuous features: age, income, saving
      - Categorical features: sex, occupation, location
    """
    rng = np.random.default_rng(seed)

    locations = ["Auckland", "Wellington", "Christchurch", "Hamilton"]
    loc_probs = [0.40, 0.25, 0.20, 0.15]
    loc_income_mult = {
        "Auckland": 1.15,
        "Wellington": 1.18,
        "Christchurch": 1.00,
        "Hamilton": 0.95,
    }

    occupations = [
        "Manager",
        "Professional",
        "Technician",
        "Trade",
        "Clerical",
        "Sales",
    ]
    occ_base_income = {
        "Manager": 95000.0,
        "Professional": 85000.0,
        "Technician": 68000.0,
        "Trade": 64000.0,
        "Clerical": 54000.0,
        "Sales": 48000.0,
    }

    records = []

    # Generate 120 households (~260 families, ~750 individuals)
    for hh_id in range(1, 121):
        hh_location = rng.choice(locations, p=loc_probs)
        n_families = int(rng.choice([1, 2, 3], p=[0.25, 0.55, 0.20]))

        family_id = 1
        for _ in range(n_families):
            n_members = int(rng.choice([2, 3, 4, 5], p=[0.35, 0.35, 0.20, 0.10]))
            fam_age_center = float(rng.uniform(20.0, 58.0))

            person_id = 1
            for m_idx in range(n_members):
                if m_idx >= 2 and rng.random() < 0.55:
                    # Child / dependent member under 18
                    age = float(np.clip(np.round(rng.uniform(4.0, 17.5), 1), 1.0, 17.9))
                else:
                    age = float(
                        np.clip(
                            np.round(rng.normal(fam_age_center, 11.0), 1), 8.0, 80.0
                        )
                    )
                sex = str(rng.choice(["Male", "Female"], p=[0.49, 0.51]))

                # Occupation probability varies slightly with age
                if age >= 35:
                    occ_p = [0.22, 0.28, 0.16, 0.14, 0.12, 0.08]
                else:
                    occ_p = [0.08, 0.24, 0.20, 0.18, 0.15, 0.15]
                occupation = str(rng.choice(occupations, p=occ_p))

                # Income depends on occupation, location, and age experience
                age_factor = 1.0 + 0.008 * max(0.0, min(age - 22.0, 30.0))
                mean_inc = (
                    occ_base_income[occupation]
                    * loc_income_mult[hh_location]
                    * age_factor
                )
                income = float(max(15000.0, np.round(rng.normal(mean_inc, 9500.0), 2)))

                # Saving correlates with income and age
                saving_rate = float(rng.uniform(0.08, 0.35))
                saving = float(
                    max(
                        0.0,
                        np.round(
                            income * saving_rate * (age / 35.0)
                            + rng.normal(0.0, 3000.0),
                            2,
                        ),
                    )
                )

                records.append(
                    {
                        "household_id": hh_id,
                        "family_id": family_id,
                        "person_id": person_id,
                        "age": age,
                        "sex": sex,
                        "income": income,
                        "saving": saving,
                        "occupation": occupation,
                        "location": hh_location,
                    }
                )
                person_id += 1
            family_id += 1

    return pd.DataFrame(records)


def run_complex_example():
    print("=" * 80)
    print("Complex Multi-Feature Confidential Aggregation & Hierarchical Synthesis")
    print("Features: age, sex, income, saving, occupation, location")
    print("=" * 80)

    raw_df = generate_complex_microdata(seed=2026)
    n_distinct_families = len(raw_df[["household_id", "family_id"]].drop_duplicates())
    print(
        f"\n[1] Confidential Unit-Record Microdata Summary:\n"
        f"    Individuals: {len(raw_df)} | Families: {n_distinct_families} | "
        f"Households: {raw_df['household_id'].nunique()}"
    )
    print(raw_df.head(8))

    cfg_path = "etc/examples/Nearest_Neighbour_Synthesis/example2.yml"
    export_dir = "./output/complex_exported_tables"
    syn_output_dir = "./output/complex_synthetic"

    # Optional survey weights DataFrame (in parallel to raw_df):
    # Can contain a single hierarchy-matched weight column such as "household_weight",
    # "family_weight", or "person_weight" (or set weight_df = None for unweighted microdata).
    rng_w = np.random.default_rng(2026)
    unique_hh = raw_df[["household_id"]].drop_duplicates().reset_index(drop=True)
    unique_hh["household_weight"] = np.round(
        rng_w.uniform(0.8, 3.5, size=len(unique_hh)), 2
    )
    weight_df = unique_hh  # Set to None to run without survey weights

    # Step 1 & Step 2: Run inside the Data Lab and export confidentialised CSV tables
    # along with IDI Output Checker audit reports and raw-to-cluster derivation tables
    agg_tables, metadata = run_datalab_aggregation(
        df=raw_df,
        config_or_path=cfg_path,
        output_dir=export_dir,
        weight_df=weight_df,
    )

    exported_files = metadata.get("exported_files", {})
    print(
        f"\n[2] Exported Data Lab Files & IDI Output Checker Reports (in '{export_dir}'):"
    )
    print(f"    - Rules Compliance Summary CSV : {exported_files.get('summary_csv')}")
    print(f"    - Consolidated Excel Workbook  : {exported_files.get('excel_report')}")
    for lvl in ("individual", "family", "household"):
        print(
            f"    - {lvl.capitalize():10s} -> Released: {exported_files['released_csvs'].get(lvl)} | "
            f"Raw Derivation: {exported_files['raw_derivation_csvs'].get(lvl)} | "
            f"Audit: {exported_files['checker_audit_csvs'].get(lvl)}"
        )

    summary_df = pd.read_csv(exported_files["summary_csv"])
    print("\n  [2a] Compliance Summary across Hierarchy Levels:")
    print(summary_df)

    ind_audit_df = pd.read_csv(exported_files["checker_audit_csvs"]["individual"])
    print("\n  [2b] Vertically Structured Audit Report for Cluster 1 (first 15 rows):")
    print(ind_audit_df[ind_audit_df["cluster_id"].astype(str) == "1"].head(15))

    ind_raw_map_df = pd.read_csv(exported_files["raw_derivation_csvs"]["individual"])
    print(
        "\n  [2c] Sample of Raw Individual Records Mapped to Cluster 1 (showing derivation of Row 1):"
    )
    print(
        ind_raw_map_df[ind_raw_map_df["cluster_id"].astype(str) == "1"][
            [
                "cluster_id",
                "cluster_raw_count",
                "cluster_released_count",
                "household_id",
                "family_id",
                "person_id",
                "age",
                "sex",
                "income",
                "saving",
                "occupation",
                "location",
            ]
        ].head(6)
    )

    # Step 3: Recreate Synthetic Unit-Record Dataset OUTSIDE the Data Lab
    # Note: We load strictly from the exported CSV directory + YAML config file (no internal metadata passed)
    syn_df, diagnostics = run_outside_synthesis(
        agg_tables_or_dir=export_dir,
        config_or_path=cfg_path,
        output_dir=syn_output_dir,
        output_filename="complex_synthetic_population.parquet",
    )

    print(
        f"\n[3] Recreated Synthetic Unit-Record Population (saved to {syn_output_dir}):\n"
        f"    Synthetic Individuals: {len(syn_df)} | "
        f"Synthetic Families: {syn_df['family_id'].nunique()} | "
        f"Synthetic Households: {syn_df['household_id'].nunique()}"
    )
    print(syn_df.head(10))

    print("\n[4] Comparison: Confidential Microdata vs. Synthetic Population")
    comp_cont = pd.DataFrame(
        {
            "Raw Mean": raw_df[["age", "income", "saving"]].mean(),
            "Synthetic Mean": syn_df[["age", "income", "saving"]].mean(),
            "Raw Std": raw_df[["age", "income", "saving"]].std(),
            "Synthetic Std": syn_df[["age", "income", "saving"]].std(),
        }
    )
    print("\n  Continuous Features Summary:")
    print(comp_cont.round(2))

    for cat_col in ["sex", "occupation", "location"]:
        raw_p = raw_df[cat_col].value_counts(normalize=True).rename("Raw Proportion")
        syn_p = (
            syn_df[cat_col].value_counts(normalize=True).rename("Synthetic Proportion")
        )
        cat_comp = pd.concat([raw_p, syn_p], axis=1).fillna(0.0)
        print(f"\n  Categorical Feature '{cat_col}' Proportions:")
        print(cat_comp.round(3))

    print(
        "\n  Cross-Occupation Comparison (Continuous Means by Occupation: Raw vs. Synthetic):"
    )
    raw_occ_means = (
        raw_df.groupby("occupation")[["age", "income", "saving"]]
        .mean()
        .add_prefix("Raw_")
    )
    syn_occ_means = (
        syn_df.groupby("occupation")[["age", "income", "saving"]]
        .mean()
        .add_prefix("Syn_")
    )
    occ_cross_df = pd.concat([raw_occ_means, syn_occ_means], axis=1)[
        ["Raw_age", "Syn_age", "Raw_income", "Syn_income", "Raw_saving", "Syn_saving"]
    ]
    print(occ_cross_df.round(2))

    print("\n[5] Hierarchy Matching Residual Errors (MAE):")
    for lvl, res in diagnostics["residuals"].items():
        print(f"  {lvl.capitalize()} level:", {k: round(v, 3) for k, v in res.items()})

    # Plot 1D and all pairwise 2D cross-feature comparisons (Raw vs. Synthetic)
    feature_cols = ["age", "sex", "income", "saving", "occupation", "location"]
    comp_plot = plot_comparison(
        raw_df,
        syn_df,
        columns=feature_cols,
        output_dir=syn_output_dir,
    )
    cross_plots = plot_cross_comparison(
        raw_df,
        syn_df,
        columns=feature_cols,
        output_dir=syn_output_dir,
    )
    print(
        f"\n[6] Saved 1D comparison ({comp_plot}) and {len(cross_plots)} pairwise "
        f"Raw vs. Synthetic cross-feature comparison plots to '{syn_output_dir}/':"
    )
    for cp in cross_plots:
        print(f"    - {cp}")


if __name__ == "__main__":
    run_complex_example()
