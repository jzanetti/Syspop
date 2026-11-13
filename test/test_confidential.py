import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import numpy as np
import pandas as pd
from syspop.confidential import (
    load_config,
    resolve_data_structure,
    rr3_round,
    compute_pairwise_distances,
    aggregate_unit_records,
    aggregate_hierarchy,
    create_synthetic_dataset,
    run_datalab_aggregation,
    run_outside_synthesis,
    run_confidential_synthetic_pipeline,
)
from syspop.model.stochastic_impute import stochastic_impute


class TestConfidentialSyntheticAlgorithm(unittest.TestCase):
    def test_step1_docx_exact_distances_and_aggregation(self):
        """Verify Step 1.1 exact distances (0.04, 1.16, 1.04, and weighted 1.12) and grouping."""
        df = pd.DataFrame(
            {
                "person_id": [1, 2, 3],
                "age": [30, 40, 50],
                "sex": ["Male", "Male", "Female"],
            }
        )
        cfg = {
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
        dists = compute_pairwise_distances(df, cfg)
        self.assertAlmostEqual(dists[0, 1], 0.04, places=6)
        self.assertAlmostEqual(dists[0, 2], 1.16, places=6)
        self.assertAlmostEqual(dists[1, 2], 1.04, places=6)

        # Weighted distance with age weight = 3
        cfg_w = {
            "output_rules": cfg["output_rules"],
            "data_structure": {
                "id_columns": {"individual": "person_id"},
                "features": {
                    "age": {"type": "continuous", "weight": 3.0},
                    "sex": {"type": "categorical", "weight": 1.0},
                },
            },
        }
        dists_w = compute_pairwise_distances(df, cfg_w)
        self.assertAlmostEqual(dists_w[1, 2], 1.12, places=6)

        # Aggregation with leftover suppressed -> 1 group of 2 people, mean age = 35, Male = 1, Female = 0
        agg_df, details = aggregate_unit_records(df, cfg, return_details=True)
        self.assertEqual(len(agg_df), 1)
        self.assertAlmostEqual(agg_df.loc[0, "age"], 35.0, places=6)
        self.assertAlmostEqual(agg_df.loc[0, "Male"], 1.0, places=6)
        self.assertAlmostEqual(agg_df.loc[0, "Female"], 0.0, places=6)
        self.assertEqual(agg_df.loc[0, "count"], 2)

        # Normalized age for Person 1, 2, 3 should be 0.6, 0.8, 1.0
        norm_ages = details["normalized_df"]["age"].tolist()
        self.assertAlmostEqual(norm_ages[0], 0.6, places=6)
        self.assertAlmostEqual(norm_ages[1], 0.8, places=6)
        self.assertAlmostEqual(norm_ages[2], 1.0, places=6)

        # Aggregation with leftover assigned to nearest group -> 1 group of 3 people
        cfg_assign = dict(cfg)
        cfg_assign["output_rules"] = dict(cfg["output_rules"], leftover_action="assign_nearest")
        agg_assign = aggregate_unit_records(df, cfg_assign)
        self.assertEqual(len(agg_assign), 1)
        self.assertEqual(agg_assign.loc[0, "count"], 3)
        self.assertAlmostEqual(agg_assign.loc[0, "age"], 40.0, places=6)

    def test_step2_household_unit_merging(self):
        """Verify Step 2 household unit-record preparation matches docx example."""
        df_hh = pd.DataFrame(
            {
                "household_id": [1, 1, 2],
                "person_id": [1, 2, 1],
                "age": [30, 20, 50],
                "sex": ["Male", "Female", "Male"],
                "income": [100, 300, 200],
            }
        )
        cfg = {
            "output_rules": {
                "min_group_count": 1,
                "min_cell_count": 1,
                "rounding_base": None,
                "subcell_action": "none",
            },
            "data_structure": {
                "hierarchy": ["individual", "household"],
                "id_columns": {"individual": "person_id", "household": "household_id"},
            },
        }
        agg_tables, meta = aggregate_hierarchy(df_hh, cfg, return_metadata=True)
        hh_unit = meta["levels"]["household"]["unit_table"]
        self.assertEqual(len(hh_unit), 2)
        hh1 = hh_unit[hh_unit["household_id"] == 1].iloc[0]
        self.assertAlmostEqual(hh1["age"], 25.0)
        self.assertAlmostEqual(hh1["Male"], 0.5)
        self.assertAlmostEqual(hh1["Female"], 0.5)
        self.assertAlmostEqual(hh1["income"], 200.0)
        self.assertAlmostEqual(hh1["individual_num"], 2.0)

        hh2 = hh_unit[hh_unit["household_id"] == 2].iloc[0]
        self.assertAlmostEqual(hh2["age"], 50.0)
        self.assertAlmostEqual(hh2["Male"], 1.0)
        self.assertAlmostEqual(hh2["Female"], 0.0)
        self.assertAlmostEqual(hh2["income"], 200.0)
        self.assertAlmostEqual(hh2["individual_num"], 1.0)

    def test_step3_docx_tables_synthesis(self):
        """Verify Step 3 synthesis from the exact 3 tables in the docx."""
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
        syn_df, diag = create_synthetic_dataset(
            {"individual": ind_table, "family": fam_table, "household": hh_table},
            config_or_path={"synthesis": {"random_seed": 42}},
            return_diagnostics=True,
        )
        self.assertGreater(len(syn_df), 0)
        self.assertIn("household_id", syn_df.columns)
        self.assertIn("family_id", syn_df.columns)
        self.assertIn("person_id", syn_df.columns)
        self.assertEqual(syn_df["household_id"].nunique(), 15)

    def test_rounding_and_subcell_rules(self):
        """Verify random rounding (rounding_base=3 vs rounding_base=None) and sub-cell protection."""
        from syspop.confidential import apply_count_rounding

        rng = np.random.default_rng(7)
        for mult in (3, 6, 9, 12, 30):
            self.assertEqual(rr3_round(mult, base=3, rng=rng), mult)
        for val in (7, 8, 10, 11, 22):
            rounded = rr3_round(val, base=3, rng=rng)
            self.assertEqual(rounded % 3, 0)
            self.assertIn(rounded, (val - (val % 3), val - (val % 3) + 3))

        # Switching rounding off via rounding_base=None / 0 / False
        for off_val in (None, False, 0, 1, "none"):
            cfg_off = load_config({"output_rules": {"rounding_base": off_val}})
            self.assertIsNone(cfg_off["output_rules"]["rounding_base"])
            self.assertEqual(apply_count_rounding(22, cfg_off["output_rules"], rng=rng), 22)

    def test_auto_discovery_and_file_pipeline(self):
        """Verify automatic feature/hierarchy discovery and file export/import pipeline."""
        rng = np.random.default_rng(99)
        rows = []
        pid = 1
        for h_id in range(1, 26):
            for f_id in (h_id * 2 - 1, h_id * 2):
                for _ in range(3):
                    rows.append(
                        {
                            "household_id": h_id,
                            "family_id": f_id,
                            "person_id": pid,
                            "age": float(rng.integers(18, 75)),
                            "income": float(rng.normal(55000, 12000)),
                            "sex": rng.choice(["Male", "Female"]),
                        }
                    )
                    pid += 1
        df = pd.DataFrame(rows)

        cfg = {
            "output_rules": {
                "min_group_count": 20,
                "subcell_action": "rr3_and_suppress",
            },
            "synthesis": {"random_seed": 42},
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            agg_tables, meta = run_datalab_aggregation(df, cfg, output_dir=tmpdir)
            self.assertTrue(os.path.exists(os.path.join(tmpdir, "individual_aggregated.csv")))
            self.assertTrue(os.path.exists(os.path.join(tmpdir, "family_aggregated.csv")))
            self.assertTrue(os.path.exists(os.path.join(tmpdir, "household_aggregated.csv")))

            syn_df, diag = run_outside_synthesis(
                agg_tables_or_dir=tmpdir,
                config_or_path=cfg,
                output_dir=tmpdir,
                output_filename="syn.csv",
            )
            self.assertTrue(os.path.exists(os.path.join(tmpdir, "syn.csv")))
            self.assertGreater(len(syn_df), 0)

    def test_complex_multi_feature_pipeline(self):
        """Verify multi-feature (age, sex, income, saving, occupation, location) aggregation & synthesis."""
        from examples.Nearest_Neighbour_Synthesis.example2 import generate_complex_microdata

        raw_df = generate_complex_microdata(seed=101)
        with tempfile.TemporaryDirectory() as tmpdir:
            agg_tables, meta = run_datalab_aggregation(
                df=raw_df,
                config_or_path="etc/sample_data/complex_confidential_cfg.yml",
                output_dir=tmpdir,
            )
            # Verify IDI Output Checker reports and raw derivation files are generated
            self.assertTrue(os.path.exists(os.path.join(tmpdir, "idi_rules_compliance_summary.csv")))
            self.assertTrue(os.path.exists(os.path.join(tmpdir, "individual_raw_with_cluster.csv")))
            self.assertTrue(os.path.exists(os.path.join(tmpdir, "individual_checker_audit_report.csv")))
            self.assertTrue(os.path.exists(os.path.join(tmpdir, "family_raw_with_cluster.csv")))
            self.assertTrue(os.path.exists(os.path.join(tmpdir, "family_checker_audit_report.csv")))
            self.assertTrue(os.path.exists(os.path.join(tmpdir, "household_raw_with_cluster.csv")))
            self.assertTrue(os.path.exists(os.path.join(tmpdir, "household_checker_audit_report.csv")))

            # Verify raw derivation records for Cluster 1 match the exported mean age of Cluster 1
            raw_map = pd.read_csv(os.path.join(tmpdir, "individual_raw_with_cluster.csv"))
            c1_raw = raw_map[raw_map["cluster_id"].astype(str) == "1"]
            c1_agg_age = agg_tables["individual"].loc[
                agg_tables["individual"]["cluster_id"] == 1, "age"
            ].iloc[0]
            self.assertAlmostEqual(c1_raw["age"].mean(), c1_agg_age, places=5)

            syn_df, diag = run_outside_synthesis(
                agg_tables_or_dir=tmpdir,
                config_or_path="etc/sample_data/complex_confidential_cfg.yml",
            )
            for expected_col in (
                "household_id",
                "family_id",
                "person_id",
                "age",
                "sex",
                "income",
                "saving",
                "occupation",
                "location",
            ):
                self.assertIn(expected_col, syn_df.columns)
            # Check synthetic means are close to raw means
            self.assertAlmostEqual(raw_df["age"].mean(), syn_df["age"].mean(), delta=3.0)
            self.assertAlmostEqual(
                raw_df["income"].mean(), syn_df["income"].mean(), delta=5000.0
            )

    def test_existing_stochastic_impute_workflow(self):
        """Ensure existing stochastic_impute linkage workflow continues to work cleanly."""
        base_pop = pd.DataFrame({"gender": [1, 2, 1], "age": [25, 30, 40], "value": [10, 15, 20]})
        inc_df = pd.DataFrame(
            {
                "gender": [1, 1, 2, 2],
                "age": [25, 30, 25, np.nan],
                "work_status": [1, 2, 1, 2],
                "income": [50000, 60000, 55000, 45000],
                "value": [8, 2, 6, 4],
            }
        )
        tasks = {
            "income": {
                "targets": {"work_status": "category", "income": "numeric"},
                "features": ["age", "gender"],
            }
        }
        syn_pop = stochastic_impute(
            {"seed": base_pop, "income": inc_df},
            tasks,
            maintain_counts=["income"],
            output_dir=None,
        )
        self.assertEqual(len(syn_pop), 45)
        self.assertIn("work_status", syn_pop.columns)
        self.assertIn("income", syn_pop.columns)

    def test_survey_weights_parallel_df(self):
        """Verify optional parallel weight_df (household_weight, family_weight, person_weight) without extra cfg."""
        rng = np.random.default_rng(77)
        rows = []
        pid = 1
        for h_id in range(1, 61):
            for f_id in (h_id * 2 - 1, h_id * 2):
                for _ in range(3):
                    rows.append(
                        {
                            "household_id": h_id,
                            "family_id": f_id,
                            "person_id": pid,
                            "age": float(rng.integers(18, 75)),
                            "income": float(rng.normal(55000, 12000)),
                            "sex": rng.choice(["Male", "Female"]),
                        }
                    )
                    pid += 1
        df = pd.DataFrame(rows)

        cfg = {
            "output_rules": {
                "min_group_count": 20,
                "min_cell_count": 6,
                "rounding_base": None,
            },
            "synthesis": {"random_seed": 42},
        }

        # 1. Household-level weight_df: ["household_id", "household_weight"]
        hh_weight_df = pd.DataFrame(
            {
                "household_id": df["household_id"].unique(),
                "household_weight": rng.uniform(1.5, 4.5, size=df["household_id"].nunique()).round(2),
            }
        )
        syn_hh, agg_hh, _ = run_confidential_synthetic_pipeline(df, cfg, weight_df=hh_weight_df)
        self.assertIn("household_weight", agg_hh["individual"].columns)
        self.assertIn("household_weight_std", agg_hh["individual"].columns)
        self.assertIn("household_weight", syn_hh.columns)
        self.assertEqual(len(syn_hh), len(df))
        # Members of the same synthetic household have identical household_weight
        hh_w_nunique = syn_hh.groupby("household_id")["household_weight"].nunique()
        self.assertTrue((hh_w_nunique == 1).all())

        # 2. Individual-level weight_df: ["person_id", "person_weight"]
        ind_weight_df = pd.DataFrame(
            {
                "person_id": df["person_id"],
                "person_weight": rng.uniform(0.8, 3.2, size=len(df)).round(2),
            }
        )
        syn_ind, agg_ind, _ = run_confidential_synthetic_pipeline(df, cfg, weight_df=ind_weight_df)
        self.assertIn("person_weight", agg_ind["individual"].columns)
        self.assertIn("person_weight_std", agg_ind["individual"].columns)
        self.assertIn("person_weight", syn_ind.columns)
        self.assertEqual(len(syn_ind), len(df))

        # 3. Validation: providing multiple weight columns in weight_df raises ValueError
        bad_weight_df = pd.DataFrame(
            {
                "person_id": df["person_id"],
                "person_weight": 1.0,
                "household_weight": 2.0,
            }
        )
        with self.assertRaises(ValueError):
            run_confidential_synthetic_pipeline(df, cfg, weight_df=bad_weight_df)

    def test_hierarchy_counts_and_feature_agg(self):
        """Verify hierarchy_counts (e.g., age < 18) and per-feature agg ('sum', 'mean', 'max') across Step 2 & Step 3."""
        rng = np.random.default_rng(123)
        rows = []
        pid = 1
        for h_id in range(1, 51):
            for f_id in (1, 2):
                for m_idx in range(4):
                    age = float(rng.integers(5, 17)) if m_idx == 0 else float(rng.integers(25, 65))
                    rows.append(
                        {
                            "household_id": h_id,
                            "family_id": f_id,
                            "person_id": pid,
                            "age": age,
                            "income": float(rng.normal(50000, 8000)),
                            "sex": rng.choice(["Male", "Female"]),
                        }
                    )
                    pid += 1
        df = pd.DataFrame(rows)

        cfg = {
            "output_rules": {
                "min_group_count": 20,
                "min_cell_count": 6,
                "rounding_base": None,
            },
            "data_structure": {
                "hierarchy": ["individual", "family", "household"],
                "hierarchy_counts": {
                    "individual_under_18": {
                        "condition": "age < 18",
                        "weight": 3.0,
                    }
                },
                "features": {
                    "age": {"type": "continuous", "weight": 2.0, "agg": "mean"},
                    "income": {"type": "continuous", "weight": 2.0, "agg": "sum"},
                    "sex": {"type": "categorical", "weight": 1.0},
                },
            },
            "synthesis": {"random_seed": 42},
        }

        syn_df, agg_tables, diag = run_confidential_synthetic_pipeline(df, cfg)
        # Verify both mean and *_std are exported across all hierarchy levels (individual, family, household)
        for lvl in ("individual", "family", "household"):
            self.assertIn("age", agg_tables[lvl].columns)
            self.assertIn("age_std", agg_tables[lvl].columns)
            self.assertIn("income", agg_tables[lvl].columns)
            self.assertIn("income_std", agg_tables[lvl].columns)
        self.assertIn("individual_num", agg_tables["family"].columns)
        self.assertIn("individual_num_std", agg_tables["family"].columns)
        self.assertIn("family_num", agg_tables["household"].columns)
        self.assertIn("family_num_std", agg_tables["household"].columns)
        self.assertIn("individual_under_18", agg_tables["family"].columns)
        self.assertIn("individual_under_18_std", agg_tables["family"].columns)
        self.assertIn("individual_under_18", agg_tables["household"].columns)
        self.assertIn("individual_under_18_std", agg_tables["household"].columns)
        # Since every family of 4 has 1 child under 18 and 4 members of ~$50k income,
        # family aggregated income should be ~4 * $50k = ~$200k (sum, not mean!)
        self.assertAlmostEqual(agg_tables["family"]["individual_under_18"].mean(), 1.0, places=2)
        self.assertGreater(agg_tables["family"]["income"].mean(), 150000.0)
        # Verify Step 3 residuals include `individual_under_18_mae`
        self.assertIn("individual_under_18_mae", diag["residuals"]["family"])
        self.assertIn("individual_under_18_mae", diag["residuals"]["household"])


if __name__ == "__main__":
    unittest.main()
