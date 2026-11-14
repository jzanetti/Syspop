import os
import tempfile
import unittest
import numpy as np
import pandas as pd

from syspop.confidential.aggregate import (
    add_correlation_features,
    prepare_encoded_features,
    compute_pairwise_distances,
    _compute_weighted_mean_std,
    _count_record_subcell_violations,
    _record_fits_cluster_categories,
    _standard_nn_clusters,
    nearest_neighbour_grouping,
    aggregate_unit_records,
    build_unit_hierarchy_table,
    aggregate_hierarchy,
)
from syspop.confidential.report import (
    build_checker_guide_df,
    build_raw_cluster_mapping_df,
    build_checker_audit_df,
    build_released_with_counts_df,
    build_rules_summary_df,
    export_datalab_checker_reports,
)
from syspop.confidential.verify import (
    evaluate_synthetic_data,
    export_synthetic_verification_report,
)


class TestAggregate(unittest.TestCase):
    def setUp(self):
        np.random.seed(42)
        self.sample_df = pd.DataFrame(
            {
                "household_id": [1, 1, 2, 2, 3, 3, 4, 4],
                "family_id": [10, 10, 20, 20, 30, 30, 40, 40],
                "person_id": [101, 102, 201, 202, 301, 302, 401, 402],
                "age": [25.0, 30.0, 45.0, 50.0, 15.0, 60.0, 35.0, 40.0],
                "income": [50000.0, 60000.0, 80000.0, 90000.0, 0.0, 70000.0, 55000.0, 65000.0],
                "sex": ["male", "female", "male", "female", "female", "male", "female", "male"],
            }
        )
        self.features_meta = {
            "age": {"type": "continuous", "weight": 1.0, "agg": "mean"},
            "income": {"type": "continuous", "weight": 2.0, "agg": "sum"},
            "sex": {"type": "categorical", "weight": 1.5},
        }

    def test_prepare_encoded_features(self):
        norm_df, X_weighted, enc_info = prepare_encoded_features(
            self.sample_df, self.features_meta
        )
        self.assertEqual(len(norm_df), 8)
        self.assertEqual(X_weighted.shape[0], 8)
        self.assertIn("age", enc_info["max_values"])
        self.assertIn("income", enc_info["max_values"])
        self.assertIn("sex", enc_info["cat_mappings"])
        self.assertIn("sex", enc_info["onehot_matrices"])

        # Age max normalization check
        expected_age_max = 60.0
        self.assertEqual(enc_info["max_values"]["age"], expected_age_max)
        np.testing.assert_allclose(norm_df["age"].to_numpy(), self.sample_df["age"] / 60.0)

        # Categorical one-hot check
        sex_onehot = enc_info["onehot_matrices"]["sex"]
        self.assertEqual(sex_onehot.shape, (8, 2))
        np.testing.assert_allclose(sex_onehot.sum(axis=1), np.ones(8))

    def test_compute_pairwise_distances(self):
        dists = compute_pairwise_distances(
            self.sample_df,
            config_or_path={
                "data_structure": {
                    "hierarchy": ["individual"],
                    "features": self.features_meta,
                }
            },
        )
        self.assertEqual(dists.shape, (8, 8))
        # Diagonal must be identically 0
        np.testing.assert_allclose(np.diag(dists), np.zeros(8))
        # Matrix must be symmetric
        np.testing.assert_allclose(dists, dists.T)
        # All pairwise distances non-negative
        self.assertTrue((dists >= 0).all())

    def test_compute_weighted_mean_std(self):
        # Empty array
        m, s = _compute_weighted_mean_std(np.array([]))
        self.assertTrue(np.isnan(m))
        self.assertTrue(np.isnan(s))

        # Single element
        m, s = _compute_weighted_mean_std(np.array([10.0]))
        self.assertEqual(m, 10.0)
        self.assertEqual(s, 0.0)

        # Unweighted multiple elements
        vals = np.array([10.0, 20.0, 30.0])
        m, s = _compute_weighted_mean_std(vals)
        self.assertAlmostEqual(m, 20.0)
        self.assertAlmostEqual(s, 10.0)

        # Weighted elements
        weights = np.array([1.0, 2.0, 1.0])
        m_w, s_w = _compute_weighted_mean_std(vals, weights)
        self.assertAlmostEqual(m_w, 20.0)
        # Sum of w = 4. Var = [1*(10-20)^2 + 2*(20-20)^2 + 1*(30-20)^2] / 4 * (3 / 2) = 200/4 * 1.5 = 75
        self.assertAlmostEqual(s_w, np.sqrt(75.0))

        # Zero or negative weights fallback to unweighted
        m_zero, s_zero = _compute_weighted_mean_std(vals, np.array([0.0, 0.0, 0.0]))
        self.assertAlmostEqual(m_zero, 20.0)
        self.assertAlmostEqual(s_zero, 10.0)

    def test_standard_nn_clusters_and_leftover(self):
        X = np.array([[0.0], [0.1], [0.2], [5.0], [5.1], [5.2], [10.0]])
        # min_count = 3 -> 2 clusters (indices [0, 1, 2] and [3, 4, 5]), record 6 leftover
        clusters_assign = _standard_nn_clusters(X, min_count=3, leftover_action="assign_nearest")
        self.assertEqual(len(clusters_assign), 2)
        total_assigned = sum(len(c) for c in clusters_assign)
        self.assertEqual(total_assigned, 7)
        # Record 6 is closest to cluster [3, 4, 5]
        self.assertIn(6, clusters_assign[1])

        clusters_suppress = _standard_nn_clusters(X, min_count=3, leftover_action="suppress")
        self.assertEqual(len(clusters_suppress), 2)
        total_suppressed = sum(len(c) for c in clusters_suppress)
        self.assertEqual(total_suppressed, 6)

        # Invalid leftover action raises ValueError
        with self.assertRaises(ValueError):
            _standard_nn_clusters(X, min_count=3, leftover_action="invalid_action")

    def test_nearest_neighbour_grouping_subcell_rules(self):
        X = np.zeros((10, 2))
        onehot = {"cat": np.array([[1, 0]] * 8 + [[0, 1]] * 2)}

        # min_cell_count = 3, category 1 has only 2 counts in the entire dataset
        # subcell_action='none' should return normal clusters
        clusters_none = nearest_neighbour_grouping(
            X, min_count=5, leftover_action="assign_nearest",
            onehot_matrices=onehot, subcell_action="none", min_cell_count=3
        )
        self.assertTrue(len(clusters_none) >= 1)

        # Dataset smaller than min_count returns empty list
        empty_clusters = nearest_neighbour_grouping(X[:3], min_count=5)
        self.assertEqual(empty_clusters, [])

    def test_joint_continuous_and_categorical_repair_avoids_merging(self):
        """
        Verify 17M + 3F (with min_count=20, min_cell_count=6) is repaired to 14M+6F or 20M+0F
        using nearest compatible records, keeping clusters at size 20 instead of merging to 40.
        """
        # Cluster region 1 (near age 0.0): 17 Male + 3 Female = 20 records
        # Cluster region 2 (near age 1.0): 9 Male + 11 Female = 20 records
        X = np.array(
            [[0.0, 1.0]] * 17
            + [[0.05, 0.0]] * 3
            + [[1.0, 1.0]] * 9
            + [[0.95, 0.0]] * 11
        )
        onehot = {
            "sex": np.array(
                [[1.0, 0.0]] * 17
                + [[0.0, 1.0]] * 3
                + [[1.0, 0.0]] * 9
                + [[0.0, 1.0]] * 11
            )
        }
        clusters = nearest_neighbour_grouping(
            X,
            min_count=20,
            leftover_action="assign_nearest",
            onehot_matrices=onehot,
            subcell_action="reassign_or_merge",
            min_cell_count=6,
        )
        # Must remain 2 clusters of size 20 (never merged into 1 cluster of 40)
        self.assertEqual(len(clusters), 2)
        for c in clusters:
            self.assertEqual(len(c), 20)
            counts = onehot["sex"][c].sum(axis=0)
            for cnt in counts:
                self.assertTrue(cnt == 0 or cnt >= 6, f"Invalid category count {cnt} in {counts}")

    def test_aggregate_unit_records_unweighted_and_weighted(self):
        # 12 records with age and sex
        df = pd.DataFrame(
            {
                "age": [20, 22, 24, 25, 26, 28, 40, 42, 44, 45, 48, 50],
                "sex": ["M", "M", "M", "F", "F", "F", "M", "M", "M", "F", "F", "F"],
                "w": [1.0, 2.0, 1.0, 1.0, 1.0, 2.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
            }
        )
        cfg = {
            "output_rules": {
                "min_group_count": 6,
                "min_cell_count": 3,
                "rounding_base": None,
                "subcell_action": "none",
                "leftover_action": "assign_nearest",
            },
            "data_structure": {
                "hierarchy": ["individual"],
                "features": {
                    "age": {"type": "continuous", "weight": 1.0},
                    "sex": {"type": "categorical", "weight": 1.0},
                },
            },
        }
        agg_df, details = aggregate_unit_records(
            df, cfg, weight_col="w", return_details=True
        )
        self.assertEqual(len(agg_df), 2)
        self.assertIn("age", agg_df.columns)
        self.assertIn("age_std", agg_df.columns)
        self.assertIn("w", agg_df.columns)
        self.assertIn("w_std", agg_df.columns)
        self.assertIn("count", agg_df.columns)
        self.assertEqual(details["weight_col"], "w")
        self.assertEqual(len(details["clusters"]), 2)

    def test_build_unit_hierarchy_table(self):
        cat_mappings = {
            "sex": {"categories": ["male", "female"], "columns": ["male", "female"]}
        }
        fam_df, fam_meta = build_unit_hierarchy_table(
            df=self.sample_df,
            group_id_cols=["family_id"],
            features_meta=self.features_meta,
            cat_mappings=cat_mappings,
            child_count_col="individual_num",
            hierarchy_counts={"minor": {"condition": "age < 18", "weight": 2.0}},
            level_name="family",
        )
        # 8 records across 4 families -> 4 rows
        self.assertEqual(len(fam_df), 4)
        self.assertIn("individual_num", fam_df.columns)
        self.assertIn("minor", fam_df.columns)
        # Check income sum aggregation: family 10 has person 101 ($50k) and person 102 ($60k) -> $110k
        fam10 = fam_df[fam_df["family_id"] == 10].iloc[0]
        self.assertEqual(fam10["income"], 110000.0)
        # Check age mean aggregation: family 10 has 25 and 30 -> 27.5
        self.assertEqual(fam10["age"], 27.5)
        # Check child count: 2
        self.assertEqual(fam10["individual_num"], 2.0)
        # Check minor count: family 30 has person 301 (age 15) -> 1.0, others have 0.0
        fam30 = fam_df[fam_df["family_id"] == 30].iloc[0]
        self.assertEqual(fam30["minor"], 1.0)
        fam20 = fam_df[fam_df["family_id"] == 20].iloc[0]
        self.assertEqual(fam20["minor"], 0.0)

    def test_add_correlation_features_all_pair_types(self):
        df = pd.DataFrame(
            {
                "age": [20.0, 60.0, 20.0, 60.0],
                "income": [20000.0, 80000.0, 80000.0, 20000.0],
                "sex": ["M", "F", "M", "F"],
                "occ": ["Eng", "Mgr", "Mgr", "Eng"],
            }
        )
        features_meta = {
            "age": {"type": "continuous", "weight": 1.0},
            "income": {"type": "continuous", "weight": 1.0},
            "sex": {"type": "categorical", "weight": 1.0},
            "occ": {"type": "categorical", "weight": 1.0},
        }
        correlations = [
            {"name": "corr_age_income", "features": ["age", "income"], "weight": 2.0},
            {"name": "corr_age_sex", "features": ["age", "sex"], "weight": 1.5},
            {"name": "corr_sex_occ", "features": ["sex", "occ"], "weight": 1.0},
        ]

        df_corr, corr_meta = add_correlation_features(df, features_meta, correlations)
        # 1. Continuous x Continuous: z-score product (+1 for aligned, -1 for opposite)
        self.assertIn("corr_age_income", df_corr.columns)
        self.assertAlmostEqual(df_corr["corr_age_income"].iloc[0], 1.0)
        self.assertAlmostEqual(df_corr["corr_age_income"].iloc[1], 1.0)
        self.assertAlmostEqual(df_corr["corr_age_income"].iloc[2], -1.0)
        self.assertAlmostEqual(df_corr["corr_age_income"].iloc[3], -1.0)
        self.assertEqual(corr_meta["corr_age_income"]["weight"], 2.0)

        # 2. Continuous x Categorical: age per sex category
        self.assertIn("corr_age_sex_M", df_corr.columns)
        self.assertIn("corr_age_sex_F", df_corr.columns)
        self.assertGreater(df_corr["corr_age_sex_M"].iloc[0], 0.0)
        self.assertEqual(df_corr["corr_age_sex_F"].iloc[0], 0.0)

        # 3. Categorical x Categorical: joint category
        self.assertIn("corr_sex_occ", df_corr.columns)
        self.assertEqual(list(df_corr["corr_sex_occ"]), ["M__Eng", "F__Mgr", "M__Mgr", "F__Eng"])
        self.assertEqual(corr_meta["corr_sex_occ"]["type"], "categorical")

        # 4. Pre-encoded proportions (family/household level)
        prop_df = pd.DataFrame(
            {
                "age": [30.0, 50.0],
                "M": [0.5, 1.0],
                "F": [0.5, 0.0],
                "Eng": [1.0, 0.0],
                "Mgr": [0.0, 1.0],
            }
        )
        pre_enc = {
            "sex": {"categories": ["M", "F"], "columns": ["M", "F"]},
            "occ": {"categories": ["Eng", "Mgr"], "columns": ["Eng", "Mgr"]},
        }
        prop_corr, prop_meta = add_correlation_features(
            prop_df, features_meta, correlations[1:], pre_encoded_categories=pre_enc
        )
        self.assertIn("corr_sex_occ_M_Eng", prop_corr.columns)
        self.assertAlmostEqual(prop_corr["corr_sex_occ_M_Eng"].iloc[0], 0.5)

    def test_correlation_distance_and_aggregation(self):
        df = pd.DataFrame(
            {
                "age": [20.0, 22.0, 24.0, 60.0, 62.0, 64.0, 20.0, 22.0, 24.0, 60.0, 62.0, 64.0],
                "income": [20000.0, 22000.0, 24000.0, 80000.0, 82000.0, 84000.0, 80000.0, 82000.0, 84000.0, 20000.0, 22000.0, 24000.0],
                "sex": ["M", "M", "M", "F", "F", "F", "M", "M", "M", "F", "F", "F"],
            }
        )
        base_cfg = {
            "output_rules": {
                "min_group_count": 6,
                "min_cell_count": 3,
                "rounding_base": None,
                "subcell_action": "none",
                "leftover_action": "assign_nearest",
            },
            "data_structure": {
                "hierarchy": ["individual"],
                "features": {
                    "age": {"type": "continuous", "weight": 1.0},
                    "income": {"type": "continuous", "weight": 1.0},
                    "sex": {"type": "categorical", "weight": 1.0},
                },
            },
        }
        corr_cfg = {
            "output_rules": base_cfg["output_rules"],
            "data_structure": {
                **base_cfg["data_structure"],
                "correlations": [
                    {"features": ["age", "income"], "weight": 3.0},
                    {"features": ["age", "sex"], "weight": 1.5},
                ],
            },
        }
        d_base = compute_pairwise_distances(df, base_cfg)
        d_corr = compute_pairwise_distances(df, corr_cfg)
        # Distance between opposite-correlation records (0 and 6) must strictly increase when correlations are enabled
        self.assertGreater(d_corr[0, 6], d_base[0, 6])

        # Aggregated output must NOT leak internal corr_* columns, while normalized_df records them
        agg_df, details = aggregate_unit_records(df, corr_cfg, return_details=True)
        self.assertEqual(len(agg_df), 2)
        for col in agg_df.columns:
            self.assertFalse(col.startswith("corr_"))
        self.assertIn("corr_age_income", details["normalized_df"].columns)
        self.assertEqual(len(details["correlations"]), 2)


class TestReport(unittest.TestCase):
    def setUp(self):
        self.metadata = {
            "hierarchy": ["individual", "household"],
            "id_columns": {"individual": "person_id", "household": "household_id"},
            "output_rules": {
                "min_group_count": 6,
                "min_cell_count": 3,
                "rounding_base": 3,
                "subcell_action": "reassign_or_merge",
                "leftover_action": "assign_nearest",
            },
            "weight_info": {"weight_col": "weight", "weight_level": "individual"},
        }
        self.raw_df = pd.DataFrame(
            {
                "person_id": [1, 2, 3, 4, 5, 6],
                "age": [20.0, 22.0, 24.0, 26.0, 28.0, 30.0],
                "sex": ["M", "M", "M", "F", "F", "F"],
            }
        )
        self.norm_df = pd.DataFrame(
            {
                "age": [0.66, 0.73, 0.8, 0.86, 0.93, 1.0],
                "M": [1.0, 1.0, 1.0, 0.0, 0.0, 0.0],
                "F": [0.0, 0.0, 0.0, 1.0, 1.0, 1.0],
            }
        )
        self.level_details = {
            "raw_df": self.raw_df,
            "normalized_df": self.norm_df,
            "cluster_audit_records": [
                {
                    "cluster_id": 1,
                    "status": "RELEASED",
                    "suppression_reason": "",
                    "indices": np.array([0, 1, 2, 3, 4, 5]),
                    "unrounded_count": 6,
                    "min_group_count": 6,
                    "min_cell_count": 3,
                    "rounded_count": 6,
                    "rounding_base": 3,
                    "continuous_audit": {
                        "age": {
                            "raw_count": 6,
                            "min_group_count": 6,
                            "raw_mean": 25.0,
                            "exported_mean": 25.0,
                            "raw_std": 3.74,
                            "exported_std": 3.74,
                            "status": "PASS",
                        }
                    },
                    "categorical_audit": {
                        "M": {
                            "feature": "sex",
                            "category": "M",
                            "unrounded_contributing_count": 3,
                            "raw_sum": 3.0,
                            "raw_proportion": 0.5,
                            "exported_proportion": 0.5,
                            "min_cell_count": 3,
                            "status": "PASS",
                        },
                        "F": {
                            "feature": "sex",
                            "category": "F",
                            "unrounded_contributing_count": 3,
                            "raw_sum": 3.0,
                            "raw_proportion": 0.5,
                            "exported_proportion": 0.5,
                            "min_cell_count": 3,
                            "status": "PASS",
                        },
                    },
                }
            ],
        }

    def test_build_checker_guide_df(self):
        guide_df = build_checker_guide_df(self.metadata)
        self.assertIsInstance(guide_df, pd.DataFrame)
        self.assertTrue(len(guide_df) >= 5)
        self.assertIn("Sheet / Item", guide_df.columns)
        self.assertIn("Description", guide_df.columns)

    def test_build_raw_cluster_mapping_df(self):
        mapping_df = build_raw_cluster_mapping_df(self.level_details)
        self.assertEqual(len(mapping_df), 6)
        self.assertIn("cluster_id", mapping_df.columns)
        self.assertIn("cluster_status", mapping_df.columns)
        self.assertIn("cluster_raw_count", mapping_df.columns)
        self.assertIn("cluster_released_count", mapping_df.columns)
        self.assertEqual(list(mapping_df["cluster_id"]), [1] * 6)
        self.assertEqual(list(mapping_df["cluster_status"]), ["RELEASED"] * 6)

    def test_build_checker_audit_df(self):
        audit_df = build_checker_audit_df("individual", self.level_details)
        # Expected rows: 1 for cluster count, 2 for age (mean, std), 2 for sex (M, F) -> 5 rows
        self.assertEqual(len(audit_df), 5)
        self.assertEqual(
            list(audit_df.columns),
            [
                "level",
                "cluster_id",
                "feature",
                "metric_or_category",
                "raw_count",
                "min_required",
                "raw_value",
                "released_value",
                "status",
            ],
        )
        self.assertTrue((audit_df["status"] == "PASS").all())

    def test_build_rules_summary_df(self):
        metadata_with_levels = dict(self.metadata)
        metadata_with_levels["levels"] = {"individual": self.level_details}
        agg_tables = {"individual": pd.DataFrame([{"cluster_id": 1, "count": 6}])}

        summary_df = build_rules_summary_df(agg_tables, metadata_with_levels)
        self.assertEqual(len(summary_df), 1)
        self.assertEqual(summary_df.iloc[0]["hierarchy_level"], "individual")
        self.assertEqual(summary_df.iloc[0]["raw_units"], 6)
        self.assertEqual(summary_df.iloc[0]["released_clusters"], 1)
        self.assertEqual(summary_df.iloc[0]["status"], "PASS")

    def test_build_released_with_counts_df(self):
        table_df = pd.DataFrame(
            [{"cluster_id": 1, "age": 25.0, "age_std": 3.74, "M": 0.5, "F": 0.5, "count": 6}]
        )
        out_df = build_released_with_counts_df(table_df, self.level_details)
        self.assertEqual(
            list(out_df.columns),
            [
                "cluster_id",
                "age",
                "age_std",
                "unrounded_age_count",
                "M",
                "unrounded_M_count",
                "F",
                "unrounded_F_count",
                "unrounded_count",
                "rounded_count",
            ],
        )
        self.assertEqual(out_df.iloc[0]["unrounded_age_count"], 6)
        self.assertEqual(out_df.iloc[0]["unrounded_M_count"], 3)
        self.assertEqual(out_df.iloc[0]["unrounded_F_count"], 3)
        self.assertEqual(out_df.iloc[0]["unrounded_count"], 6)
        self.assertEqual(out_df.iloc[0]["rounded_count"], 6)

    def test_export_datalab_checker_reports(self):
        metadata_with_levels = dict(self.metadata)
        metadata_with_levels["levels"] = {"individual": self.level_details}
        agg_tables = {
            "individual": pd.DataFrame(
                [{"cluster_id": 1, "age": 25.0, "age_std": 3.74, "M": 0.5, "F": 0.5, "count": 6}]
            )
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            saved = export_datalab_checker_reports(agg_tables, metadata_with_levels, tmp_dir)
            self.assertIsNone(saved["summary_csv"])
            self.assertIn("excel_report", saved)
            self.assertTrue(os.path.exists(saved["excel_report"]))
            self.assertEqual(saved["released_csvs"], {})
            self.assertEqual(saved["raw_derivation_csvs"], {})
            self.assertEqual(saved["checker_audit_csvs"], {})
            self.assertEqual([f for f in os.listdir(tmp_dir) if f.endswith(".csv")], [])

            excel_released_df = pd.read_excel(saved["excel_report"], sheet_name="Ind_Released_Output")
            self.assertIn("unrounded_age_count", excel_released_df.columns)
            self.assertIn("unrounded_M_count", excel_released_df.columns)
            self.assertIn("unrounded_F_count", excel_released_df.columns)
            self.assertIn("unrounded_count", excel_released_df.columns)
            self.assertIn("rounded_count", excel_released_df.columns)
            self.assertEqual(excel_released_df.iloc[0]["unrounded_age_count"], 6)
            self.assertEqual(excel_released_df.iloc[0]["unrounded_M_count"], 3)
            self.assertEqual(excel_released_df.iloc[0]["unrounded_F_count"], 3)
            self.assertEqual(excel_released_df.iloc[0]["unrounded_count"], 6)
            self.assertEqual(excel_released_df.iloc[0]["rounded_count"], 6)

            saved_csv = export_datalab_checker_reports(
                agg_tables, metadata_with_levels, tmp_dir, export_csv_reports=True
            )
            self.assertTrue(os.path.exists(saved_csv["released_csvs"]["individual"]))
            self.assertTrue(os.path.exists(saved_csv["summary_csv"]))
            self.assertTrue(os.path.exists(saved_csv["raw_derivation_csvs"]["individual"]))
            self.assertTrue(os.path.exists(saved_csv["checker_audit_csvs"]["individual"]))

    def test_evaluate_and_export_synthetic_verification_report(self):
        raw_df = pd.DataFrame(
            {
                "age": [20.0, 25.0, 30.0, 35.0, 40.0, 45.0],
                "income": [40000.0, 50000.0, 60000.0, 70000.0, 80000.0, 90000.0],
                "sex": ["M", "M", "M", "F", "F", "F"],
                "region": ["A", "A", "B", "B", "A", "B"],
            }
        )
        # Identical synthetic dataset should achieve exact 1.0 similarity and 0.0 distances
        res_exact = evaluate_synthetic_data(
            raw_df=raw_df,
            syn_df=raw_df.copy(),
            columns=["age", "income", "sex", "region"],
            diagnostics={"residuals": {"family": {"age_sum": 0.25}}},
        )
        self.assertTrue((res_exact["continuous_fidelity"]["ks_statistic"] == 0.0).all())
        self.assertTrue((res_exact["continuous_fidelity"]["wasserstein_distance"] == 0.0).all())
        self.assertTrue((res_exact["categorical_summary"]["tvd"] == 0.0).all())
        self.assertTrue((res_exact["pairwise_correlations"]["abs_diff"] < 1e-12).all())
        self.assertEqual(len(res_exact["hierarchy_residuals"]), 1)

        with tempfile.TemporaryDirectory() as tmp_dir:
            xlsx_path = export_synthetic_verification_report(
                raw_df=raw_df,
                syn_df=raw_df.copy(),
                output_dir=tmp_dir,
                columns=["age", "income", "sex", "region"],
                diagnostics={"residuals": {"family": {"age_sum": 0.25}}},
            )
            self.assertTrue(os.path.exists(xlsx_path))
            xl = pd.ExcelFile(xlsx_path)
            self.assertEqual(
                xl.sheet_names,
                [
                    "0_Metric_Guide",
                    "1_Overall_Summary",
                    "2_Continuous_Fidelity",
                    "3_Categorical_Summary",
                    "4_Category_Proportions",
                    "5_Pairwise_Correlations",
                    "6_Hierarchy_Residuals",
                ],
            )


if __name__ == "__main__":
    unittest.main()
