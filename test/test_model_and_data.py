import os
import tempfile
import unittest
from unittest.mock import patch
import numpy as np
import pandas as pd

from syspop.model.stochastic_impute import get_target_values, stochastic_impute
from syspop.model.utils import obtain_all_tasks, check_deps_charts
from syspop.data.data import encode_weights, encode_sum, obtain_data
from syspop.data.utils import stats_data_proc, check_data_consistency


class TestModelUtils(unittest.TestCase):
    def test_obtain_all_tasks(self):
        target_cfg = {
            "task_a": {"features": ["age"], "targets": {"income": "continuous"}},
            "task_b": {"features": ["income"], "targets": {"car": "category"}},
        }
        # Chained tasks: "task_a -> task_b"
        chained = obtain_all_tasks("task_a -> task_b", target_cfg)
        self.assertEqual(list(chained.keys()), ["task_a", "task_b"])
        self.assertEqual(chained["task_a"], target_cfg["task_a"])
        self.assertEqual(chained["task_b"], target_cfg["task_b"])

        # Single task as list or string
        single = obtain_all_tasks("task_a", target_cfg)
        self.assertEqual(list(single.keys()), ["task_a"])

    def test_check_deps_charts(self):
        # Should execute without throwing error regardless of graphviz availability
        models_cfg = {
            "task_a": {"features": ["age", "sex"], "targets": {"income": "continuous"}},
        }
        with tempfile.TemporaryDirectory() as tmp_dir:
            check_deps_charts(models_cfg, output_dir=tmp_dir)


class TestStochasticImpute(unittest.TestCase):
    def setUp(self):
        np.random.seed(42)

    def test_get_target_values(self):
        # Test exact match
        prob_mapping = {
            ("M",): (np.array(["A", "B"]), np.array([0.5, 0.5])),
        }
        group = pd.DataFrame({"dummy": [1, 2, 3]})
        group.name = "M"
        res = get_target_values(group, prob_mapping)
        self.assertEqual(len(res), 3)
        self.assertTrue(set(res.values).issubset({"A", "B"}))

        # Test missing key returns NaNs
        group_missing = pd.DataFrame({"dummy": [1]})
        group_missing.name = "Unknown"
        res_missing = get_target_values(group_missing, prob_mapping)
        self.assertTrue(res_missing.isna().all())

    def test_stochastic_impute_categorical_and_maintain_counts(self):
        # Seed with 10 rows
        seed_df = pd.DataFrame(
            {
                "age_group": ["young", "young", "old", "old"] * 5,
                "value": [1] * 20,
            }
        )
        task_table = pd.DataFrame(
            {
                "age_group": ["young", "young", "old", "old"],
                "education": ["high", "low", "high", "low"],
                "value": [8, 2, 3, 7],
            }
        )
        data_dict = {
            "seed": seed_df,
            "task_edu": task_table,
        }
        task_list = {
            "task_edu": {
                "features": ["age_group"],
                "targets": {"education": "category"},
            }
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            res_df = stochastic_impute(
                data_dict=data_dict,
                task_list=task_list,
                maintain_counts=["task_edu"],
                output_dir=tmp_dir,
                output_filename="imputed.parquet",
            )
            self.assertEqual(len(res_df), 20)
            self.assertIn("education", res_df.columns)
            self.assertTrue(os.path.exists(os.path.join(tmp_dir, "imputed.parquet")))
            # Check maintain_counts: sum of values in task_edu was 20, so valid non-NaN count <= 20
            self.assertTrue(res_df["education"].notna().sum() <= 20)

    def test_stochastic_impute_continuous_averaging(self):
        # Seed has an existing continuous column that gets updated with mean logic
        seed_df = pd.DataFrame(
            {
                "region": ["north", "south", "north", "south"],
                "score": [10.0, 20.0, 30.0, 40.0],
                "value": [1, 1, 1, 1],
            }
        )
        task_table = pd.DataFrame(
            {
                "region": ["north", "south"],
                "score": [50.0, 60.0],
                "value": [5, 5],
            }
        )
        data_dict = {
            "seed": seed_df,
            "task_score": task_table,
        }
        task_list = {
            "task_score": {
                "features": ["region"],
                "targets": {"score": "continuous"},
            }
        }
        with tempfile.TemporaryDirectory() as tmp_dir:
            res_df = stochastic_impute(
                data_dict=data_dict,
                task_list=task_list,
                output_dir=tmp_dir,
            )
            # Continuous targets already in result_df are combined by mean
            # North row 0: (10.0 + 50.0) / 2 = 30.0
            self.assertEqual(len(res_df), 4)
            self.assertAlmostEqual(res_df.loc[0, "score"], 30.0)


class TestDataModule(unittest.TestCase):
    def test_encode_weights_and_sum(self):
        seed_df = pd.DataFrame(
            {
                "cat": ["A", "B"],
                "value": [2, 3],
            }
        )
        other_df = pd.DataFrame(
            {
                "cat": ["A", "B", "A", "B"],
                "target": [1, 1, 2, 2],
                "value": [10, 20, 30, 40],
            }
        )
        data_dict = {
            "seed": seed_df,
            "table2": other_df,
        }

        # Test encode_sum
        sum_dict = encode_sum(data_dict, maintain_counts=["seed", "table2"])
        self.assertEqual(sum_dict["seed"], 5)
        self.assertEqual(sum_dict["table2"], 100)

        # Test encode_weights
        enc_dict = encode_weights(data_dict)
        # seed should be repeated 2 + 3 = 5 times and "value" column dropped
        self.assertEqual(len(enc_dict["seed"]), 5)
        self.assertNotIn("value", enc_dict["seed"].columns)
        self.assertEqual(list(enc_dict["seed"]["cat"]), ["A", "A", "B", "B", "B"])

        # table2 should have probability column summing to 1.0
        self.assertIn("probability", enc_dict["table2"].columns)
        self.assertNotIn("value", enc_dict["table2"].columns)
        self.assertAlmostEqual(enc_dict["table2"]["probability"].sum(), 1.0)

    def test_stats_data_proc(self):
        raw_df = pd.DataFrame(
            {
                "raw_col1": ["A", "B", "C"],
                "raw_val": ["10", "25", "invalid_num"],
            }
        )
        cfg = {"map": {"raw_col1": "category", "raw_val": "value"}}
        proc_df = stats_data_proc(raw_df, cfg)
        # Row with invalid_num dropped
        self.assertEqual(len(proc_df), 2)
        self.assertEqual(list(proc_df.columns), ["category", "value"])
        self.assertEqual(list(proc_df["value"]), [10, 25])
        self.assertTrue(np.issubdtype(proc_df["value"].dtype, np.integer))

    def test_obtain_data_with_mock_api(self):
        fake_api_df = pd.DataFrame(
            {
                "col_a": ["keep1", "keep2", "exclude_me", "drop_me"],
                "col_val": ["1", "2", "3", "4"],
            }
        )
        cfg = {
            "api": {"endpoint": "mock"},
            "map": {"col_a": "group", "col_val": "value"},
            "inclusion": {"group": ["keep1", "keep2", "exclude_me"]},
            "exclusion": {"group": ["exclude_me"]},
        }
        with patch("syspop.data.data.obtain_stats_data", return_value=fake_api_df):
            df = obtain_data(cfg, api_key="test_key")
            # keep1 repeated 1 time, keep2 repeated 2 times -> 3 rows
            # 'exclude_me' excluded by exclusion rule, 'drop_me' excluded by inclusion rule
            self.assertEqual(len(df), 2)
            self.assertEqual(set(df["group"]), {"keep1", "keep2"})

    def test_check_data_consistency(self):
        df1 = pd.DataFrame({"region": ["North", "South"], "age": [20, 30]})
        df2 = pd.DataFrame({"region": ["North", "South"], "income": [5000, 6000]})
        consistent_dict = {"d1": df1, "d2": df2}

        with tempfile.TemporaryDirectory() as tmp_dir:
            # Consistent datasets should pass without error
            check_data_consistency(consistent_dict, check_err=True, output_dir=tmp_dir)
            self.assertTrue(os.path.exists(os.path.join(tmp_dir, "data_consistency.csv")))

        # Inconsistent datasets should throw error when throw_err=True
        df3 = pd.DataFrame({"region": ["North", "East"]})
        inconsistent_dict = {"d1": df1, "d3": df3}
        with self.assertRaises(ValueError):
            check_data_consistency(inconsistent_dict, check_err=True, throw_err=True)


if __name__ == "__main__":
    unittest.main()
