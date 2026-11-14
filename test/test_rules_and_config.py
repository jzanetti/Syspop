import os
import sys
import tempfile
import unittest
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from syspop.confidential.rules import (
    rr3_round,
    apply_count_rounding,
    check_subcell_violation,
    confidentialise_category_proportions,
)
from syspop.confidential.config import (
    load_config,
    resolve_data_structure,
    resolve_weight_dataframe,
    _infer_feature_type,
    _normalize_type_name,
    _normalize_agg_name,
    _normalize_rounding_base,
)


class TestConfidentialRules(unittest.TestCase):
    def test_rr3_round_exact_multiples(self):
        """Values that are exact multiples of base must not change."""
        rng = np.random.default_rng(42)
        for base in (3, 5, 10):
            for mult in (0, base, base * 2, base * 7):
                self.assertEqual(rr3_round(mult, base=base, rng=rng), mult)

    def test_rr3_round_zero_and_negative(self):
        """Values <= 0 round to 0."""
        self.assertEqual(rr3_round(0, base=3), 0)
        self.assertEqual(rr3_round(-5, base=3), 0)
        self.assertEqual(rr3_round(0.2, base=3), 0)

    def test_rr3_round_probabilities(self):
        """Values round up/down with probability proportional to distance to nearest multiple."""
        rng = np.random.default_rng(123)
        # Value 7 with base 3: remainder 1 -> lower 6 (prob 2/3), upper 9 (prob 1/3)
        trials = [rr3_round(7, base=3, rng=rng) for _ in range(3000)]
        for res in trials:
            self.assertIn(res, (6, 9))
        mean_res = float(np.mean(trials))
        # Expected value is 7.0
        self.assertAlmostEqual(mean_res, 7.0, delta=0.1)

    def test_apply_count_rounding_disabled(self):
        """Rounding can be turned off via None, False, 0, 1, or string indicators."""
        rng = np.random.default_rng(42)
        for off_val in (None, False, 0, 1, "none", "null", "false", "off", "0", "1", ""):
            rules = {"rounding_base": off_val}
            self.assertEqual(apply_count_rounding(22, rules, rng=rng), 22)

    def test_apply_count_rounding_enabled(self):
        """Rounding works with integer or string representations of base >= 2."""
        rng = np.random.default_rng(42)
        res_int = apply_count_rounding(22, {"rounding_base": 3}, rng=rng)
        self.assertIn(res_int, (21, 24))
        res_str = apply_count_rounding(22, {"rounding_base": "3"}, rng=rng)
        self.assertIn(res_str, (21, 24))

    def test_check_subcell_violation(self):
        """Verify detection of categorical cell counts strictly between 0 and min_cell_count."""
        # 3 individuals: [Male, Male, Female] -> Male has count 2, Female has count 1
        onehot = {
            "sex": np.array(
                [
                    [1.0, 0.0],  # Person 0: Male
                    [1.0, 0.0],  # Person 1: Male
                    [0.0, 1.0],  # Person 2: Female
                ]
            )
        }
        # With min_cell_count = 6, both 2 and 1 are in (0, 6) -> violation
        self.assertTrue(check_subcell_violation(np.array([0, 1, 2]), onehot, min_cell_count=6))
        # With min_cell_count = 2, Male (2) is valid, Female (1) is in (0, 2) -> violation
        self.assertTrue(check_subcell_violation(np.array([0, 1, 2]), onehot, min_cell_count=2))
        # Only Persons [0, 1] (Male=2, Female=0) with min_cell_count = 2 -> no violation
        self.assertFalse(check_subcell_violation(np.array([0, 1]), onehot, min_cell_count=2))
        # With min_cell_count <= 1 -> always False
        self.assertFalse(check_subcell_violation(np.array([0, 1, 2]), onehot, min_cell_count=1))

    def test_confidentialise_category_proportions_options(self):
        """Test all 5 subcell_action options: none, suppress_cluster, suppress_cell_nan, reassign_or_merge, rr3_and_suppress."""
        # Matrix with 10 records: 8 Category A, 2 Category B
        cat_mat = np.zeros((10, 2), dtype=float)
        cat_mat[:8, 0] = 1.0
        cat_mat[8:, 1] = 1.0

        # 1. Action: "none"
        rules_none = {"min_cell_count": 6, "subcell_action": "none"}
        p_none = confidentialise_category_proportions(cat_mat, rules_none)
        np.testing.assert_allclose(p_none, [0.8, 0.2])

        # 2. Action: "suppress_cluster"
        rules_sup = {"min_cell_count": 6, "subcell_action": "suppress_cluster"}
        p_sup = confidentialise_category_proportions(cat_mat, rules_sup)
        np.testing.assert_allclose(p_sup, [0.8, 0.2])

        # 3. Action: "suppress_cell_nan" (Cat B count = 2 < 6 -> set to NaN)
        rules_nan = {"min_cell_count": 6, "subcell_action": "suppress_cell_nan"}
        p_nan = confidentialise_category_proportions(cat_mat, rules_nan)
        self.assertAlmostEqual(p_nan[0], 0.8)
        self.assertTrue(np.isnan(p_nan[1]))

        # 4. Action: "reassign_or_merge" (Cat B masked to 0.0, Cat A normalized to 1.0)
        rules_mask = {"min_cell_count": 6, "subcell_action": "reassign_or_merge"}
        p_mask = confidentialise_category_proportions(cat_mat, rules_mask)
        np.testing.assert_allclose(p_mask, [1.0, 0.0])

        # 5. Action: "rr3_and_suppress"
        rng = np.random.default_rng(42)
        rules_rr3 = {
            "min_cell_count": 6,
            "subcell_action": "rr3_and_suppress",
            "rounding_base": 3,
        }
        p_rr3 = confidentialise_category_proportions(cat_mat, rules_rr3, rng=rng)
        # Cat B masked to 0, Cat A rounded from 8 to (6 or 9) and normalized to 1.0
        np.testing.assert_allclose(p_rr3, [1.0, 0.0])

    def test_confidentialise_category_proportions_weighted(self):
        """Verify thresholds checked on unweighted counts while proportions derived from survey weights."""
        # 10 records: 7 Category A (each weight 1.0), 3 Category B (each weight 5.0)
        cat_mat = np.zeros((10, 2), dtype=float)
        cat_mat[:7, 0] = 1.0
        cat_mat[7:, 1] = 1.0
        weights = np.array([1.0] * 7 + [5.0] * 3)

        # min_cell_count = 5. Unweighted Cat B count = 3 < 5 -> masked to 0 in reassign_or_merge
        rules = {"min_cell_count": 5, "subcell_action": "reassign_or_merge"}
        p_weighted = confidentialise_category_proportions(cat_mat, rules, unit_weights=weights)
        np.testing.assert_allclose(p_weighted, [1.0, 0.0])

        # min_cell_count = 3. Unweighted Cat B count = 3 >= 3 -> both valid!
        # Weighted sum: Cat A = 7 * 1.0 = 7.0, Cat B = 3 * 5.0 = 15.0 -> total = 22.0
        rules_ok = {"min_cell_count": 3, "subcell_action": "reassign_or_merge"}
        p_ok = confidentialise_category_proportions(cat_mat, rules_ok, unit_weights=weights)
        self.assertAlmostEqual(p_ok[0], 7.0 / 22.0)
        self.assertAlmostEqual(p_ok[1], 15.0 / 22.0)


class TestConfidentialConfig(unittest.TestCase):
    def test_load_config_defaults(self):
        """load_config without arguments loads default_rules.yml correctly."""
        cfg = load_config(None)
        rules = cfg["output_rules"]
        self.assertEqual(rules["rounding_base"], 3)
        self.assertEqual(rules["leftover_action"], "assign_nearest")
        self.assertEqual(rules["continuous"]["min_count"], 20)
        self.assertEqual(rules["categorical"]["min_count"], 6)
        self.assertEqual(rules["categorical"]["action"], "reassign_or_merge")
        # Backward-compatible flat aliases remain populated
        self.assertEqual(rules["min_group_count"], 20)
        self.assertEqual(rules["min_cell_count"], 6)
        self.assertEqual(rules["subcell_action"], "reassign_or_merge")

    def test_load_config_nested_continuous_categorical(self):
        """load_config supports top-level rounding/leftover and continuous/categorical sections."""
        cfg = load_config(
            {
                "output_rules": {
                    "rounding_base": None,
                    "leftover_action": "suppress",
                    "continuous": {
                        "min_count": 25,
                    },
                    "categorical": {
                        "min_count": 8,
                        "action": "rr3_and_suppress",
                    },
                }
            }
        )
        rules = cfg["output_rules"]
        self.assertIsNone(rules["rounding_base"])
        self.assertEqual(rules["leftover_action"], "suppress")
        self.assertEqual(rules["continuous"]["min_count"], 25)
        self.assertEqual(rules["categorical"]["min_count"], 8)
        self.assertEqual(rules["categorical"]["action"], "rr3_and_suppress")

    def test_load_config_yaml_file(self):
        """load_config from a temporary YAML file."""
        yaml_content = """
output_rules:
  min_group_count: 15
  min_cell_count: 5
  rounding_base: 5
data_structure:
  hierarchy:
    - individual
    - household
"""
        with tempfile.NamedTemporaryFile("w", suffix=".yml", delete=False) as tf:
            tf.write(yaml_content)
            tf_name = tf.name

        try:
            cfg = load_config(tf_name)
            self.assertEqual(cfg["output_rules"]["min_group_count"], 15)
            self.assertEqual(cfg["output_rules"]["min_cell_count"], 5)
            self.assertEqual(cfg["output_rules"]["rounding_base"], 5)
            self.assertEqual(cfg["data_structure"]["hierarchy"], ["individual", "household"])
        finally:
            if os.path.exists(tf_name):
                os.remove(tf_name)

    def test_load_config_invalid_type(self):
        """load_config raises TypeError on invalid argument types."""
        with self.assertRaises(TypeError):
            load_config([1, 2, 3])

    def test_normalize_rounding_base(self):
        """_normalize_rounding_base handles various inputs."""
        self.assertIsNone(_normalize_rounding_base(None))
        self.assertIsNone(_normalize_rounding_base(False))
        self.assertIsNone(_normalize_rounding_base("none"))
        self.assertIsNone(_normalize_rounding_base("null"))
        self.assertIsNone(_normalize_rounding_base(0))
        self.assertIsNone(_normalize_rounding_base(1))
        self.assertEqual(_normalize_rounding_base(3), 3)
        self.assertEqual(_normalize_rounding_base("5"), 5)

    def test_normalize_agg_name(self):
        """_normalize_agg_name supports mean, sum, min, max, median, and raises on invalid."""
        self.assertEqual(_normalize_agg_name(None), "mean")
        self.assertEqual(_normalize_agg_name("mean"), "mean")
        self.assertEqual(_normalize_agg_name("avg"), "mean")
        self.assertEqual(_normalize_agg_name("sum"), "sum")
        self.assertEqual(_normalize_agg_name("total"), "sum")
        self.assertEqual(_normalize_agg_name("min"), "min")
        self.assertEqual(_normalize_agg_name("max"), "max")
        self.assertEqual(_normalize_agg_name("median"), "median")
        with self.assertRaises(ValueError):
            _normalize_agg_name("geometric_mean")

    def test_infer_feature_type(self):
        """_infer_feature_type distinguishes continuous vs categorical correctly."""
        self.assertEqual(_infer_feature_type(pd.Series(["A", "B", "C"])), "categorical")
        self.assertEqual(_infer_feature_type(pd.Series([True, False, True])), "categorical")
        # Integer with few unique values -> categorical
        self.assertEqual(_infer_feature_type(pd.Series([1, 2, 1, 2, 1, 2, 1, 2, 1, 2] * 5)), "categorical")
        # Float with decimals -> continuous
        self.assertEqual(_infer_feature_type(pd.Series([1.5, 2.3, 3.8, 4.1, 5.0])), "continuous")
        # Empty series -> continuous
        self.assertEqual(_infer_feature_type(pd.Series([], dtype=float)), "continuous")

    def test_normalize_type_name(self):
        """_normalize_type_name standardizes continuous and categorical strings."""
        for c in ("continuous", "numeric", "float", "int", "number", "cont"):
            self.assertEqual(_normalize_type_name(c), "continuous")
        for k in ("categorical", "category", "cat", "string", "str", "bool"):
            self.assertEqual(_normalize_type_name(k), "categorical")
        with self.assertRaises(ValueError):
            _normalize_type_name("unsupported_type")

    def test_resolve_data_structure_hierarchy_and_counts(self):
        """resolve_data_structure properly extracts hierarchy, ID columns, features, and hierarchy_counts."""
        df = pd.DataFrame(
            {
                "household_id": [1, 2],
                "family_id": [1, 2],
                "person_id": [1, 2],
                "age": [25.0, 45.0],
                "sex": ["M", "F"],
            }
        )
        cfg = {
            "data_structure": {
                "hierarchy": ["individual", "family", "household"],
                "hierarchy_counts": {
                    "under_18": "age < 18",
                    "retiree": {"condition": "age >= 65", "weight": 2.0},
                },
                "features": {
                    "age": {"type": "continuous", "agg": "mean", "weight": 2.0},
                    "sex": {"type": "categorical", "weight": 1.0},
                },
            }
        }
        res = resolve_data_structure(df, cfg)
        self.assertEqual(res["hierarchy"], ["individual", "family", "household"])
        self.assertIn("under_18", res["hierarchy_counts"])
        self.assertEqual(res["hierarchy_counts"]["under_18"]["condition"], "age < 18")
        self.assertEqual(res["hierarchy_counts"]["retiree"]["condition"], "age >= 65")
        self.assertEqual(res["hierarchy_counts"]["retiree"]["weight"], 2.0)
        self.assertEqual(res["features"]["age"]["agg"], "mean")
        self.assertEqual(res["features"]["age"]["type"], "continuous")
        self.assertEqual(res["features"]["sex"]["type"], "categorical")

    def test_resolve_weight_dataframe_validations(self):
        """resolve_weight_dataframe validates column count and alignment."""
        df = pd.DataFrame(
            {
                "household_id": [1, 1, 2],
                "person_id": [1, 2, 3],
                "age": [20, 30, 40],
            }
        )
        id_cols = {"individual": "person_id", "household": "household_id"}
        hierarchy = ["individual", "household"]

        # 1. Valid household_weight aligned by shared ID
        w_df_hh = pd.DataFrame({"household_id": [1, 2], "household_weight": [2.5, 1.5]})
        aligned_df, w_info = resolve_weight_dataframe(df, w_df_hh, hierarchy, id_cols)
        self.assertEqual(w_info["weight_col"], "household_weight")
        self.assertEqual(w_info["weight_level"], "household")
        self.assertIn("household_weight", aligned_df.columns)

        # 2. Valid person_weight aligned by length
        w_df_ind = pd.DataFrame({"person_weight": [1.0, 2.0, 3.0]})
        aligned_ind, w_info_ind = resolve_weight_dataframe(df, w_df_ind, hierarchy, id_cols)
        self.assertEqual(w_info_ind["weight_col"], "person_weight")
        self.assertEqual(w_info_ind["weight_level"], "individual")

        # 3. Invalid: multiple weight columns in weight_df
        w_df_multi = pd.DataFrame(
            {
                "person_id": [1, 2, 3],
                "person_weight": [1.0, 2.0, 3.0],
                "household_weight": [2.0, 2.0, 1.0],
            }
        )
        with self.assertRaises(ValueError):
            resolve_weight_dataframe(df, w_df_multi, hierarchy, id_cols)

        # 4. Invalid: weight column name doesn't match any hierarchy level
        w_df_bad_name = pd.DataFrame(
            {"person_id": [1, 2, 3], "sample_multiplier": [1.0, 2.0, 3.0]}
        )
        with self.assertRaises(ValueError):
            resolve_weight_dataframe(df, w_df_bad_name, hierarchy, id_cols)

        # 5. Invalid: unaligned length without ID columns
        w_df_unaligned = pd.DataFrame({"person_weight": [1.0, 2.0]})
        with self.assertRaises(ValueError):
            resolve_weight_dataframe(df, w_df_unaligned, hierarchy, id_cols)

    def test_resolve_correlations_config(self):
        """resolve_data_structure parses correlations in list, dict, or string formats."""
        df = pd.DataFrame(
            {
                "person_id": [1, 2, 3],
                "age": [25, 35, 45],
                "income": [40000, 60000, 80000],
                "sex": ["M", "F", "M"],
            }
        )
        cfg = {
            "data_structure": {
                "features": {
                    "age": {"type": "continuous", "weight": 1.0},
                    "income": {"type": "continuous", "weight": 1.0},
                    "sex": {"type": "categorical", "weight": 1.0},
                },
                "correlations": [
                    {"features": ["age", "income"], "weight": 2.5},
                    ["age", "sex"],
                    "sex-income",
                ],
            }
        }
        struct = resolve_data_structure(df, cfg)
        corrs = struct["correlations"]
        self.assertEqual(len(corrs), 3)
        self.assertEqual(corrs[0], {"name": "corr_age_income", "features": ["age", "income"], "weight": 2.5})
        self.assertEqual(corrs[1], {"name": "corr_age_sex", "features": ["age", "sex"], "weight": 1.0})
        self.assertEqual(corrs[2], {"name": "corr_sex_income", "features": ["sex", "income"], "weight": 1.0})

        # Unknown feature in correlation raises ValueError
        bad_cfg = {
            "data_structure": {
                "features": cfg["data_structure"]["features"],
                "correlations": [["age", "nonexistent"]],
            }
        }
        with self.assertRaises(ValueError):
            resolve_data_structure(df, bad_cfg)


if __name__ == "__main__":
    unittest.main()
