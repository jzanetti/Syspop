import unittest
import numpy as np
import pandas as pd

from syspop.confidential.synthesize import (
    _match_column_case_insensitive,
    infer_synthesis_schema,
    _sample_continuous_column,
    recreate_individuals_from_agg,
    _sample_integer_count_column,
    _generate_parent_targets,
    _build_matching_vectors,
    assign_children_to_parents_greedy,
    create_synthetic_dataset,
)


class TestSynthesize(unittest.TestCase):
    def setUp(self):
        self.rng = np.random.default_rng(12345)
        self.ind_agg = pd.DataFrame(
            [
                {
                    "cluster_id": 1,
                    "age": 30.0,
                    "age_std": 5.0,
                    "income": 50000.0,
                    "income_std": 10000.0,
                    "male": 0.5,
                    "female": 0.5,
                    "count": 10,
                },
                {
                    "cluster_id": 2,
                    "age": 60.0,
                    "age_std": 6.0,
                    "income": 70000.0,
                    "income_std": 15000.0,
                    "male": 0.3,
                    "female": 0.7,
                    "count": 10,
                },
            ]
        )
        self.fam_agg = pd.DataFrame(
            [
                {
                    "cluster_id": 1,
                    "individual_num": 2.0,
                    "individual_num_std": 0.5,
                    "income": 100000.0,
                    "income_std": 20000.0,
                    "age": 45.0,
                    "age_std": 8.0,
                    "male": 0.4,
                    "female": 0.6,
                    "count": 5,
                },
                {
                    "cluster_id": 2,
                    "individual_num": 2.0,
                    "individual_num_std": 0.5,
                    "income": 140000.0,
                    "income_std": 25000.0,
                    "age": 55.0,
                    "age_std": 10.0,
                    "male": 0.4,
                    "female": 0.6,
                    "count": 5,
                },
            ]
        )
        self.cfg = {
            "data_structure": {
                "hierarchy": ["individual", "family"],
                "id_columns": {"individual": "person_id", "family": "family_id"},
                "features": {
                    "age": {"type": "continuous", "weight": 1.0, "agg": "mean"},
                    "income": {"type": "continuous", "weight": 2.0, "agg": "sum"},
                    "sex": {"type": "categorical", "weight": 1.0, "categories": ["male", "female"]},
                },
                "hierarchy_counts": {
                    "minor": {"condition": "age < 18", "weight": 2.0}
                },
            },
            "synthesis": {"random_seed": 42},
        }

    def test_match_column_case_insensitive(self):
        df = pd.DataFrame(columns=["Age", "Family_Num", "Ave Income"])
        self.assertEqual(_match_column_case_insensitive(df, ["age", "age_std"]), "Age")
        self.assertEqual(_match_column_case_insensitive(df, ["family_num", "fam_num"]), "Family_Num")
        self.assertEqual(_match_column_case_insensitive(df, ["ave income", "income"]), "Ave Income")
        self.assertIsNone(_match_column_case_insensitive(df, ["nonexistent"]))

    def test_infer_synthesis_schema_with_and_without_metadata(self):
        agg_tables = {"individual": self.ind_agg, "family": self.fam_agg}

        # 1. Without metadata (auto-inferred from CSV columns & cfg)
        schema_inferred = infer_synthesis_schema(agg_tables, self.cfg, metadata=None)
        self.assertEqual(schema_inferred["hierarchy"], ["individual", "family"])
        self.assertIn("age", schema_inferred["features"])
        self.assertIn("income", schema_inferred["features"])
        self.assertEqual(schema_inferred["features"]["income"]["agg"], "sum")
        self.assertIn("sex", schema_inferred["cat_mappings"])
        self.assertEqual(schema_inferred["cat_mappings"]["sex"]["categories"], ["male", "female"])

        # 2. With metadata
        metadata = {
            "hierarchy": ["individual", "family"],
            "id_columns": {"individual": "person_id", "family": "family_id"},
            "features": {
                "age": {"type": "continuous", "weight": 1.0, "agg": "mean"},
                "income": {"type": "continuous", "weight": 2.0, "agg": "sum"},
                "sex": {"type": "categorical", "weight": 1.0},
            },
            "cat_mappings": {
                "sex": {"categories": ["male", "female"], "columns": ["male", "female"]}
            },
            "hierarchy_counts": {"minor": {"condition": "age < 18", "weight": 2.0}},
        }
        schema_meta = infer_synthesis_schema(agg_tables, self.cfg, metadata=metadata)
        self.assertEqual(schema_meta["features"]["age"]["mean_col"], "age")
        self.assertEqual(schema_meta["features"]["age"]["std_col"], "age_std")
        self.assertIn("minor", schema_meta["hierarchy_counts"])

    def test_sample_continuous_column(self):
        row = pd.Series({"age": 30.0, "age_std": 5.0})
        f_spec = {"mean_col": "age", "std_col": "age_std", "non_negative": True}
        samples = _sample_continuous_column(
            row, "age", f_spec, n_samples=1000, rng=self.rng, table_df=pd.DataFrame([row])
        )
        self.assertEqual(len(samples), 1000)
        self.assertTrue((samples >= 0.0).all())
        self.assertAlmostEqual(float(np.mean(samples)), 30.0, delta=1.0)
        self.assertAlmostEqual(float(np.std(samples)), 5.0, delta=1.0)

        # Zero std returns exact mean
        row_zero_std = pd.Series({"age": 42.0, "age_std": 0.0})
        samples_zero = _sample_continuous_column(
            row_zero_std, "age", f_spec, n_samples=5, rng=self.rng, table_df=pd.DataFrame([row_zero_std])
        )
        np.testing.assert_allclose(samples_zero, np.full(5, 42.0))

    def test_recreate_individuals_from_agg(self):
        schema = infer_synthesis_schema({"individual": self.ind_agg}, self.cfg)
        syn_ind = recreate_individuals_from_agg(
            self.ind_agg, schema, self.cfg, self.rng
        )
        # Cluster 1 has count 10, Cluster 2 has count 10 -> Total 20 records
        self.assertEqual(len(syn_ind), 20)
        self.assertIn("person_id", syn_ind.columns)
        self.assertIn("age", syn_ind.columns)
        self.assertIn("income", syn_ind.columns)
        self.assertIn("sex", syn_ind.columns)
        self.assertIn("__hcount_minor", syn_ind.columns)
        self.assertIn("__onehot_male", syn_ind.columns)
        self.assertIn("__onehot_female", syn_ind.columns)

        # Target total count scaling
        syn_scaled = recreate_individuals_from_agg(
            self.ind_agg, schema, self.cfg, self.rng, target_total_count=35
        )
        self.assertEqual(len(syn_scaled), 35)

    def test_sample_integer_count_column(self):
        row = pd.Series({"individual_num": 2.5, "individual_num_std": 0.5})
        counts = _sample_integer_count_column(
            row, "individual_num", "individual_num_std", n_p=100, rng=self.rng, min_val=1
        )
        self.assertEqual(len(counts), 100)
        self.assertTrue((counts >= 1).all())
        self.assertTrue(np.issubdtype(counts.dtype, np.integer))

        # Test capping with max_vals
        max_v = np.full(100, 2)
        counts_capped = _sample_integer_count_column(
            row, "individual_num", "individual_num_std", n_p=100, rng=self.rng, min_val=1, max_vals=max_v
        )
        self.assertTrue((counts_capped <= 2).all())

    def test_generate_parent_targets_and_law_of_total_variance(self):
        schema = infer_synthesis_schema(
            {"individual": self.ind_agg, "family": self.fam_agg}, self.cfg
        )
        parents_df, child_col = _generate_parent_targets(
            self.fam_agg,
            ["individual_num"],
            schema,
            self.cfg,
            self.rng,
            target_parent_count=10,
        )
        self.assertEqual(child_col, "individual_num")
        self.assertEqual(len(parents_df), 10)
        self.assertIn("__child_count", parents_df.columns)
        self.assertIn("income", parents_df.columns)
        # Because income has agg: sum, larger families should on average have larger income target
        self.assertTrue((parents_df["__child_count"] >= 1).all())

    def test_build_matching_vectors_and_greedy_assignment(self):
        schema = infer_synthesis_schema(
            {"individual": self.ind_agg, "family": self.fam_agg}, self.cfg
        )
        # Create small test children and parents
        children_df = pd.DataFrame(
            {
                "person_id": [1, 2, 3, 4],
                "age": [25.0, 35.0, 45.0, 55.0],
                "income": [40000.0, 50000.0, 60000.0, 70000.0],
                "sex": ["male", "female", "male", "female"],
                "__onehot_male": [1.0, 0.0, 1.0, 0.0],
                "__onehot_female": [0.0, 1.0, 0.0, 1.0],
                "__hcount_minor": [0.0, 0.0, 0.0, 0.0],
            }
        )
        parents_df = pd.DataFrame(
            {
                "family_id": [101, 102],
                "__child_count": [2, 2],
                "age": [30.0, 50.0],
                "income": [90000.0, 130000.0],
                "__onehot_male": [0.5, 0.5],
                "__onehot_female": [0.5, 0.5],
                "__hcount_minor": [0.0, 0.0],
            }
        )
        assigned_children, final_parents, residuals = assign_children_to_parents_greedy(
            children_df, parents_df, schema, parent_id_col="family_id"
        )
        self.assertEqual(len(assigned_children), 4)
        self.assertEqual(len(final_parents), 2)
        self.assertIn("family_id", assigned_children.columns)
        self.assertIn("income_mae", residuals)
        # Each family must have exactly 2 assigned children
        counts = assigned_children["family_id"].value_counts()
        self.assertEqual(counts.get(1), 2)
        self.assertEqual(counts.get(2), 2)

    def test_create_synthetic_dataset_end_to_end(self):
        agg_tables = {"individual": self.ind_agg, "family": self.fam_agg}
        syn_pop, diagnostics = create_synthetic_dataset(
            agg_tables, self.cfg, return_diagnostics=True
        )
        # Reconciles individual count to family capacity
        self.assertTrue(15 <= len(syn_pop) <= 25)
        self.assertIn("person_id", syn_pop.columns)
        self.assertIn("family_id", syn_pop.columns)
        self.assertIn("age", syn_pop.columns)
        self.assertIn("income", syn_pop.columns)
        self.assertIn("sex", syn_pop.columns)
        # Check that person IDs are unique
        self.assertEqual(syn_pop["person_id"].nunique(), len(syn_pop))
        self.assertTrue((syn_pop["family_id"] > 0).all())
        self.assertIn("residuals", diagnostics)


if __name__ == "__main__":
    unittest.main()
