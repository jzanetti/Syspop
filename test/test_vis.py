import os
import tempfile
import unittest
import numpy as np
import pandas as pd

from syspop.postp.vis import (
    _is_categorical,
    plot_distribution,
    plot_comparison,
    plot_cross_comparison,
)


class TestVis(unittest.TestCase):
    def setUp(self):
        np.random.seed(42)
        n = 100
        self.raw_df = pd.DataFrame(
            {
                "age": np.random.uniform(18, 70, size=n),
                "income": np.random.uniform(20000, 120000, size=n),
                "sex": np.random.choice(["male", "female"], size=n),
                "education": np.random.choice(["high", "medium", "low"], size=n),
            }
        )
        self.syn_df = pd.DataFrame(
            {
                "age": np.random.uniform(18, 70, size=n),
                "income": np.random.uniform(20000, 120000, size=n),
                "sex": np.random.choice(["male", "female"], size=n),
                "education": np.random.choice(["high", "medium", "low"], size=n),
            }
        )

    def test_is_categorical(self):
        # Object dtype
        s_obj = pd.Series(["apple", "banana", "cherry"])
        self.assertTrue(_is_categorical(s_obj))

        # Category dtype
        s_cat = pd.Series(pd.Categorical(["a", "b", "c"]))
        self.assertTrue(_is_categorical(s_cat))

        # Bool dtype
        s_bool = pd.Series([True, False, True])
        self.assertTrue(_is_categorical(s_bool))

        # Numeric with small unique count <= max_unique
        s_few_nums = pd.Series([1, 2, 3, 1, 2, 3])
        self.assertTrue(_is_categorical(s_few_nums, max_unique=5))

        # Numeric with large unique count > max_unique
        s_many_nums = pd.Series(np.arange(100))
        self.assertFalse(_is_categorical(s_many_nums, max_unique=15))

    def test_plot_distribution_single_column(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            # 1. Categorical single column
            p1 = plot_distribution(self.raw_df, ["sex"], output_dir=tmp_dir)
            self.assertTrue(os.path.exists(p1))
            self.assertGreater(os.path.getsize(p1), 0)

            # 2. Continuous single column
            p2 = plot_distribution(self.raw_df, "age", output_dir=tmp_dir)
            self.assertTrue(os.path.exists(p2))
            self.assertGreater(os.path.getsize(p2), 0)

    def test_plot_distribution_two_columns(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Cat x Cat
            p_cc = plot_distribution(self.raw_df, ["sex", "education"], output_dir=tmp_dir)
            self.assertTrue(os.path.exists(p_cc))
            self.assertGreater(os.path.getsize(p_cc), 0)

            # Cat x Cont
            p_co = plot_distribution(self.raw_df, ["sex", "income"], output_dir=tmp_dir)
            self.assertTrue(os.path.exists(p_co))
            self.assertGreater(os.path.getsize(p_co), 0)

            # Cont x Cont
            p_oo = plot_distribution(self.raw_df, ["age", "income"], output_dir=tmp_dir)
            self.assertTrue(os.path.exists(p_oo))
            self.assertGreater(os.path.getsize(p_oo), 0)

    def test_plot_comparison(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            out_file = plot_comparison(
                raw_df=self.raw_df,
                syn_df=self.syn_df,
                columns=["age", "income", "sex", "education"],
                output_dir=tmp_dir,
                filename="test_comparison.png",
            )
            self.assertTrue(os.path.exists(out_file))
            self.assertGreater(os.path.getsize(out_file), 0)

    def test_plot_cross_comparison(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            # 1. All combinations
            cols = ["sex", "age", "education"]
            paths = plot_cross_comparison(
                raw_df=self.raw_df,
                syn_df=self.syn_df,
                columns=cols,
                output_dir=tmp_dir,
            )
            # 3 pairs: (sex, age), (sex, education), (age, education)
            self.assertEqual(len(paths), 3)
            for p in paths:
                self.assertTrue(os.path.exists(p))
                self.assertGreater(os.path.getsize(p), 0)

            # 2. Focus column
            focus_paths = plot_cross_comparison(
                raw_df=self.raw_df,
                syn_df=self.syn_df,
                columns=cols,
                output_dir=tmp_dir,
                focus_column="sex",
            )
            # 2 pairs: (sex, age), (sex, education)
            self.assertEqual(len(focus_paths), 2)
            for p in focus_paths:
                self.assertTrue(os.path.exists(p))


if __name__ == "__main__":
    unittest.main()
