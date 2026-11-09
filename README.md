# Syspop: Synthetic Population & Confidential Microdata Engine

**Contact**: _Sijin Zhang (zsjzyhzp@gmail.com)_

A lightweight Python package (built on `numpy`, `pandas`, `pyyaml`, and `matplotlib`) providing two complementary workflows for synthetic unit-record population generation:

1. **Nearest-Neighbour Confidential Aggregation & Hierarchical Synthesis (`syspop.confidential`)**:
   Produces unit-record synthetic data from confidential microdata (e.g., IDI data from Stats NZ) by first aggregating unit records into homogeneous clusters via a _weighted nearest-neighbour algorithm_ under configurable confidentiality rules, and then recreating linked structured synthetic unit records (e.g., _Individual → Family → Household_) outside the secure environment.
2. **Stochastic Population Imputation (`syspop.model.stochastic_impute`)**:
   Statistically links multiple publicly published aggregated tables (e.g., from Stats Data Explorer) into a unified unit-record dataset using conditional probability distributions.

---

## Table of Contents
- [Syspop: Synthetic Population \& Confidential Microdata Engine](#syspop-synthetic-population--confidential-microdata-engine)
  - [Table of Contents](#table-of-contents)
  - [1. Installation](#1-installation)
  - [2. Method 1: Nearest-Neighbour Confidential Aggregation \& Hierarchical Synthesis](#2-method-1-nearest-neighbour-confidential-aggregation--hierarchical-synthesis)
    - [Configuration](#configuration)
    - [Quickstart Example](#quickstart-example)
  - [3. Method 2: Stochastic Imputation from Aggregated Tables](#3-method-2-stochastic-imputation-from-aggregated-tables)
- [For Develpers](#for-develpers)

---

<a name="installation"></a>
## 1. Installation

```bash
pip install syspop_v2
```

---

<a name="method1"></a>
## 2. Method 1: Nearest-Neighbour Confidential Aggregation & Hierarchical Synthesis
<a name="configuration"></a>
### Configuration

Example configuration file can be found `etc/sample_data/sample_confidential_cfg.yml`

All confidentiality thresholds and data structures are controlled via YAML configuration (or Python `dict`), with automatic feature and hierarchy discovery when omitted:

```yaml
output_rules:
  min_group_count: 20                  # Minimum units per cluster (for means & stds)
  min_cell_count: 6                    # Minimum units per count / categorical cell
  rounding_base: 3                     # Random rounding base (e.g. 3 for RR3; set to null / 0 / false to switch off)
  subcell_action: "reassign_or_merge"  # "reassign_or_merge" (default), "suppress_cluster", "suppress_cell_nan", "rr3_and_suppress", "none"
  leftover_action: "assign_nearest"    # "assign_nearest" or "suppress"

data_structure:
  hierarchy:
    - individual
    - family
    - household
  id_columns:
    individual: "person_id"
    family: "family_id"
    household: "household_id"
  hierarchy_counts:                    # Optional sub-group member counts when rolling up to family/household
    individual_under_18:
      condition: "age < 18"
      weight: 2.0
  features:                            # Omit or set partial weights; unspecified columns are auto-detected
    age:
      type: "continuous"
      weight: 3.0
      agg: "mean"                      # Roll-up rule to family/household: "mean" (default), "sum", "min", "max", "median"
    income:
      type: "continuous"
      weight: 1.0
      agg: "sum"                       # e.g., family/household income = sum of member incomes
    sex:
      type: "categorical"
      weight: 1.0

synthesis:
  reconcile_counts: "auto"
  random_seed: 42
```

---

<a name="method1-example"></a>
### Quickstart Example

```python
from syspop.confidential import (
    run_datalab_aggregation,
    run_outside_synthesis,
    run_confidential_synthetic_pipeline,
)

# Option A: Run end-to-end (Steps 1, 2, and 3)
# syn_df, agg_tables, diagnostics = run_confidential_synthetic_pipeline(
#    df=raw_microdata_df,
#    config_or_path="etc/sample_data/sample_confidential_cfg.yml",
#    output_dir="./output",
# )

# Option B: Run Step 1 & 2 inside the secure environment, and Step 3 outside
# (Optionally pass `weight_df` in parallel to `df` if using survey weights, e.g. household_weight / family_weight / person_weight)
agg_tables, metadata = run_datalab_aggregation(
    df=raw_microdata_df,
    config_or_path="etc/sample_data/sample_confidential_cfg.yml",
    output_dir="./output/exported_tables",
    weight_df=None,  # Optional DataFrame with e.g. ["household_id", "household_weight"] or ["person_id", "person_weight"]
)

syn_df, diagnostics = run_outside_synthesis(
    agg_tables_or_dir="./output/exported_tables",
    config_or_path="etc/sample_data/sample_confidential_cfg.yml",
    output_dir="./output",
)
```

See:
- [`examples/Nearest_Neighbour_Synthesis/example1.py`](examples/Nearest_Neighbour_Synthesis/example1.py)
- [`examples/Nearest_Neighbour_Synthesis/example2.py`](examples/Nearest_Neighbour_Synthesis/example2.py) & [`etc/sample_data/complex_confidential_cfg.yml`](etc/sample_data/complex_confidential_cfg.yml): Multi-feature example (`age`, `sex`, `income`, `saving`, `occupation`, `location`) with vertically structured audit reports and raw-to-synthetic distribution comparisons.

---

<a name="method2"></a>
## 3. Method 2: Stochastic Imputation from Aggregated Tables

See [`examples/Stochastic_Imputation/simple_example.py`](examples/Stochastic_Imputation/simple_example.py) and [`examples/Stochastic_Imputation/example_nz_census.py`](examples/Stochastic_Imputation/example_nz_census.py):

```python
from pandas import DataFrame
from numpy import nan
from syspop.model.stochastic_impute import stochastic_impute
from syspop.postp.vis import plot_distribution

base_population_data = DataFrame(
    {
        "gender": [1, 2, 1],
        "age": [25, 30, 40],
        "value": [50, 60, 70],
    }
)

income_data = DataFrame(
    {
        "gender": [1, 1, 2, 2],
        "age": [25, 30, 25, nan],
        "work_status": [1, 2, 1, 2],
        "income": [50000, 60000, 55000, 45000],
        "value": [8, 2, 6, 4],
    }
)

data = {"seed": base_population_data, "income": income_data}
task_list = {
    "income": {
        "targets": {"work_status": "category", "income": "numeric"},
        "features": ["age", "gender"],
    }
}

syn_pop = stochastic_impute(data, task_list, maintain_counts=["income"])
plot_distribution(syn_pop, ["age"])
plot_distribution(syn_pop, ["gender", "age"])
```

# For Develpers
```
# 1. Make sure build and twine are installed
pip install --upgrade build twine
# 2. Clean old build artifacts
rm -rf dist/ build/ *.egg-info
# 3. Build the new wheel (.whl) and source distribution (.tar.gz) for v0.5.0
python -m build
# 4. Upload to PyPI (will prompt for your PyPI API token)
python -m twine upload dist/*
```