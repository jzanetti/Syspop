# Syspop: Synthetic Population & Confidential Microdata Engine

A lightweight Python package (built on `numpy`, `pandas`, `pyyaml`, and `matplotlib`) providing two complementary workflows for synthetic unit-record population generation:

1. **Nearest-Neighbour Confidential Aggregation & Hierarchical Synthesis (`syspop.confidential`)**:
   Produces unit-record synthetic data from confidential microdata by first aggregating unit records into homogeneous clusters via a **weighted nearest-neighbour algorithm** under configurable confidentiality rules, and then recreating linked **Individual → Family → Household** synthetic unit records outside the secure environment.
2. **Stochastic Population Imputation (`syspop.model.stochastic_impute`)**:
   Statistically links multiple publicly published aggregated tables into a unified unit-record dataset using conditional probability distributions.

---

## Table of Contents
- [Syspop: Synthetic Population \& Confidential Microdata Engine](#syspop-synthetic-population--confidential-microdata-engine)
  - [Table of Contents](#table-of-contents)
  - [1. Installation](#1-installation)
  - [2. Method 1: Nearest-Neighbour Confidential Aggregation \& Hierarchical Synthesis](#2-method-1-nearest-neighbour-confidential-aggregation--hierarchical-synthesis)
    - [Configuration (`etc/sample_data/sample_confidential_cfg.yml`)](#configuration-etcsample_datasample_confidential_cfgyml)
    - [Quickstart Example](#quickstart-example)
  - [3. Method 2: Stochastic Imputation from Aggregated Tables](#3-method-2-stochastic-imputation-from-aggregated-tables)

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
### Configuration (`etc/sample_data/sample_confidential_cfg.yml`)

All confidentiality thresholds and data structures are controlled via YAML configuration (or Python `dict`), with automatic feature and hierarchy discovery when omitted:

```yaml
output_rules:
  preset: "idi"                        # "idi", "survey", or "custom"
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
  features:                            # Omit or set partial weights; unspecified columns are auto-detected
    age:
      type: "continuous"
      weight: 3.0
    income:
      type: "continuous"
      weight: 1.0
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
agg_tables, metadata = run_datalab_aggregation(
    df=raw_microdata_df,
    config_or_path="etc/sample_data/sample_confidential_cfg.yml",
    output_dir="./output/exported_tables",
)

syn_df, diagnostics = run_outside_synthesis(
    agg_tables_or_dir="./output/exported_tables",
    config_or_path="etc/sample_data/sample_confidential_cfg.yml",
    output_dir="./output",
)
```

See:
- [`examples/Nearest_Neighbour_Synthesis/example1.py`](examples/Nearest_Neighbour_Synthesis/example1.py): Step-by-step walkthrough of [`etc/Synthetic_Data_Production_Algorithm.docx`](etc/Synthetic_Data_Production_Algorithm.docx).
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
