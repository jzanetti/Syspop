# Syspop: Synthetic Data & Confidential Microdata Engine

**Contact**: _Sijin Zhang (zsjzyhzp@gmail.com)_

A Python package providing two complementary workflows for synthetic unit-record data generation:

## Why:

Secure data environments restrict the release of unit-record data because disclosure checkers must be able to verify that no individual or entity can be identified. In practice, this limits standard micro-to-micro synthetic data generators (such as `CTGAN` or `Copulas`): their black-box neural weights and continuous joint distributions lack deterministic privacy guarantees and cannot be easily audited against strict cell-suppression and rounding rules.

To bridge this gap, **SysPop** implements two practical pathways that let researchers work with realistic unit-record data while strictly honoring statistical disclosure rules:


1. **Confidential Aggregation & Hierarchical Synthesis (`syspop.confidential`)**:
   Produces unit-record synthetic data from confidential microdata (e.g., `IDI data from Stats NZ`) by first aggregating unit records into homogeneous clusters to meet the minimum confidentiality rules, and then recreating linked hierarchical synthetic unit records (e.g., _Individual → Family → Household_) outside the secure environment.
2. **Stochastic Data Imputation (`syspop.model.stochastic_impute`)**:
   Statistically links multiple publicly published aggregated tables (e.g., `Stats Data Explorer`) into a unified unit-record dataset using conditional probability distributions.

---

## Table of Contents
- [Syspop: Synthetic Data \& Confidential Microdata Engine](#syspop-synthetic-data--confidential-microdata-engine)
  - [Why:](#why)
  - [Table of Contents](#table-of-contents)
  - [1. Installation](#1-installation)
  - [2. Method 1: Confidential Aggregation \& Hierarchical Synthesis](#2-method-1-confidential-aggregation--hierarchical-synthesis)
    - [Configuration](#configuration)
    - [Quickstart Example](#quickstart-example)
  - [3. Method 2: Stochastic Data Imputation from Aggregated Tables](#3-method-2-stochastic-data-imputation-from-aggregated-tables)
- [For Develpers](#for-develpers)

---

<a name="installation"></a>
## 1. Installation

```bash
pip install syspop_v2
```

---

<a name="method1"></a>
## 2. Method 1: Confidential Aggregation & Hierarchical Synthesis
<a name="configuration"></a>
### Configuration

Example configuration file can be found at `etc/examples/Confidential_Aggregation_and_Hierarchical_Synthesis/example.yml`.
All stage sections (`agg`, `verif`, `synthesis`) share `common`, and any unused stage can be set to `null` (e.g., `verif: null` / `synthesis: null` when running aggregation only).

```yaml
common:
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
  correlations:                        # Optional cross-feature correlations added before clustering
    - features: ["age", "income"]
      weight: 1.5
    - features: ["age", "sex"]
      weight: 1.0

agg:                                   # Set to `null` if running only synthesis/verification
  rounding_base: 3                     # Random rounding base for counts (3 for RR3, or null / 0 / false to switch off)
  leftover_action: "assign_nearest"    # Leftover records after clustering: "assign_nearest" or "suppress"
  continuous:
    min_count: 20                      # Minimum units per cluster (for continuous means, stds & cluster count)
  categorical:
    min_count: 6                       # Minimum unweighted units per category cell inside a cluster

verif:                                 # Set to `null` if running only aggregation/synthesis
  min_count: 20                        # Minimum raw units per bin/cell/category in verification plots & reports
  columns: ["age", "income", "sex"]

synthesis:                             # Set to `null` if running only aggregation/verification
  reconcile_counts: "auto"
  random_seed: 42
```

---

<a name="method1-example"></a>
### Quickstart Example

See details: [`etc/examples/Confidential_Aggregation_and_Hierarchical_Synthesis/example.py`](etc/examples/Confidential_Aggregation_and_Hierarchical_Synthesis/example.py)

```python
from syspop.confidential import (
    run_aggregation,
    run_synthesis,
    run_verif,
)

# Step 1 & 2: Run aggregation (e.g., inside the secure environment)
# (Optionally pass `weight_df` in parallel to `df` if using survey weights)
agg_tables, metadata = run_aggregation(
    df=raw_microdata_df,
    config_or_path="etc/examples/Confidential_Aggregation_and_Hierarchical_Synthesis/example.yml",
    output_dir="./output/exported_tables",
    weight_df=None,
)

# Step 3: Recreate synthetic unit records outside the secure environment
syn_df, diagnostics = run_synthesis(
    agg_tables_or_dir="./output/exported_tables",
    config_or_path="etc/examples/Confidential_Aggregation_and_Hierarchical_Synthesis/example.yml",
    output_dir="./output",
)

# Step 4: Run hierarchical verification plots and reports (inside the secure environment)
run_verif(
    raw_df=raw_microdata_df,
    syn_df=syn_df,
    output_dir="./output",
    config_or_path="etc/examples/Confidential_Aggregation_and_Hierarchical_Synthesis/example.yml",
    diagnostics=diagnostics,
)
```

---

<a name="method2"></a>
## 3. Method 2: Stochastic Data Imputation from Aggregated Tables

See details:
- [`etc/examples/Stochastic_Imputation/simple_example.py`](etc/examples/Stochastic_Imputation/simple_example.py)
- [`etc/examples/Stochastic_Imputation/example_nz_census.py`](etc/examples/Stochastic_Imputation/example_nz_census.py)

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
# 1. Test the package
python -m unittest discover -s test -p "test_*.py" -v
# 2. Make sure build and twine are installed
pip install --upgrade build twine
# 3. Clean old build artifacts
rm -rf dist/ build/ *.egg-info
# 4. Build the new wheel (.whl) and source distribution (.tar.gz) for v0.5.0
python -m build
# 5. Upload to PyPI (will prompt for your PyPI API token)
python -m twine upload dist/*
```