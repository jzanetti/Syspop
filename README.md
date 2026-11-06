# Syspop: Synthetic Population & Confidential Microdata Engine

A lightweight Python package (built on pure `numpy`, `pandas`, `pyyaml`, and `matplotlib`) providing two complementary workflows for synthetic unit-record population generation:

1. **Confidential Unit-Record Aggregation & Hierarchical Synthesis (`syspop.confidential`)**:
   Produces unit-record synthetic data from confidential microdata (e.g., inside the Stats NZ Data Lab / IDI or surveys) by first aggregating unit records via a **weighted nearest-neighbour algorithm** that strictly enforces [Stats NZ Microdata Output Guide (2020 v5, Sept 2022 update)](https://www.stats.govt.nz/assets/Methods/Microdata-Output-Guide-2020-v5-Sept22update.pdf) confidentiality rules, and then recreating linked **Individual → Family → Household** synthetic unit records outside the secure environment.
2. **Stochastic Population Imputation (`syspop.model.stochastic_impute`)**:
   Statistically links multiple publicly published aggregated tables (e.g., from [Stats NZ Data Explorer](https://explore.data.stats.govt.nz)) into a unified unit-record dataset using conditional probability distributions.

---

## Table of Contents
1. [Installation](#installation)
2. [Method 1: Nearest-Neighbour Confidential Aggregation & Hierarchical Synthesis](#method1)
   - [Algorithm Overview (Steps 1–3)](#algorithm-overview)
   - [Configuration File (`output_rules` & `data_structure`)](#configuration)
   - [Quickstart Example](#method1-example)
3. [Method 2: Stochastic Imputation from Public Aggregated Tables](#method2)

---

<a name="installation"></a>
## 1. Installation

```bash
pip install syspop_v2
```

---

<a name="method1"></a>
## 2. Method 1: Nearest-Neighbour Confidential Aggregation & Hierarchical Synthesis

<a name="algorithm-overview"></a>
### Algorithm Overview

Detailed in [`etc/Synthetic_Data_Production_Algorithm.docx`](etc/Synthetic_Data_Production_Algorithm.docx):

* **Step 1: Aggregate Unit Records (Inside Data Lab)**
  * **One-Hot Encoding & Max Normalization**: Categorical features (e.g., `sex`) are converted into `0`/`1` columns, and continuous features (e.g., `age`, `income`) are normalized by their maximum value inside the Data Lab.
  * **Weighted Nearest-Neighbour Distance**: Pairwise distance between records $i$ and $j$ is computed as:
    $$d(i, j) = \sum_{f \in \text{continuous}} w_f \left(x'_{i,f} - x'_{j,f}\right)^2 + \sum_{c \in \text{categorical}} w_c \cdot \frac{1}{2} \sum_{m=1}^{K_c} \left(x'_{i,c_m} - x'_{j,c_m}\right)^2$$
    where the factor $\frac{1}{2}$ prevents categorical variables from being double-counted across their one-hot columns, and $w_f, w_c$ are configurable feature importance weights.
  * **Confidential Clustering**: Records are grouped into tight nearest-neighbour clusters meeting the minimum unweighted observation count required by the active Stats NZ output rules (e.g., $\ge 20$ for IDI means, $\ge 5$ for survey means, $\ge 6$ for cell counts, or higher if percentiles are exported). Leftover records are either assigned to the nearest cluster (`assign_nearest`) or suppressed (`suppress`).
  * **Resuming Original Scale & Output Rules**: Before export, continuous features are summarised on their un-normalized scale (`mean` and `std`, plus optional allowed percentiles under Rule 4.4) without exporting sample minimums or maximums (Rule 4.6). Categorical features are exported as confidentialised group proportions (protecting small sub-cells under Rule 4.5), and group counts are rounded using Random Rounding to base 3 (`RR3`, Rule 4.1).

* **Step 2: Data Hierarchy (`Individual -> Family -> Household`)**
  * Unit records are first aggregated at the `individual` level.
  * For `family` and `household` levels, individual records are first merged by `family_id` / `household_id` (computing unit-level feature averages and member counts `individual_num` / `family_num`) and then clustered using Step 1 so every exported table satisfies the confidentiality rules.

* **Step 3: Recreate Synthetic Dataset (Outside Data Lab)**
  * Synthetic individuals, families, and households are generated from the released aggregated tables (using normal distributions $\mathcal{N}(\mu, \sigma^2)$ or exported percentiles for continuous features, and multinomial sampling for categorical proportions).
  * Synthetic individuals are assigned to families, and families to households, via greedy nearest-neighbour matching in weighted normalized feature space to minimize family- and household-level residual errors.

---

<a name="configuration"></a>
### Configuration (`etc/sample_data/sample_confidential_cfg.yml`)

All Stats NZ output rules and data structures are controlled via YAML configuration (or Python `dict`), with automatic feature/hierarchy discovery when omitted:

```yaml
output_rules:
  preset: "idi"                # "idi" (min_count_mean=20, min_cell_count=6, rr3), "survey" (min_count_mean=5), or "custom"
  min_cell_count: 6            # Rule 4.1 / 4.2: Minimum unweighted cell count
  min_count_mean: 20           # Rule 4.3: Minimum unweighted count for means (20 for IDI, 5 for survey)
  min_count_std: 20            # Rule 4.3: Minimum unweighted count for standard deviation
  rounding_method: "rr3"       # Rule 4.1: "rr3" (Random Rounding to base 3), "deterministic", or "none"
  rounding_base: 3
  suppress_min_max: true       # Rule 4.6: Never export sample minimums or maximums
  use_percentiles: false       # Rule 4.4: Export percentiles (enforces 10th/90th: 50, 25th/75th: 20, 50th: 10)
  percentiles: [25, 50, 75]
  subcell_action: "merge"      # Rule 4.5: Small categorical sub-cell protection ("merge", "rr3_and_suppress", "none")
  min_subcell_count: 6
  leftover_action: "assign_nearest"  # "assign_nearest" or "suppress"

data_structure:
  hierarchy: ["individual", "family", "household"]
  id_columns:
    individual: "person_id"
    family: "family_id"
    household: "household_id"
  features:                    # Omit or set partial weights; unspecified columns are auto-detected!
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
syn_df, agg_tables, diagnostics = run_confidential_synthetic_pipeline(
    df=raw_microdata_df,
    config_or_path="etc/sample_data/sample_confidential_cfg.yml",
    output_dir="./output",
)

# Option B: Run Step 1 & 2 inside the Data Lab, and Step 3 outside the Data Lab
# When `output_dir` is provided, `run_datalab_aggregation` automatically exports:
#   - `{level}_aggregated.csv`: Confidentialised table for release
#   - `{level}_raw_with_cluster.csv`: Raw unit records mapped to `cluster_id` (derivation proof)
#   - `{level}_checker_audit_report.csv`: Cluster-by-cluster rule compliance audit
#   - `idi_rules_compliance_summary.csv`: Executive checklist of Stats NZ Output Guide rules
#   - `idi_output_checker_report.xlsx`: Multi-sheet Excel workbook for IDI Output Checkers
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
- [`examples/confidential_example.py`](examples/confidential_example.py): Step-by-step verification of [`etc/Synthetic_Data_Production_Algorithm.docx`](etc/Synthetic_Data_Production_Algorithm.docx).
- [`examples/complex_confidential_example.py`](examples/complex_confidential_example.py) & [`etc/sample_data/complex_confidential_cfg.yml`](etc/sample_data/complex_confidential_cfg.yml): Multi-feature example (`age`, `sex`, `income`, `saving`, `occupation`, `location`) with IDI Output Checker audit reports and raw-to-synthetic distribution comparisons.

---

<a name="method2"></a>
## 3. Method 2: Stochastic Imputation from Public Aggregated Tables

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
