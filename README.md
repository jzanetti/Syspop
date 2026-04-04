# Stochastic Impute: Synthetic Data Generation Engine

A Python utility designed to generate an **integreated synthetic unit record population data** using aggregated public data sources (e.g., from [Stats NZ Data Explorer](https://explore.data.stats.govt.nz)).

This tool is optimized for large-scale microdata generation where individual attributes are assigned based on conditional probability distributions.

---

## 🚀 Key Features

* **Synthetic Unit Record Generation**: Transforms multiple aggregated data sources into a unified, granular unit-record dataset.
* **Dynamic Column Matching**: Automatically identifies shared features between the base population and reference data.
* **Missingness-Aware Logic**: Handles rows with `NaN` values by dynamically re-calculating probabilities based only on the available non-null features.
* **Stochastic Selection**: Uses weighted random sampling to preserve the natural variance and distribution of the source data.
* **Dual-Source Integration**: If a target attribute already exists, the engine can blend the existing data with the synthetic prediction using row-wise averaging.
* **Optimized Performance**: Processes millions rows in seconds by grouping identical "missingness patterns" rather than iterating row-by-row.

> Please see the [FAQ](#-faq) for more details.


---

## 📋 Data Requirements

### 1. Population Seed
The starting point for your synthetic data. 
* Can contain existing columns you wish to "refine."
* Can contain `NaN` values; the engine will ignore these specific columns during matching for those specific rows.

### 2. Reference Distributions
Each entry in the dictionary must be a DataFrame containing:
* **Shared Features**: (e.g., `age`, `location`) to match against the seed.
* **Target Column**: The attribute being generated.

---

## 💻 Example Usage

```python
from pandas import DataFrame
from numpy import nan
from process.model.stochastic_impute import stochastic_impute
from process.postp.vis import plot_distribution

# ---------------------------------
# 1. Define base aggregated population data (e.g., from a census)
#    For example, a total of 50 + 60 + 70 people in this example
# ---------------------------------
base_population_data = DataFrame(
    {
        "gender": [1, 2, 1],
        "age": [25, 30, 40],
        "value": [50, 60, 70],
    }
)

# ---------------------------------
# 2. Define reference aggregated data (e.g., Work Status distribution)
#    For example, a total of 8 + 2 + 6 + 4 people in this reference data
# ---------------------------------
income_data = DataFrame(
    {
        "gender": [1, 1, 2, 2],
        "age": [25, 30, 25, nan],
        "work_status": [1, 2, 1, 2],
        "income": [50000, 60000, 55000, 45000],
        "value": [8, 2, 6, 4],
    }
)

# ---------------------------------
# 3. Combine data into a dictionary for the imputation process
# ---------------------------------
data = {"seed": base_population_data, "income": income_data}

# ---------------------------------
# 4. Define the imputation tasks:
#   - For the "income" task, we want to impute both "work_status" and "income" based on "age" and "gender
# ---------------------------------
task_list = {
    "income": {
        "targets": {"work_status": "category", "income": "numeric"},
        "features": ["age", "gender"],
    }
}

# ---------------------------------
# 5. Run the stochastic imputation process
# ---------------------------------
syn_pop = stochastic_impute(data, task_list)

# ---------------------------------
# 6. Plot distribution
# ---------------------------------
# 6.1 Plot distribution for age
plot_distribution(syn_pop, ["age"])
# 6.2 Plot joint distribution for gender + age
plot_distribution(syn_pop, ["gender", "age"])
```

<a name="faq"></a>
## 🧠 FAQ
### Is generating synthetic unit-record data in this way actually accurate?
Well, it depends on your use case and the quality of your inputs. Ideally, you should work with real unit-record data. However, in practice, this isn't always feasible (i.e., in New Zealand, if you live far away from a Stats NZ IDI data lab). This utility provides a method to statistically link different aggregated, published population benchmarks together. This allows you to build out full data pipelines and prototype products locally before taking your code into a restricted environment.

### Maximizing accuracy?
The accuracy of the synthetic output depends heavily on task design. The more shared variables (covariates) present in your reference data to condition the probabilities, the closer the synthetic distribution will mirror reality.

Also, you can run the process multiple times to capture inherent uncertainties.

### Are we doing any prediction modelling here ?
It is out of scope at the moment. If certain covariate values are missing from the reference data to condition the probabilities, the process simply ignores those missing values when linking the reference data to the seed data (for example, if the reference data does not contain income information for children, when integrating the reference data into the seed population data, the integrated data will just set the income for children as NaN). The reason is that many covariates in these datasets are categorical, and applying simple prediction models can struggle to capture the nuances and introduce unwanted noise or uncertainty into the output data. However, you are welcome to apply your own predictive models to handle missing data prior to running this process if your use case requires it.