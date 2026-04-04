from pandas import DataFrame
from numpy import nan
from process.model.stochastic_impute import stochastic_impute
from process.postp.vis import plot_distribution

# ---------------------------------
# 1. Define base aggregated population data (e.g., from a census)
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
