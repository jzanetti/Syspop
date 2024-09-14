import pandas as pd
from ipf import ipf_adjustment, pop_pivot, postproc

# ---------------------------------------
# Test data
#   - population data: number of people for different sex/age/ethnicity
#   - household data: number of household with different household composition
# ---------------------------------------
population_data = pd.DataFrame(
    {
        "sex": ["male", "male", "female", "female", "female"],
        "age": ["child", "adult", "child", "adult", "adult"],
        "ethnicity": ["european", "european", "maori", "pacific", "asian"],
        "count": [50, 100, 60, 90, 20],
    }
)

household_data = pd.DataFrame(
    {"adult": [1, 2, 5], "child": [0, 1, 2], "count": [30, 30, 20]}
)

# ---------------------------------------
# Step 1: create Pivot table for the population
# ---------------------------------------
population_data_pivot = pop_pivot(
    population_data, pivot_cfg={"index": ["sex", "ethnicity"], "columns": ["age"]}
)

# ---------------------------------------
# Step 2: apply IPF adjustment
# ---------------------------------------
population_data_update = ipf_adjustment(
    population_data_pivot,
    constrains={
        "col": [
            sum(household_data["adult"] * household_data["count"]),
            sum(household_data["child"] * household_data["count"]),
        ],
        "index": None,
    },
)

# ---------------------------------------
# Step 3: Postprocessing
# ---------------------------------------
population_data_update = postproc(
    population_data, population_data_update, colnames=["sex", "ethnicity", "age"]
)

print(population_data_update)
