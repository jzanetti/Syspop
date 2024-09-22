import pandas as pd
from ipf import ipf_adjustment, pop_pivot, postproc

# ---------------------------------------
# Test data
#   - population data: number of people for different sex/age/ethnicity
#   - household data: number of household with different household composition
#   - work data: number of people with different occupations/incomes
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
    {"adult": [1, 2, 5], "child": [0, 1, 2], "count": [30, 10, 20]}
)

work_data = pd.DataFrame(
    {
        "occupation": [
            "teacher",
            "teacher",
            "doctor",
            "doctor",
            "others",
            "others",
        ],
        "ethnicity": [
            "european",
            "asian",
            "maori",
            "pacific",
            "european",
            "pacific",
        ],
        "income": [100, 120, 80, 70, 50, 10],
        "count": [30, 50, 100, 30, 50, 15],
    }
)

# <><><><><><><><><><><><><><><><><><><><><><><><>
# Section 1: Fitting household data
# <><><><><><><><><><><><><><><><><><><><><><><><>

# ---------------------------------------
# 1.1: create Pivot table for the population
# ---------------------------------------
population_data_pivot = pop_pivot(
    population_data, pivot_cfg={"index": ["sex", "ethnicity"], "columns": ["age"]}
)

# ---------------------------------------
# 1.2: apply IPF adjustment
# ---------------------------------------
population_data_update = ipf_adjustment(
    population_data_pivot,
    constrains={
        "columns": [
            sum(household_data["adult"] * household_data["count"]),
            sum(household_data["child"] * household_data["count"]),
        ],
        "index": None,
    },
)

# ---------------------------------------
# 1.3: Postprocessing
# ---------------------------------------
population_data = postproc(
    population_data, population_data_update, colnames=["sex", "ethnicity", "age"]
)

# <><><><><><><><><><><><><><><><><><><><><><><><>
# Section 2: Fitting work data
# <><><><><><><><><><><><><><><><><><><><><><><><>

# ---------------------------------------
# 2.1: create Pivot table for the population based on ethnicity
# ---------------------------------------
population_data_pivot = pop_pivot(
    population_data,
    pivot_cfg={"index": ["sex", "age"], "columns": ["ethnicity"]},
)

# ---------------------------------------
# 2.2: apply IPF adjustment
# ---------------------------------------
constrains_eth = []
for proc_eth in population_data_pivot.columns:
    constrains_eth.append(work_data[work_data["ethnicity"] == proc_eth]["count"].sum())

population_data_update = ipf_adjustment(
    population_data_pivot,
    constrains={
        "columns": constrains_eth,
        "index": None,
    },
)

# ---------------------------------------
# 1.3: Postprocessing
# ---------------------------------------
population_data = postproc(
    population_data, population_data_update, colnames=["ethnicity"]
)

x = 3
