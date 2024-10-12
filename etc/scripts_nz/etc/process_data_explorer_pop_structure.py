# -------------------------------------------------
# Data Explorer is the new way to get aggregated data from Stats NZ,
# however the data downloaded directly from there usually come with 
# many unrelated information
#
# This script is used to clean up the downloaded data from Data Explorer
# https://explore.data.stats.govt.nz
# -------------------------------------------------

from pandas import read_csv

# ---------------------
# Population structure
# ---------------------
population_structure_data_path = "/Users/sijinzhang/Downloads/STATSNZ,CEN23_POP_007,1.0+all.csv"

population_structure_data = read_csv(population_structure_data_path)

population_structure_data = population_structure_data[
    ["CEN23_YEAR_001: Census year", 
     "CEN23_CNA_002: Census night address", 
     "CEN23_ETH_002: Ethnicity",
     "CEN23_AGE_001: Age",
     "CEN23_GEN_002: Gender",
     "OBS_VALUE"]]

population_structure_data = population_structure_data.rename(
    columns={
        "CEN23_YEAR_001: Census year": "year",
        "CEN23_CNA_002: Census night address": "sa2",
        "CEN23_ETH_002: Ethnicity": "ethnicity",
        "CEN23_AGE_001: Age": "age",
        "CEN23_GEN_002: Gender": "gender",
        "OBS_VALUE": "value"
    }
)

population_structure_data = population_structure_data[
    population_structure_data["year"] == "2023: 2023"]

population_structure_data = population_structure_data[
    population_structure_data["sa2"].str.split(':').str[0].str.len() == 6]
population_structure_data["sa2"] = population_structure_data["sa2"].str.split(':').str[0]
population_structure_data["sa2"] = population_structure_data["sa2"].astype(int)
population_structure_data = population_structure_data[population_structure_data["sa2"] < 999999]

population_structure_data["ethnicity"] = population_structure_data["ethnicity"].str.split(':').str[0]
population_structure_data = population_structure_data[population_structure_data["ethnicity"].isin(["1", "2", "3", "4", "5"])]
population_structure_data["ethnicity"] = population_structure_data["ethnicity"].astype(int)

population_structure_data = population_structure_data[
    population_structure_data["age"].str.split(':').str[0].str.len() == 3]
population_structure_data["age"] = population_structure_data["age"].str.split(':').str[0]
population_structure_data["age"] = population_structure_data["age"].astype(int)


population_structure_data = population_structure_data[population_structure_data["gender"].isin([1, 2, 3])]
population_structure_data = population_structure_data.dropna()
population_structure_data = population_structure_data.reset_index()
population_structure_data = population_structure_data[["sa2", "ethnicity", "age", "gender", "value"]]

population_structure_data.to_csv("etc/data/open_data/population_structure_2023.csv")
