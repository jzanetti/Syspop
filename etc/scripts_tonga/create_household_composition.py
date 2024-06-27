# https://microdata.worldbank.org/index.php/catalog/4177/data-dictionary/F2?file_name=hl


from os.path import join

from pandas import read_csv

workdir = "etc/data/raw_tonga_latest"

area_name = ["Urban Tongatapu", "Rural Tongatapu"]

household_info_data = read_csv("etc/data/raw_tonga_latest/hh.csv")
household_list_data = read_csv("etc/data/raw_tonga_latest/hl.csv")
school_data = read_csv("etc/data/raw_tonga_latest/fs.csv")


household_info_data = household_info_data[household_info_data["HH7A"].isin(area_name)]

household_list_data = household_list_data[
    household_list_data["HH1"].isin(household_info_data["HH1"])
]
school_data = school_data[school_data["HH1"].isin(household_info_data["HH1"])]

household_list_data = household_list_data.drop("Unnamed: 0", axis=1)

household_list_data_map = {
    "HL1": "person_id",
    "HL4": "sex",
    "HL6": "age",
    "ethnicity": "ethnicity",
    "windex5": "wealth_index",
}

household_list_data = household_list_data[household_list_data_map.keys()]

household_list_data = household_list_data.rename(
    columns=household_list_data_map, inplace=False
)

household_list_data["hhd_id"] = (household_list_data["person_id"] == 1).cumsum()
household_list_data["age"] = household_list_data["age"].replace("95+", "95")
household_list_data["age"] = household_list_data["age"].astype(float)


def classify_age(age):
    if age >= 18:
        return "adult"
    else:
        return "child"


# Apply the function to the 'age' column
household_list_data["age_group"] = household_list_data["age"].apply(classify_age)

# Group by 'hhd_id' and 'age_group', and count the number of people in each group
df_grouped = (
    household_list_data.groupby(["hhd_id", "age_group"]).size().unstack(fill_value=0)
)

# Calculate the percentage of households with each composition
df_grouped["percentage"] = df_grouped["adult"] / (
    df_grouped["adult"] + df_grouped["child"]
)

# Rename the columns for clarity
df_grouped.columns = ["child_num", "adult_num", "percentage"]

# Group by 'child_num' and 'adult_num', and calculate the percentage for each unique combination
df_composition = df_grouped.groupby(["child_num", "adult_num"]).size() / len(df_grouped)

# Convert the series to a dataframe and reset the index
df_composition = df_composition.reset_index()

# Rename the columns for clarity
df_composition.columns = ["child_num", "adult_num", "percentage"]

# Print the new dataframe
print(df_composition)

df_composition.to_csv(join(workdir, "household_composition.csv"))
