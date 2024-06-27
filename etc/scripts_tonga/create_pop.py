from os.path import join

from pandas import DataFrame, concat, read_excel

workdir = "etc/data/raw_tonga_latest"
area_name = "Tongatapu"
# --------------------------------
# 1.1 Number of total people vs age:
# --------------------------------
total_people = read_excel(
    "etc/data/raw_tonga_latest/tonga_pop_data.xlsx", sheet_name="Total people and age"
)

total_people = total_people.rename(columns={"Age / Sex": "age"})


new_df = DataFrame(columns=["area"] + list(range(101)))

# Set the area name
new_df.loc[0, "area"] = area_name


# Iterate over each row in the original dataframe
for i, row in total_people.iterrows():
    # Get the age range and the count
    age_range = row["age"]
    count = row[area_name]

    # If the age range is '< 5', consider it as '0-4'
    if age_range == "< 5":
        age_range = "0-4"

    # If the age range is '75+', set the count for ages 75 to 100
    if age_range == "75+":
        new_df.loc[0, 75:100] = (
            count // 26
        )  # 26 is the number of ages from 75 to 100 inclusive
    else:
        # Get the start and end of the age range
        start, end = map(int, age_range.split("-"))

        # Set the count for each age in the range
        new_df.loc[0, start : end + 1] = count // (end - start + 1)

new_df.to_csv(join(workdir, "population_by_age-2019.csv"), index=False)


# --------------------------------
# 1.2 Number of people for different ethnicities vs age:
# --------------------------------
ethnicity_people = read_excel(
    "etc/data/raw_tonga_latest/tonga_pop_data.xlsx", sheet_name="Ethnicity and age"
)

ethnicity_people = ethnicity_people.iloc[1:]
ethnicity_people = ethnicity_people.rename(columns={"Age / Sex": "age"})
ethnicity_people["age"] = ethnicity_people["age"].replace("75+", "75-100")


# Melt the DataFrame
df_melted = ethnicity_people.melt(id_vars="age", var_name="ethnicity")

# Add the 'area' column
df_melted["area"] = area_name

# Split the 'Age' column into separate columns for each age
df_melted[["age_start", "age_end"]] = df_melted["age"].str.split("-", expand=True)

# Convert the new age columns to integers
df_melted["age_start"] = df_melted["age_start"].astype(int)
df_melted["age_end"] = df_melted["age_end"].astype(int)

# Create a new DataFrame to hold the final result
df_final = DataFrame()

# Iterate over each row in the melted DataFrame
for index, row in df_melted.iterrows():
    # For each age in the range for this row
    for age in range(row["age_start"], row["age_end"] + 1):
        # Copy the row to a new DataFrame and set the 'Age' column to the current age
        new_row = row.copy()
        new_row["age"] = age
        new_row["value"] = new_row["value"] / (row["age_end"] - row["age_start"] + 1)
        df_final = concat([df_final, DataFrame([new_row])], ignore_index=True)

# Drop the 'Age_start' and 'Age_end' columns
df_final = df_final.drop(columns=["age_start", "age_end"])

# Pivot the DataFrame to get one column per age
df_final = df_final.pivot_table(
    index=["area", "ethnicity"], columns="age", values="value"
)

# Reset the index
df_final = df_final.reset_index()

df_final.columns.name = None

df_final.to_csv(join(workdir, "population_by_ethnicity-2022.csv"), index=False)

# --------------------------------
# 1.3 Number of people for different gender vs age:
# --------------------------------
gender_people = read_excel(
    "etc/data/raw_tonga_latest/tonga_pop_data.xlsx", sheet_name="Gender and age"
)
gender_people = gender_people.rename(
    columns={"Age": "age", "Gender": "gender", "Number": "num"}
)
gender_people["age"] = gender_people["age"].replace("75+", "75-100")
gender_people["age"] = gender_people["age"].replace("< 5", "0-4")
new_df = DataFrame(columns=["area", "gender"] + [str(i) for i in range(101)])

# Populate the new dataframe
for index, row in gender_people.iterrows():
    age_range = row["age"].split("-")
    start_age = int(age_range[0])
    end_age = int(age_range[1])
    num_per_year = row["num"] / (end_age - start_age + 1)

    new_row = {"area": "area1", "gender": row["gender"]}
    for age in range(start_age, end_age + 1):
        new_row[str(age)] = num_per_year

    new_df = concat([new_df, DataFrame([new_row])], ignore_index=True)

# Group by 'area' and 'gender' and apply forward fill to fill NaN values
df_grouped = new_df.groupby(["area", "gender"], as_index=False).apply(
    lambda group: group.ffill()
)

df_grouped = df_grouped.groupby(["area", "gender"], as_index=False).apply(
    lambda group: group.bfill()
)
df_grouped = df_grouped.drop_duplicates()

df_grouped.reset_index(inplace=True)

# df_grouped = df_grouped.drop(["level_0", "level_1", "level_2"], axis=1)

df_grouped = df_grouped.iloc[[0, 2]]

df_grouped.reset_index(inplace=True)

df_grouped = df_grouped.drop(["index", "level_0", "level_1", "level_2"], axis=1)

df_grouped["area"] = area_name

df_grouped.to_csv(join(workdir, "population_by_gender-2022.csv"), index=False)
