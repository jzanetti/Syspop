from dask import compute as dask_compute
from dask import delayed
from dask.distributed import Client
from numpy.random import choice
from pandas import DataFrame, concat, read_csv, read_parquet


def read_data():
    age_data = read_csv("data/inputs/processed/age_data.csv")
    ethnicity_data = read_csv("data/inputs/processed/ethnicity_and_age_data.csv")
    gender_data = read_csv("data/inputs/processed/female_ratio_data.csv")

    gender_data.columns = ["output_area"] + [
        int(col) for col in gender_data.columns if col not in ["output_area"]
    ]

    ethnicity_data.columns = ["output_area", "ethnicity"] + [
        int(col)
        for col in ethnicity_data.columns
        if col not in ["output_area", "ethnicity"]
    ]

    age_data.columns = ["output_area"] + [
        int(col) for col in age_data.columns if col not in ["output_area"]
    ]

    return {
        "age_data": age_data,
        "gender_data": gender_data,
        "ethnicity_data": ethnicity_data,
    }


# ----------------------------------
# convert percentage to actual people
# ----------------------------------
def map_feature_percentage_data_with_age_population_data(
    age_data: DataFrame,
    feature_percentage_data: DataFrame,
    check_consistency: bool = True,
):
    output = []
    for proc_area in age_data["output_area"].unique():
        proc_age_data = age_data[age_data["output_area"] == proc_area]

        proc_feature_percentage_data = (
            feature_percentage_data[feature_percentage_data["output_area"] == proc_area]
            .reset_index()
            .drop("index", axis=1)
        )
        proc_age_data_repeated = (
            concat(
                [proc_age_data] * len(proc_feature_percentage_data), ignore_index=True
            )
            .reset_index()
            .drop("index", axis=1)
        )

        proc_feature_percentage_data[list(range(101))] = (
            proc_age_data_repeated[list(range(101))]
            * proc_feature_percentage_data[list(range(101))]
        )

        output.append(proc_feature_percentage_data)

    output = concat(output, axis=0, ignore_index=True)

    if check_consistency:
        total_data_from_feature = output[list(range(101))].sum().sum()

        total_data_from_age = age_data[list(range(101))].sum().sum()

        if total_data_from_feature != total_data_from_age:
            raise Exception("Matching process may have issues ...")

    return output


def create_gender_percentage_for_each_age(age_data: DataFrame, gender_data: DataFrame):
    # add Male/Female groups:
    gender_data_male = gender_data.copy()
    gender_data_male[[15, 40, 65, 90]] = gender_data_male[[15, 40, 65, 90]].applymap(
        lambda x: 1.0 - x
    )
    gender_data_male["gender"] = "male"
    gender_data["gender"] = "female"

    # Concatenate the two DataFrames
    gender_data = concat([gender_data, gender_data_male]).reset_index(drop=True)

    # create percentage for each age
    age_mapping = {
        15: list(range(0, 15)),
        40: list(range(15, 40)),
        65: list(range(40, 65)),
        90: list(range(65, 101)),
    }

    gender_data_percentage_all = []
    for proc_area in age_data["output_area"].unique():
        proc_gender_data = gender_data[gender_data["output_area"] == proc_area]

        df_new = proc_gender_data.copy()
        for age_group, ages in age_mapping.items():
            # calculate the percentage for each individual age
            individual_percentage = proc_gender_data[age_group]

            # create new columns for each individual age
            for age in ages:
                df_new[age] = individual_percentage

        gender_data_percentage_all.append(df_new)

    return concat(gender_data_percentage_all, axis=0, ignore_index=True)


def create_ethnicity_percentage_for_each_age(
    age_data: DataFrame, ethnicity_data: DataFrame
):
    age_mapping = {
        0: list(range(0, 15)),
        15: list(range(15, 30)),
        30: list(range(30, 65)),
        65: list(range(65, 101)),
    }
    ethnicity_data_percentage = []
    for proc_area in age_data["output_area"].unique():
        proc_ethnicity_data = ethnicity_data[ethnicity_data["output_area"] == proc_area]
        age_group_keys = list(age_mapping.keys())
        proc_ethnicity_data_total = proc_ethnicity_data[age_group_keys].sum()
        proc_ethnicity_data[age_group_keys] = (
            proc_ethnicity_data[age_group_keys] / proc_ethnicity_data_total
        )

        df_new = proc_ethnicity_data.copy()
        for age_group, ages in age_mapping.items():
            # calculate the percentage for each individual age
            individual_percentage = proc_ethnicity_data[age_group]

            # create new columns for each individual age
            for age in ages:
                df_new[age] = individual_percentage

        ethnicity_data_percentage.append(df_new)

    return concat(ethnicity_data_percentage, axis=0, ignore_index=True)


def sort_column_by_names(data_input: DataFrame, columns_to_exclude: list):
    cols = list(data_input.columns)

    # Remove 'output_area' and 'gender' from the list
    for proc_col_to_remove in columns_to_exclude:
        cols.remove(proc_col_to_remove)

    # Sort the remaining columns
    cols = sorted(cols, key=lambda x: int(x))

    # Add 'output_area' and 'gender' at the start
    cols = columns_to_exclude + cols

    # Reindex the dataframe
    return data_input.reindex(columns=cols)


@delayed
def process_output_area_age(output_area, age, df_gender_melt, df_ethnicity_melt):
    population = []
    # Get the gender and ethnicity probabilities for the current output_area and age
    gender_probs = df_gender_melt.loc[
        (df_gender_melt["output_area"] == output_area) & (df_gender_melt["age"] == age),
        ["gender", "prob", "count"],
    ]
    ethnicity_probs = df_ethnicity_melt.loc[
        (df_ethnicity_melt["output_area"] == output_area)
        & (df_ethnicity_melt["age"] == age),
        ["ethnicity", "prob", "count"],
    ]

    # Determine the number of individuals for the current output_area and age
    n_individuals = int(gender_probs["count"].sum())

    if n_individuals == 0:
        return []

    # Randomly assign gender and ethnicity to each individual
    genders = choice(gender_probs["gender"], size=n_individuals, p=gender_probs["prob"])

    ethnicities = choice(
        ethnicity_probs["ethnicity"],
        size=n_individuals,
        p=ethnicity_probs["prob"],
    )

    for gender, ethnicity in zip(genders, ethnicities):
        individual = {
            "output_area": output_area,
            "age": age,
            "gender": gender,
            "ethnicity": ethnicity,
        }
        population.append(individual)
    return population


def create_synpop(gender_data, ethnicity_data):
    # Assuming df_gender and df_ethnicity are your dataframes
    df_gender_melt = gender_data.melt(
        id_vars=["output_area", "gender"], var_name="age", value_name="count"
    )
    df_ethnicity_melt = ethnicity_data.melt(
        id_vars=["output_area", "ethnicity"], var_name="age", value_name="count"
    )

    # Normalize the data
    df_gender_melt["prob"] = df_gender_melt.groupby(["output_area", "age"])[
        "count"
    ].apply(lambda x: x / x.sum())
    df_ethnicity_melt["prob"] = df_ethnicity_melt.groupby(["output_area", "age"])[
        "count"
    ].apply(lambda x: x / x.sum())

    # Create synthetic population
    client = Client(n_workers=8)
    results = []
    for output_area in df_gender_melt["output_area"].unique():
        for age in df_gender_melt["age"].unique():
            result = process_output_area_age(
                output_area, age, df_gender_melt, df_ethnicity_melt
            )
            results.append(result)

    population = dask_compute(*results)
    population = [item for sublist in population for item in sublist]
    client.close()

    # Convert the population to a DataFrame
    return DataFrame(population)


if __name__ == "__main__":
    df = read_parquet("syspop.parquet")

    my_data = read_data()
    gender_data_percentage = create_gender_percentage_for_each_age(
        my_data["age_data"], my_data["gender_data"]
    )
    gender_data = map_feature_percentage_data_with_age_population_data(
        my_data["age_data"], gender_data_percentage, check_consistency=True
    )
    gender_data = sort_column_by_names(gender_data, ["output_area", "gender"])

    ethnicity_data_percentage = create_ethnicity_percentage_for_each_age(
        my_data["age_data"], my_data["ethnicity_data"]
    )

    ethnicity_data = map_feature_percentage_data_with_age_population_data(
        my_data["age_data"], ethnicity_data_percentage, check_consistency=True
    )
    ethnicity_data = sort_column_by_names(ethnicity_data, ["output_area", "ethnicity"])

    gender_data.to_parquet("gender_data.parquet")
    ethnicity_data.to_parquet("ethnicity_data.parquet")

    # syspop = create_synpop(gender_data, ethnicity_data)

    # syspop.to_parquet("syspop.parquet")
