from copy import deepcopy
from math import ceil as math_ceil
from os.path import join
from re import match as re_match

from numpy import inf, nan
from pandas import DataFrame, concat, melt, merge, read_csv, read_excel, to_numeric

from funcs import RAW_DATA
from funcs.utils import sort_column_by_names

from funcs.preproc import _read_raw_population, _read_raw_age, _read_raw_gender, _read_raw_ethnicity, _read_raw_nzdep


def create_age_based_on_scaler(total_population_data: DataFrame, raw_age_path: str) -> DataFrame:
    """
    Adjusts age-based population data using a scaling factor derived from total population data.

    Args:
        total_population_data (DataFrame): A DataFrame containing total population data by region.

    Returns:
        DataFrame: A DataFrame with adjusted population data by age and region.

    The function performs the following steps:
    1. Reads raw age-based population data.
    2. Merges the age-based data with the total population data on the 'area' column.
    3. Calculates a scaling factor for each region by dividing the population by the total.
    4. Adjusts the age-based population counts using the scaling factor.
    5. Replaces infinite values with NaN and drops rows with NaN values.
    6. Rounds the adjusted population counts and converts them to integers.
    7. Drops the 'total', 'population', and 'scaler' columns.
    8. Renames the columns to ensure they are integers representing age groups.

    Returns:
        DataFrame: The final DataFrame with adjusted age-based population data by region.
    """
    age_data = _read_raw_age(raw_age_path) 

    age_data = age_data.merge(total_population_data, on="area")
    age_data["scaler"] = age_data["population"] / age_data["total"]

    for col in [item for item in list(age_data.columns) if item not in [
        "area", "total", "population", "ratop"]]:
        age_data[col] = age_data[col] / age_data["scaler"]

    age_data.replace([inf, -inf], nan, inplace=True)
    age_data.dropna(inplace=True)

    age_data = age_data.round().astype(int)

    age_data = age_data.drop(["total", "population", "scaler"], axis=1)

    age_data.columns = ["area"] + [
        int(col) for col in age_data.columns if col not in ["area"]
    ]

    return age_data


def create_ethnicity_ratio(raw_ethnicity_path: str, age_group_keys: list = [0, 15, 30, 65]):
    """
    Processes raw ethnicity data and creates a DataFrame with ethnicity and age group proportions.

    Args:
        raw_ethnicity_path (str): The file path to the raw ethnicity data.
        age_group_keys (list, optional): List of age group keys to 
        be used for grouping. Defaults to [0, 15, 30, 65].

        The output looks like:

                area ethnicity         0        15        30        65
        0      100100  European  0.356250  0.333333  0.467797  0.630252
        1      100200  European  0.427152  0.440217  0.519280  0.644928
        2      100300  European  1.000000  1.000000  0.842105  1.000000
        3      100400  European  0.381818  0.418182  0.612903  0.780702
        4      100500  European  0.435484  0.473684  0.504854  0.597015
        ....

        For each area, we have sth like:
                area ethnicity        0        15        30        65
        0     100100  European  0.35625  0.333333  0.467797  0.630252
        2134  100100     Maori  0.55625  0.555556  0.474576  0.344538
        4268  100100   Pacific  0.06250  0.070707  0.037288  0.025210
        6402  100100     Asian  0.01875  0.040404  0.013559  0.000000
        8536  100100     MELAA  0.00625  0.000000  0.006780  0.000000

    Returns:
        DataFrame: A DataFrame with columns for area, ethnicity, 
        and the proportions of each age group within each area.
    """
    dfs = _read_raw_ethnicity(raw_ethnicity_path)

    dfs_output = []
    for proc_age in list(dfs["age"].unique()):
        dfs_output.append(
            melt(
                dfs[dfs["age"] == proc_age],
                id_vars=["area"],
                value_vars=[
                    "European",
                    "Maori",
                    "Pacific",
                    "Asian",
                    "MELAA",
                ],
                var_name="ethnicity",
                value_name=proc_age,
            )
        )

    # Assuming 'dataframes' is a list containing your DataFrames
    combined_df = merge(dfs_output[0], dfs_output[1], on=["area", "ethnicity"])
    for i in range(2, len(dfs_output)):
        combined_df = merge(combined_df, dfs_output[i], on=["area", "ethnicity"])

    combined_df.columns = ["area", "ethnicity"] + [
        int(col)
        for col in combined_df.columns
        if col not in ["area", "ethnicity"]
    ]

    combined_df[age_group_keys] = combined_df.groupby(
        "area")[age_group_keys].transform(lambda x: x / x.sum())

    return combined_df


def create_female_ratio(raw_gender_path: str) -> DataFrame:
    """Write gender_profile_female_ratio

    Args:
        workdir (str): Working directory
        gender_profile_female_ratio_cfg (dict): gender_profile_female_ratio configuration
    """

    df = _read_raw_gender(raw_gender_path)

    for age in ["15", "40", "65", "90"]:
        df[age] = df[f"Female ({age})"] / (df[f"Male ({age})"] + df[f"Female ({age})"])

    df = df[["area", "15", "40", "65", "90"]]

    df = df.dropna()

    df.columns = ["area"] + [
        int(col) for col in df.columns if col not in ["area"]
    ]

    return df


def create_base_population(raw_population_path: str):
    """Read population
    """
    return _read_raw_population(raw_population_path)


def create_nzdep(nzdep_data_path: str):
    """Read nz deprivation index
    """
    return _read_raw_nzdep(nzdep_data_path)



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
    for proc_area in age_data["area"].unique():
        proc_gender_data = gender_data[gender_data["area"] == proc_area]

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
    """
    Calculate the percentage of each ethnicity for each age group within specified areas.

    This function maps age groups to individual ages and calculates the percentage of each ethnicity
    for each age within those groups. The results are concatenated into a single DataFrame.

    Parameters:
        age_data (DataFrame): A DataFrame containing age-related data, including an 'area' column.
        ethnicity_data (DataFrame): A DataFrame containing ethnicity data, including an 'area' column and columns for each age group.
    
    The output is sth like

                area  ethnicity            1        2        3      ...           99       100
        0      100100  European     0.356250  0.333333  0.467797    ...     0.630252  0.630252
        1      100100     Maori     0.556250  0.555556  0.474576    ...     0.556250  0.556250  
        2      100100   Pacific     0.062500  0.070707  0.037288    ...     0.062500  0.062500  
        3      100100     Asian     0.018750  0.040404  0.013559    ...     0.018750  0.018750  
        4      100100     MELAA     0.006250  0.000000  0.006780    ...     0.006250  0.006250
            ......
    
    Returns:
        DataFrame: A concatenated DataFrame with the percentage of each ethnicity for each age within the specified areas.
    """
    age_mapping = {
        0: list(range(0, 15)),
        15: list(range(15, 30)),
        30: list(range(30, 65)),
        65: list(range(65, 101)),
    }

    ethnicity_data_percentage = []
    for proc_area in age_data["area"].unique():
        proc_ethnicity_data = ethnicity_data[ethnicity_data["area"] == proc_area]

        df_new = proc_ethnicity_data.copy()
        for age_group, ages in age_mapping.items():
            # calculate the percentage for each individual age
            individual_percentage = proc_ethnicity_data[age_group]
            for age in ages:
                df_new[age] = individual_percentage

        ethnicity_data_percentage.append(df_new)

    return concat(ethnicity_data_percentage, axis=0, ignore_index=True)


def map_feature_percentage_data_with_age_population_data(
    age_data: DataFrame,
    feature_percentage_data: DataFrame,
    check_consistency: bool = True,
):
    output = []
    for proc_area in age_data["area"].unique():
        proc_age_data = age_data[age_data["area"] == proc_area]

        proc_feature_percentage_data = (
            feature_percentage_data[feature_percentage_data["area"] == proc_area]
            .reset_index()
            .drop("index", axis=1)
        )

        if len(proc_feature_percentage_data) == 0:
            continue

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
            cols = [col for col in output.columns if col not in ["area", "gender", "ethnicity"]]
            output[cols] = output[cols] * (total_data_from_age / total_data_from_feature)

    return sort_column_by_names(output, ["area", "gender", "ethnicity"])