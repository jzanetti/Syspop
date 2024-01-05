from copy import deepcopy
from math import ceil as math_ceil
from os.path import join
from re import match as re_match

from numpy import inf, nan
from pandas import DataFrame, concat, melt, merge, read_csv, read_excel, to_numeric

from funcs import RAW_DATA
from funcs.utils import sort_column_by_names

def create_age(total_population_data: DataFrame):
    def _find_range(number, ranges):
        for age_range in ranges:
            start, end = map(int, age_range.split("-"))
            if start <= number <= end:
                return age_range
        return None

    df = read_excel(RAW_DATA["population"]["population_by_age"], header=2)

    df.columns = df.columns.str.strip()

    df = df[
        [
            "Region and Age",
            "0-4 Years",
            "5-9 Years",
            "10-14 Years",
            "15-19 Years",
            "20-24 Years",
            "25-29 Years",
            "30-34 Years",
            "35-39 Years",
            "40-44 Years",
            "45-49 Years",
            "50-54 Years",
            "55-59 Years",
            "60-64 Years",
            "65-69 Years",
            "70-74 Years",
            "75-79 Years",
            "80-84 Years",
            "85-89 Years",
            "90 Years and over",
        ]
    ]

    df = df.drop(df.index[-1])

    df["Region and Age"] = df["Region and Age"].str.strip()

    df = df[~df["Region and Age"].isin(["NZRC", "NIRC", "SIRC"])]

    df["Region and Age"] = df["Region and Age"].astype(int)

    df = df[df["Region and Age"] > 10000]

    df = df.set_index("Region and Age")

    df.columns = [str(name).replace(" Years", "") for name in df]
    df = df.rename(columns={"90 and over": "90-100"})

    new_df = DataFrame(columns=["Region"] + list(range(0, 101)))

    for cur_age in list(new_df.columns):
        if cur_age == "Region":
            new_df["Region"] = df.index
        else:
            age_range = _find_range(cur_age, list(df.columns))
            age_split = age_range.split("-")
            start_age = int(age_split[0])
            end_age = int(age_split[1])
            age_length = end_age - start_age + 1
            new_df[cur_age] = (df[age_range] / age_length).values

    new_df = new_df.applymap(math_ceil)

    new_df = new_df.rename(columns={"Region": "area"})

    all_ages = range(101)
    for index, row in new_df.iterrows():
        total = sum(row[col] for col in all_ages)
        new_df.at[index, "total"] = total

    total_population_data = total_population_data.rename(
        columns={"area": "area"}
    )
    df_after_ratio = new_df.merge(total_population_data, on="area")
    df_after_ratio["ratio"] = df_after_ratio["population"] / df_after_ratio["total"]

    for col in all_ages:
        df_after_ratio[col] = df_after_ratio[col] / df_after_ratio["ratio"]

    df_after_ratio.replace([inf, -inf], nan, inplace=True)
    df_after_ratio.dropna(inplace=True)

    df_after_ratio = df_after_ratio.round().astype(int)

    new_df = df_after_ratio.drop(["total", "population", "ratio"], axis=1)

    new_df.columns = ["area"] + [
        int(col) for col in new_df.columns if col not in ["area"]
    ]


    return new_df


def create_ethnicity_and_age(total_population_data: DataFrame):
    dfs = {}

    for proc_age_key in RAW_DATA["population"]["population_by_age_by_ethnicity"]:
        df = read_excel(
            RAW_DATA["population"]["population_by_age_by_ethnicity"][proc_age_key],
            header=4,
        )
        df = df.drop([0, 1]).drop(df.tail(3).index)
        df = df.drop("Unnamed: 1", axis=1)
        df.columns = df.columns.str.strip()

        df = df.rename(
            columns={
                "Ethnic group": "area",
                "Pacific Peoples": "Pacific",
                "Middle Eastern/Latin American/African": "MELAA",
            }
        )

        df = (
            df.apply(to_numeric, errors="coerce").dropna().astype(int)
        )  # convert str ot others to NaN, and drop them and convert the rests to int

        df["total"] = (
            df["European"] + df["Maori"] + df["Pacific"] + df["Asian"] + df["MELAA"]
        )

        dfs[proc_age_key] = df

    df_ratio = concat(list(dfs.values()))
    df_ratio = df_ratio.groupby("area").sum().reset_index()
    total_population_data = total_population_data.rename(
        columns={"area": "area"}
    )
    df_ratio = df_ratio.merge(total_population_data, on="area")
    df_ratio["ratio"] = df_ratio["population"] / df_ratio["total"]
    df_ratio = df_ratio.drop(
        ["European", "Maori", "Pacific", "Asian", "MELAA", "total", "population"],
        axis=1,
    )

    dfs_after_ratio = {}
    for proc_age in dfs:
        df = dfs[proc_age]

        df = df.merge(df_ratio, on="area")
        for race_key in ["European", "Maori", "Pacific", "Asian", "MELAA", "total"]:
            df[race_key] = df[race_key] * df["ratio"]
        df = df.drop(["ratio", "total"], axis=1)
        # df = df.astype(int)
        # df = df.apply(math_ceil).astype(int)
        df = df.round().astype(int)
        dfs_after_ratio[proc_age] = df

    dfs = dfs_after_ratio

    dfs_output = []
    for proc_age in dfs:
        dfs_output.append(
            melt(
                dfs[proc_age],
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


    return combined_df


def create_female_ratio():
    """Write gender_profile_female_ratio

    Args:
        workdir (str): Working directory
        gender_profile_female_ratio_cfg (dict): gender_profile_female_ratio configuration
    """

    df = read_excel(RAW_DATA["population"]["population_by_age_by_gender"], header=3)

    df = df.rename(
        columns={
            "Male": "Male (15)",
            "Female": "Female (15)",
            "Male.1": "Male (40)",
            "Female.1": "Female (40)",
            "Male.2": "Male (65)",
            "Female.2": "Female (65)",
            "Male.3": "Male (90)",
            "Female.3": "Female (90)",
            "Sex": "area",
        }
    )

    df = df.drop("Unnamed: 1", axis=1)

    df = df.drop([0, 1, 2]).drop(df.tail(3).index).astype(int)

    df = df[df["area"] > 10000]

    for age in ["15", "40", "65", "90"]:
        df[age] = df[f"Female ({age})"] / (df[f"Male ({age})"] + df[f"Female ({age})"])

    df = df[["area", "15", "40", "65", "90"]]

    df = df.dropna()

    df.columns = ["area"] + [
        int(col) for col in df.columns if col not in ["area"]
    ]

    return df


def create_population():
    """Read population

    Args:
        population_path (str): Population data path
    """
    data = read_excel(RAW_DATA["population"]["total_population"], header=6)

    data = data.rename(columns={"Area": "area", "Unnamed: 2": "population"})

    data = data.drop("Unnamed: 1", axis=1)

    # Drop the last row
    data = data.drop(data.index[-1])

    data = data.astype(int)

    data = data[data["area"] > 10000]

    return data


def create_socialeconomic(geography_hierarchy_data: DataFrame):
    """Write area area_socialeconomic_index data

    Args:
        workdir (str): Working directory
        area_socialeconomic_index_cfg (dict): Area_socialeconomic_index configuration
        geography_hierarchy_definition (DataFrame or None): Geography hierarchy definition
    """
    data = read_csv(RAW_DATA["population"]["socialeconomics"])[
        ["SA22018_code", "SA2_average_NZDep2018"]
    ]

    data = data.rename(
        columns={
            "SA22018_code": "area",
            "SA2_average_NZDep2018": "socioeconomic_centile",
        }
    )

    # get hierarchy defination data
    geog_hierarchy = geography_hierarchy_data[["area"]]

    data = merge(data, geog_hierarchy, on="area")

    return data



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
    age_mapping = {
        0: list(range(0, 15)),
        15: list(range(15, 30)),
        30: list(range(30, 65)),
        65: list(range(65, 101)),
    }
    ethnicity_data_percentage = []
    for proc_area in age_data["area"].unique():
        proc_ethnicity_data = ethnicity_data[ethnicity_data["area"] == proc_area]
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