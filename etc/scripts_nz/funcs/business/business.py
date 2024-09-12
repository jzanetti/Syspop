from copy import copy, deepcopy
from math import ceil as math_ceil
from os.path import join
from re import match as re_match

from funcs import RAW_DATA, RAW_DATA_INFO
from funcs.utils import read_anzsic_code, read_leed
from numpy import inf, nan
from pandas import (
    DataFrame,
    concat,
    merge,
    pivot_table,
    read_csv,
    read_excel,
    to_numeric,
)


def _read_employers_by_employees_data() -> DataFrame:
    """Read the number of employers by employee number

    Returns:
        DataFrame: Employers number by employees number
    """

    data = read_csv(RAW_DATA["business"]["employers_by_employees_number"])[
        ["Area", "Measure", "Enterprise employee count size group", "Value"]
    ]
    data["Area"] = data["Area"].str.replace(" Region", "")

    data = data.drop(
        data[data["Enterprise employee count size group"] == "Total"].index
    )
    data = data[data["Measure"] == "Geographic Units"]

    data = data[["Area", "Enterprise employee count size group", "Value"]]

    data["Area"] = data["Area"].replace("Manawatu-Wanganui", "Manawatu-Whanganui")

    data["Enterprise employee count size group"] = (
        data["Enterprise employee count size group"]
        .replace("1 to 19", "1-19")
        .replace("20 to 49", "20-49")
        .replace("50+", "50-xxx")
    )

    return data.rename(columns={"Area": "region", "Value": "employer_num"})


def read_leed(
    leed_path: str, anzsic_code: DataFrame, if_rate: bool = False
) -> DataFrame:
    """Read NZ stats LEED data

    Args:
        leed_path (str): leed path to be processed
        anzsic_code (Dataframe): ANZSIC codes
        if_rate (bool): if return male and female rate

    Returns:
        DataFrame: Leed dataset
    """
    df = read_excel(leed_path)
    industrial_row = df.iloc[0].fillna(method="ffill")

    if anzsic_code is not None:
        for i, row in enumerate(industrial_row):
            row = row.strip()

            if row in ["Industry", "Total people"]:
                continue

            code = anzsic_code[anzsic_code["Description"] == row]["Anzsic06"].values[0]
            industrial_row[i] = code

    # x = anzsic_code.set_index("Description")
    sec_row = df.iloc[1].fillna(method="ffill")
    titles = industrial_row + "," + sec_row
    titles[
        "Number of Employees by Industry, Age Group, Sex, and Region (derived from 2018 Census)"
    ] = "Area"
    titles["Unnamed: 1"] = "Age"
    titles["Unnamed: 2"] = "tmp"

    df = df.iloc[3:]
    df = df.drop(df.index[-1:])
    df = df.rename(columns=titles)
    df = df.drop("tmp", axis=1)
    df["Area"] = df["Area"].fillna(method="ffill")
    # return df.rename(columns=lambda x: x.strip())

    df["Area"] = df["Area"].replace(
        "Manawatu-Wanganui Region", "Manawatu-Whanganui Region"
    )

    if anzsic_code is not None:
        character_indices = set(
            [
                col.split(",")[0][0]
                for col in df.columns
                if col
                not in ["Area", "Age", "Total people,Male", "Total people, Female"]
            ]
        )

        # Iterate over the unique character indices to sum the corresponding columns
        for char_index in character_indices:
            subset_cols_male = [
                col
                for col in df.columns
                if col.startswith(char_index)
                and col.endswith("Male")
                and col
                not in ["Area", "Age", "Total people,Male", "Total people,Female"]
            ]
            subset_cols_female = [
                col
                for col in df.columns
                if col.startswith(char_index)
                and col.endswith("Female")
                and col
                not in ["Area", "Age", "Total people,Male", "Total people,Female"]
            ]
            summed_col_male = f"{char_index},Male"
            summed_col_female = f"{char_index},Female"
            df[summed_col_male] = df[subset_cols_male].sum(axis=1)
            df[summed_col_female] = df[subset_cols_female].sum(axis=1)
            df = df.drop(subset_cols_male + subset_cols_female, axis=1)

    df["Area"] = df["Area"].str.replace(" Region", "")

    if not if_rate:
        return df

    industrial_columns = [
        x
        for x in list(df.columns)
        if x not in ["Area", "Age", "Total people,Male", "Total people,Female"]
    ]

    df = df.groupby("Area")[industrial_columns].sum()

    df_rate = deepcopy(df)

    # Calculate percentages
    for column in df.columns:
        group = column.split(",")[0]
        total = df[[f"{group},Male", f"{group},Female"]].sum(
            axis=1
        )  # Calculate the total for the group

        total.replace(0, nan, inplace=True)
        df_rate[column] = df[column] / total

    return df_rate


def create_employee_by_gender_by_sector(
    pop: DataFrame,
    geography_hierarchy_data: DataFrame,
):
    """Write the number of employees by gender for different area

    Args:
        workdir (str): Working directory
        employees_cfg (dict): Configuration
        use_sa3_as_super_area (bool): If apply SA3 as super area, otherwise using regions
    """

    def _rename_column(column_name):
        # Define a regular expression pattern to match the column names
        pattern = r"([a-zA-Z]+) ([a-zA-Z]+)"
        matches = re_match(pattern, column_name)
        if matches:
            gender = matches.group(1)[0].lower()
            category = matches.group(2)
            return f"{gender} {category}"
        return column_name

    # Read Leed rate for region
    data_leed_rate = read_leed(
        RAW_DATA["business"]["employee_by_gender_by_sector"]["leed"],
        read_anzsic_code(
            RAW_DATA["business"]["employee_by_gender_by_sector"]["anzsic_code"]
        ),
        if_rate=True,
    )

    # Read employees for different area
    data = read_csv(
        RAW_DATA["business"]["employee_by_gender_by_sector"]["employee_by_area"]
    )[["anzsic06", "Area", "ec_count", "geo_count"]]

    data = data[
        data["anzsic06"].isin(
            list(set([col.split(",")[0] for col in data_leed_rate.columns]))
        )
    ]

    data = data[data["Area"].str.startswith("A")]

    data = data.rename(columns={"Area": "area"})

    data["area"] = data["area"].str[1:].astype(int)

    data = data.merge(
        geography_hierarchy_data[["area", "super_area", "region"]],
        on="area",
        how="left",
    )
    data = data.dropna()

    data_leed_rate = data_leed_rate.reset_index()

    data_leed_rate = data_leed_rate.rename(columns={"Area": "region"})
    data = data.merge(data_leed_rate, on="region", how="left")

    industrial_codes = []
    industrial_codes_with_genders = []
    for proc_item in list(data.columns):
        if proc_item.endswith("Male"):
            proc_code = proc_item.split(",")[0]
            industrial_codes.append(proc_code)
            industrial_codes_with_genders.extend(
                [f"{proc_code},Male", f"{proc_code},Female"]
            )

    # Create new columns 'male' and 'female' based on 'anzsic06' prefix
    for category in industrial_codes:
        male_col = f"{category},Male"
        female_col = f"{category},Female"
        data.loc[data["anzsic06"] == category, "Male"] = (
            data[male_col] * data["ec_count"]
        )
        data.loc[data["anzsic06"] == category, "Female"] = (
            data[female_col] * data["ec_count"]
        )

    anzsic_unique_values = data["anzsic06"].unique()
    anzsic_mapping = {
        anzsic: [f"{anzsic},Male", f"{anzsic},Female"]
        for anzsic in anzsic_unique_values
    }

    all_data = []
    for i in range(len(data)):
        proc_data = data.iloc[[i]]
        proc_anzsic = proc_data["anzsic06"].values[0]
        proc_data = proc_data[
            ["area", "anzsic06", "geo_count", "ec_count"] + anzsic_mapping[proc_anzsic]
        ]
        proc_data.columns = [
            col.split(",")[1] if "," in col else col for col in proc_data.columns
        ]
        all_data.append(proc_data)

    all_data = concat(all_data, ignore_index=True)

    # all_data["super_area"] = all_data["super_area"].astype(int)

    all_data = all_data.rename(
        columns={
            "geo_count": "employer_number",
            "ec_count": "employee_number",
            "anzsic06": "business_code",
            "Male": "employee_male_ratio",
            "Female": "employee_female_ratio",
        }
    )

    return all_data
