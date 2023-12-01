from copy import deepcopy
from math import ceil as math_ceil
from os.path import join
from re import match as re_match

from numpy import arctan2, argmin, cos, inf, nan, radians, sin, sqrt, unique
from pandas import DataFrame, concat, melt, merge, read_csv, read_excel, to_numeric
from shapely.wkt import loads as wkt_loads


def haversine_distance(lat1, lon1, lat2, lon2):
    """Function to calculate the Haversine distance between two points

    Args:
        lat1 (float): Latitude 1
        lon1 (float): Longitude 1
        lat2 (float): Latitude 1
        lon2 (float): Longitude 2

    Returns:
        _type_: _description_
    """
    r = 6371  # Earth's radius in kilometers
    phi1 = radians(lat1)
    phi2 = radians(lat2)
    delta_phi = radians(lat2 - lat1)
    delta_lambda = radians(lon2 - lon1)
    a = sin(delta_phi / 2) ** 2 + cos(phi1) * cos(phi2) * sin(delta_lambda / 2) ** 2
    c = 2 * arctan2(sqrt(a), sqrt(1 - a))
    distance = r * c
    return distance


def get_central_point(wkt_string: str):
    """Get central point from MultiPolygon

    Args:
        wkt (str): WKT description

    Returns:
        _type_: _description_
    """
    polygon = wkt_loads(wkt_string)
    central_point = polygon.centroid
    return central_point


def read_anzsic_code(anzsic06_code_path: str) -> DataFrame:
    """Read ANZSIC code

    Args:
        anzsic06_code_path (str): ANZSIC data path

    Returns:
        DataFrame: code in a dataframe
    """
    anzsic_code = read_csv(anzsic06_code_path)

    for _, row in anzsic_code.iterrows():
        row["Description"] = " ".join(row["Description"].split()[1:])

    return anzsic_code


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
