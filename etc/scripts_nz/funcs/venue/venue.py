from os import makedirs
from os.path import dirname, exists, join
from time import sleep

import overpy
from funcs import RAW_DATA, RAW_DATA_INFO, REGION_NAMES_CONVERSIONS
from funcs.utils import get_central_point, haversine_distance
from geopy.geocoders import Nominatim
from numpy import argmin
from pandas import DataFrame, merge, read_csv
from scipy.spatial.distance import cdist
from funcs.preproc import _read_raw_schools, _read_raw_kindergarten, _read_raw_hospital, _read_original_csv


def create_osm_space(data_path: str, geography_location: DataFrame):
    """Write shared space from OSM

    Args:
        workdir (str): Working directory
        space_name (str), name such as supermakrts
    """
    data = _read_original_csv(data_path)
    distances = cdist(
        data[["lat", "lon"]],
        geography_location[["latitude", "longitude"]],
        lambda x, y: haversine_distance(x[0], x[1], y[0], y[1]),
    )
    # Find the nearest location in A for each point in B
    nearest_indices = argmin(distances, axis=1)
    data["area"] = geography_location["area"].iloc[nearest_indices].values

    data.dropna(inplace=True)
    data[["area"]] = data[["area"]].astype(int)
    data = data.rename(columns={"lat": "latitude", "lon": "longitude"})

    return data


def create_kindergarten(kindergarten_data_path: str) -> DataFrame:
    """
    Reads and processes raw New Zealand kindergarten data from a CSV file.

    This function reads a CSV file containing kindergarten data and processes it using the
    `_read_raw_nz_kindergarten` function.

    Args:
        kindergarten_data_path (str): The file path to the CSV file containing the raw kindergarten data.

    Returns:
        DataFrame: A pandas DataFrame containing the processed kindergarten data.
    """
    return _read_raw_kindergarten(kindergarten_data_path)


def create_school(
    school_data_path: str,
    sa2_loc: DataFrame,
    max_to_cur_occupancy_ratio=1.2,
) -> dict:
    """Write schools information

    Args:
        workdir (str): Working directory
        school_cfg (dict): School configuration
        max_to_cur_occupancy_ratio (float, optional): In the data, we have the estimated occupancy
            for a school, while in JUNE we need the max possible occupancy. Defaults to 1.2.

    The output is sth like:
                    area  max_students             sector   latitude   longitude  age_min  age_max
        0     133400             0          secondary -36.851138  174.760643       14       19
        1     167100          1087  primary_secondary -36.841742  175.696738        5       19
        3     358500            28            primary -46.207408  168.541883        5       13
        8     101100           296  primary_secondary -34.994245  173.463766        5       19
        9     106600          1728          secondary -35.713358  174.318881       14       19
        .....

    Returns:
        dict: The dict contains the school information
    """
    data = _read_raw_schools(school_data_path)

    distances = cdist(
        data[["latitude", "longitude"]],
        sa2_loc[["latitude", "longitude"]],
        lambda x, y: haversine_distance(x[0], x[1], y[0], y[1]),
    )

    # Find the nearest location in A for each point in B
    nearest_indices = argmin(distances, axis=1)
    data["area"] = sa2_loc["area"].iloc[nearest_indices].values

    data["max_students"] = data["estimated_occupancy"] * max_to_cur_occupancy_ratio

    data["max_students"] = data["max_students"].astype(int)

    data = data[
        [
            "area",
            "max_students",
            "sector",
            "latitude",
            "longitude",
            "age_min",
            "age_max",
        ]
    ]

    # make sure columns are in integer
    for proc_key in ["area", "max_students", "age_min", "age_max"]:
        data[proc_key] = data[proc_key].astype(int)

    # make sure columns are in float
    for proc_key in ["latitude", "longitude"]:
        data[proc_key] = data[proc_key].astype(float)

    return data


def create_hospital(
    hospital_data_path: str,
    sa2_loc: DataFrame,
) -> DataFrame:
    """Write hospital locations

    The output looks like:
            area   latitude   longitude  beds
    2     100800 -35.119186  173.260926    32
    4     350400 -45.858787  170.473064    90
    5     229800 -40.337130  175.616683    11
    6     233300 -40.211906  176.098154    11
    7     125500 -36.779884  174.756511    35
    ...      ...        ...         ...   ...

    Args:
        workdir (str): Working directory
        hospital_locations_cfg (dict): Hospital location configuration
    """
    data = _read_raw_hospital(hospital_data_path)

    distances = cdist(
        data[["latitude", "longitude"]],
        sa2_loc[["latitude", "longitude"]],
        lambda x, y: haversine_distance(x[0], x[1], y[0], y[1]),
    )

    # Find the nearest location in A for each point in B
    nearest_indices = argmin(distances, axis=1)
    data["area"] = sa2_loc["area"].iloc[nearest_indices].values

    data.drop(columns=["source_facility_id"], inplace=True)

    data = data.rename(
        columns={
            "estimated_occupancy": "beds",
        }
    )
    data.dropna(inplace=True)
    data[["beds", "area"]] = data[["beds", "area"]].astype(int)

    return data[["area", "latitude", "longitude", "beds"]]
