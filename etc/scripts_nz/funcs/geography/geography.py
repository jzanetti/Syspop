from copy import deepcopy
from math import ceil as math_ceil
from os.path import join
from re import match as re_match

from numpy import inf, nan
from pandas import DataFrame, concat, melt, merge, read_csv, read_excel, to_numeric

from funcs import RAW_DATA, REGION_CODES, REGION_NAMES_CONVERSIONS


def create_geography_location_super_area(geography_hierarchy_data: DataFrame):
    data = read_csv(RAW_DATA["geography"]["geography_location"])

    data = data[["SA22018_V1_00", "LATITUDE", "LONGITUDE"]]

    data = data.rename(
        columns={
            "SA22018_V1_00": "area",
            "LATITUDE": "latitude",
            "LONGITUDE": "longitude",
        }
    )

    data = merge(data, geography_hierarchy_data, on="area", how="inner")

    data = data.groupby("super_area")[["latitude", "longitude"]].mean().reset_index()

    return data


def create_geography_location_area():
    """Write area location data

    Args:
        workdir (str): Working directory
        area_location_cfg (dict): Area location configuration
    """
    data = read_csv(RAW_DATA["geography"]["geography_location"])

    data = data[["SA22018_V1_00", "LATITUDE", "LONGITUDE"]]

    data = data.rename(
        columns={
            "SA22018_V1_00": "area",
            "LATITUDE": "latitude",
            "LONGITUDE": "longitude",
        }
    )

    return data


def create_geography_name_super_area() -> dict:
    """Write super area names

    Args:
        workdir (str): Working directory
        use_sa3_as_super_area (bool): Use SA3 as super area, otherwise we will use regions
        geography_hierarchy_definition_cfg (dict or None): Geography hierarchy definition configuration
    """

    data = {"super_area": [], "city": []}

    data = read_csv(RAW_DATA["geography"]["geography_hierarchy"])
    data = data[["SA32023_code", "SA32023_name"]]
    data = data.rename(columns={"SA32023_code": "super_area", "SA32023_name": "city"})
    data = data.drop_duplicates()

    return data


def create_geography_hierarchy():
    """Create geography

    Args:
        workdir (str): _description_
    """

    def _map_codes2(code: str) -> list:
        """Create a mapping function

        Args:
            code (str): Regional code to be mapped

        Returns:
            list: The list contains north and south island
        """
        for key, values in REGION_NAMES_CONVERSIONS.items():
            if code == key:
                return values
        return None

    data = read_csv(RAW_DATA["geography"]["geography_hierarchy"])

    data = data[["REGC2023_code", "SA32023_code", "SA32023_name", "SA22018_code"]]

    data = data[~data["REGC2023_code"].isin(REGION_CODES["Others"])]

    data["REGC2023_name"] = data["REGC2023_code"].map(_map_codes2)

    data = data.rename(
        columns={
            "REGC2023_name": "region",
            "SA32023_code": "super_area",
            "SA22018_code": "area",
            "SA32023_name": "super_area_name",
        }
    ).drop_duplicates()

    data = data[["region", "super_area", "area", "super_area_name"]]

    data = data[~data["area"].duplicated(keep=False)]

    return data
