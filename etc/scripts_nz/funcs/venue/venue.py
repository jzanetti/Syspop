from os import makedirs
from os.path import dirname, exists, join
from time import sleep

import overpy
from geopy.geocoders import Nominatim
from numpy import argmin
from pandas import DataFrame, merge, read_csv
from scipy.spatial.distance import cdist

from funcs import RAW_DATA, RAW_DATA_INFO, REGION_NAMES_CONVERSIONS
from funcs.utils import get_central_point, haversine_distance


def create_shared_space(space_name: str, geography_location: DataFrame):
    """Write shared space

    Args:
        workdir (str): Working directory
        space_name (str), name such as supermakrts
    """
    data = read_csv(RAW_DATA["venue"][space_name])
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

def write_leisures(workdir: str):
    """Write cinema information

    Args:
        workdir (str): Working directory
    """

    def _get_data_from_osm(
        name: str,
        super_area_id: str,
        geo_name: str,
        output: dict,
        osm_query_key: dict,
        api,
        radius: int = 500.0,
        domain_range: dict = {"lat": [-50.0, -30.0], "lon": [160.0, 180.0]},
    ) -> list:
        """Query data from OSM

        Args:
            name (str): the amenity type, e.g., cinema
            geo_name (str): city or region name, e.g., Wellington
            radius (float, default: 500): Query radius in km

        Returns:
            list: the queried results from OSM
        """
        geolocator = Nominatim(user_agent="{name}_extraction")

        retry = 0

        location = None
        while retry < 5:
            try:
                location = geolocator.geocode(f"{geo_name}, New Zealand", timeout=15)
                break
            except:
                sleep(15 * (retry + 1))
                retry += 1

        if location is None:
            raise Exception(f"Not able to get geolocator with retries = {retry} ...")

        lat = location.latitude
        lon = location.longitude

        radius *= 1000.0

        proc_query_key = osm_query_key[name]

        query = "("
        for query_key in proc_query_key:
            proc_data_list = proc_query_key[query_key]

            for proc_data_name in proc_data_list:
                query += f'node["{query_key}"="{proc_data_name}"](around:{radius},{lat},{lon});'

        query = query + "); \nout;"

        tried = 0
        while True:
            try:
                result = api.query(query)
                break
            except:
                if tried > 3:
                    raise Exception("Tried too many times, give up ...")
                sleep(10)
                tried += 1

        for node in result.nodes:
            node_lat = float(node.lat)
            node_lon = float(node.lon)

            if (
                node_lat > domain_range["lat"][1]
                or node_lat < domain_range["lat"][0]
                or node_lon < domain_range["lon"][0]
                or node_lat > domain_range["lon"][1]
            ):
                raise Exception(
                    f"Seems there is an issue for locating {name} in {geo_name}"
                )

            output["lat"].append(float(node.lat))
            output["lon"].append(float(node.lon))
            output["super_area"].append(super_area_id)

        return output

    api = overpy.Overpass()

    for proc_leisure in ["gym", "grocery", "cinema", "pub"]:
        output = {"lat": [], "lon": [], "super_area": []}
        for super_area_id in REGION_NAMES_CONVERSIONS:
            if super_area_id == 99:
                continue

            output = _get_data_from_osm(
                proc_leisure,
                super_area_id,
                REGION_NAMES_CONVERSIONS[super_area_id],
                output,
                RAW_DATA_INFO["base"]["venue"]["leisures"]["osm_query_key"],
                api,
            )

        output = DataFrame.from_dict(output)[["super_area", "lat", "lon"]]

        output.to_csv(join(workdir, f"leisure_{proc_leisure}.csv"), index=False)


def create_school(
    sa2_loc: DataFrame,
    max_to_cur_occupancy_ratio=1.2,
) -> dict:
    """Write schools information

    Args:
        workdir (str): Working directory
        school_cfg (dict): School configuration
        max_to_cur_occupancy_ratio (float, optional): In the data, we have the estimated occupancy
            for a school, while in JUNE we need the max possible occupancy. Defaults to 1.2.

    Returns:
        dict: The dict contains the school information
    """
    data = read_csv(RAW_DATA["venue"]["school"])

    data = data[data["use"] == "School"]

    data = data[
        ~data["use_type"].isin(
            [
                "Teen Parent Unit",
                "Correspondence School",
            ]
        )
    ]

    data["use_type"] = data["use_type"].map(
        RAW_DATA_INFO["base"]["venue"]["school"]["school_age_table"]
    )

    data[["sector", "age_range"]] = data["use_type"].str.split(" ", n=1, expand=True)
    data["age_range"] = data["age_range"].str.strip("()")
    data[["age_min", "age_max"]] = data["age_range"].str.split("-", expand=True)

    # data[["sector", "age_min", "age_max"]] = data["use_type"].str.extract(
    #    r"([A-Za-z\s]+)\s\((\d+)-(\d+)\)"
    # )

    data["Central Point"] = data["WKT"].apply(get_central_point)

    data["latitude"] = data["Central Point"].apply(lambda point: point.y)
    data["longitude"] = data["Central Point"].apply(lambda point: point.x)

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
    sa2_loc: DataFrame,
) -> DataFrame:
    """Write hospital locations

    Args:
        workdir (str): Working directory
        hospital_locations_cfg (dict): Hospital location configuration
    """
    data = read_csv(RAW_DATA["venue"]["hospital"])

    data = data[data["use"] == "Hospital"]

    data["Central Point"] = data["WKT"].apply(get_central_point)

    data["latitude"] = data["Central Point"].apply(lambda point: point.y)
    data["longitude"] = data["Central Point"].apply(lambda point: point.x)

    data = data[["latitude", "longitude", "estimated_occupancy", "source_facility_id"]]

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

    return data
