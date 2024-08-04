import os
import time
from argparse import ArgumentParser
from copy import deepcopy
from os import makedirs
from os.path import exists, join
from pickle import dump as pickle_dump
from pickle import load as pickle_load
from random import randint
from random import uniform
from random import uniform as random_uniform
from uuid import uuid4

import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
import osmnx as ox
from networkx.classes.multidigraph import MultiDiGraph
from numpy import isnan as numpy_isnan
from pandas import DataFrame
from pandas import read_parquet as pandas_read_parquet
from PIL import Image
from scipy.interpolate import interp1d
from shapely.geometry import LineString

ox.config(use_cache=True, log_console=True)


def get_domain_range(df: DataFrame) -> dict:
    # Remove the 'id' column
    df = df.drop(columns=["id"])

    # Flatten the DataFrame to a Series
    s = df.values.flatten()

    # Convert tuples to a DataFrame
    df = DataFrame(s, columns=["coordinates"])

    # Split the coordinates column into lat and lon
    df[["lat", "lon"]] = DataFrame(df["coordinates"].tolist(), index=df.index)

    # Get the min and max for lat and lon
    min_lat, max_lat = df["lat"].min(), df["lat"].max()
    min_lon, max_lon = df["lon"].min(), df["lon"].max()

    return {
        "south": min_lat - 0.1,
        "north": max_lat + 0.1,
        "west": min_lon - 0.1,
        "east": max_lon + 0.1,
    }


def interpolate_coordinates(latlon: list, frames: int):

    # Separate the lat and lon into two lists
    lat = [x[0] for x in latlon]
    lon = [x[1] for x in latlon]

    # Create the interpolation function
    f_lat = interp1d(np.arange(len(lat)), lat, kind="cubic")
    f_lon = interp1d(np.arange(len(lon)), lon, kind="cubic")

    # Create the new indices for interpolation
    new_indices = np.linspace(0, len(lat) - 1, frames)

    # Interpolate the lat and lon
    new_lat = f_lat(new_indices)
    new_lon = f_lon(new_indices)

    # Combine the interpolated lat and lon into a list of tuples
    new_lat_lon = list(zip(new_lat, new_lon))

    # Print the interpolated lat and lon
    return new_lat_lon


def read_data(
    sypop_base_path: str,
    sypop_address_path: str,
    syspop_diaries_path: str,
    area_id: list,
    people_ids: int,
):
    synpop_data = pandas_read_parquet(sypop_base_path)
    synpop_address = pandas_read_parquet(sypop_address_path)
    syspop_diaries = pandas_read_parquet(syspop_diaries_path)

    synpop_data = synpop_data[synpop_data["area"] == int(area_id)]
    people_ids = [int(item) for item in people_ids]
    synpop_data = synpop_data[synpop_data["id"].isin(people_ids)]

    all_hours = [int(item) for item in list(syspop_diaries.hour.unique())]

    latlon_data = {}
    for proc_hr in all_hours:
        latlon_data[proc_hr] = []
    latlon_data["id"] = []

    for _, proc_agent in synpop_data.iterrows():

        proc_diary = syspop_diaries[syspop_diaries["id"] == proc_agent.id]
        # proc_diary = syspop_diaries[syspop_diaries["id"] == proc_agent.id]

        latlon_data["id"].append(proc_agent.id)

        for proc_hr in all_hours:

            proc_location = proc_diary[proc_diary.hour == str(proc_hr)][
                "location"
            ].values[0]

            if proc_location is not None:
                proc_address = synpop_address[synpop_address["name"] == proc_location]

                proc_latlon = (
                    proc_address.latitude.values[0],
                    proc_address.longitude.values[0],
                )
            else:
                proc_latlon = (
                    proc_address.latitude.values[0] + random_uniform(-0.03, 0.03),
                    proc_address.longitude.values[0] + random_uniform(-0.03, 0.03),
                )
                # proc_lat += random_uniform(-0.03, 0.03)
                # proc_lon += random_uniform(-0.03, 0.03)

            latlon_data[proc_hr].append(proc_latlon)

    return DataFrame.from_dict(latlon_data)


def create_geo_object(domain: dict):
    G = ox.graph_from_bbox(
        domain["north"],
        domain["south"],
        domain["east"],
        domain["west"],
        network_type="all",
    )

    # impute missing edge speed and add travel times
    G = ox.add_edge_speeds(G)
    G = ox.add_edge_travel_times(G)

    return G


def create_routes(
    G: MultiDiGraph,
    hourly_data: DataFrame,
    frames=60,
):

    all_hours = list(hourly_data.columns)
    all_hours.remove("id")

    routes_data = {}
    for _, proc_agent in hourly_data.iterrows():

        routes_data[proc_agent.id] = {}

        for proc_hr in all_hours:
            proc_hr_start = proc_hr
            proc_hr_end = proc_hr + 1

            if proc_hr_end > 24:
                continue

            if proc_hr_end == 24:
                proc_hr_end = 0

            start = proc_agent[proc_hr_start]
            end = proc_agent[proc_hr_end]

            # calculate shortest path minimizing travel time
            orig = ox.nearest_nodes(G, start[1], start[0])
            dest = ox.nearest_nodes(G, end[1], end[0])

            routes = {}
            try:
                if orig == dest:
                    latlon = [(G.nodes[orig]["y"], G.nodes[orig]["x"])]
                    length = 0
                else:
                    route = nx.shortest_path(G, orig, dest, weight="length")
                    length = nx.shortest_path_length(G, source=orig, target=dest)

                    latlon = []
                    for proc_node in route:
                        latlon.append(
                            (G.nodes[proc_node]["y"], G.nodes[proc_node]["x"]),
                        )
                    try:
                        latlon = interpolate_coordinates(latlon, frames)
                    except ValueError:
                        latlon = None

            except nx.exception.NetworkXNoPath:
                continue

            routes["routes"] = latlon
            routes["length"] = length

            routes_data[proc_agent.id][proc_hr] = routes

    return routes_data


def main(
    workdir: str,
    area_id: str,
    people_ids: list,
    sypop_base_path: str,
    sypop_address_path: str,
    syspop_diaries_path: str,
):

    if not os.path.exists(workdir):
        makedirs(workdir)

    hourly_data = read_data(
        sypop_base_path,
        sypop_address_path,
        syspop_diaries_path,
        area_id=area_id,
        people_ids=people_ids,
    )

    domain = get_domain_range(hourly_data)
    G = create_geo_object(domain)
    routes = create_routes(G, hourly_data)

    pickle_dump(routes, open(join(workdir, f"routes_{str(uuid4())[:6]}.pickle"), "wb"))


if __name__ == "__main__":
    parser = ArgumentParser(description="Creating NZ data")

    parser.add_argument(
        "--workdir",
        type=str,
        required=False,
        default="/tmp/agents_movement",
        help="Working directory",
    )

    parser.add_argument(
        "--area_id",
        type=str,
        required=False,
        default="251400",
        help="Area ID",
    )

    parser.add_argument(
        "--people_ids", nargs="+", help="People IDs in a list", required=True
    )

    parser.add_argument(
        "--sypop_base_path",
        type=str,
        required=True,
        default="0",
        help="Base syspop path",
    )

    parser.add_argument(
        "--sypop_address_path",
        type=str,
        required=True,
        default="0",
        help="Syspop address path",
    )

    parser.add_argument(
        "--syspop_diaries_path",
        type=str,
        required=True,
        default="0",
        help="Syspop diary path",
    )

    args = parser.parse_args()
    """
    args = parser.parse_args(
        [
            "--workdir",
            "/tmp/agents_movement",
            "--area_id",
            "251400",
            "--people_ids",
            "3479584",
            "3479586",
            "--sypop_base_path",
            "/DSC/digital_twin/abm/PHA_report_202405/syspop/NZ/2023/median/syspop_base.parquet",
            "--sypop_address_path",
            "/DSC/digital_twin/abm/PHA_report_202405/syspop/NZ/2023/median/syspop_location.parquet",
            "--syspop_diaries_path",
            "/DSC/digital_twin/abm/PHA_report_202405/syspop/NZ/2023/median/syspop_diaries.parquet",
        ]
    )
    """

    main(
        args.workdir,
        args.area_id,
        args.people_ids,
        args.sypop_base_path,
        args.sypop_address_path,
        args.syspop_diaries_path,
    )
