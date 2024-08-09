from argparse import ArgumentParser
from os import makedirs
from os.path import join
from pickle import dump as pickle_dump
from random import uniform as random_uniform
from uuid import uuid4

import networkx as nx
import numpy as np
import osmnx as ox
from networkx.classes.multidigraph import MultiDiGraph
from pandas import DataFrame
from pandas import read_parquet as pandas_read_parquet
from scipy.interpolate import interp1d

ox.config(use_cache=True, log_console=True)


def get_domain_range(df: DataFrame) -> dict:
    """Obtain domain range

    Args:
        df (DataFrame): address data frame

    Returns:
        dict: Domain range, min and max for lat/lon
    """
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


def interpolate_coordinates(latlon: list, frames: int) -> list:
    """Interpolate coordinates for lat and lon

    Args:
        latlon (list): the list of lat and lon to be interploated
        frames (int): number of frames

    Returns:
        list: the list of coordinates
    """
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
    area_ids: list,
    people_ids: int,
    pertubate_latlon: float = 0.0,
) -> DataFrame:
    """Read datasets

    Args:
        sypop_base_path (str): Basic sypop datasets
        sypop_address_path (str): Syspop address dataset path
        syspop_diaries_path (str): Syspop diary dataset path
        area_ids (list): a list of area ids
        people_ids (int): a list of people ids

    Returns:
        DataFrame: The dataframe for the processed data
    """
    synpop_data = pandas_read_parquet(sypop_base_path)
    synpop_address = pandas_read_parquet(sypop_address_path)
    syspop_diaries = pandas_read_parquet(syspop_diaries_path)

    area_ids = [int(item) for item in area_ids]
    synpop_data = synpop_data[synpop_data["area"].isin(area_ids)]
    people_ids = [int(item) for item in people_ids]
    synpop_data = synpop_data[synpop_data["id"].isin(people_ids)]

    all_hours = [int(item) for item in list(syspop_diaries.hour.unique())]
    # all_hours = [7, 8]
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

            if (proc_location is not None) and (proc_location != "nan"):
                proc_address = synpop_address[synpop_address["name"] == proc_location]

                proc_latlon = (
                    proc_address.latitude.values[0],
                    proc_address.longitude.values[0],
                )

            else:
                proc_latlon = (
                    proc_address.latitude.values[0]
                    + random_uniform(-pertubate_latlon, pertubate_latlon),
                    proc_address.longitude.values[0]
                    + random_uniform(-pertubate_latlon, pertubate_latlon),
                )

            latlon_data[proc_hr].append(proc_latlon)

    return DataFrame.from_dict(latlon_data)


def create_geo_object(domain: dict):
    """Create geo object for OSMNX

    Args:
        domain (dict): Domain size

    Returns:
        _type_: Geo object for OSMNX
    """
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
    G: MultiDiGraph, hourly_data: DataFrame, interp: bool, frames=60
) -> dict:
    """Creating routes with OSMNX

    Args:
        G (MultiDiGraph): OSMNX object
        hourly_data (DataFrame): Hourly diary data
        frames (int, optional): default number of fames. Defaults to 60.

    Returns:
        dict: Route data
    """

    all_hours = list(hourly_data.columns)
    all_hours.remove("id")
    # all_hours = [7]

    routes_data = {}
    total_agents = len(hourly_data)

    for i, proc_agent in hourly_data.iterrows():

        # if proc_agent.id != 127114:
        #    continue

        print(f"Processing {i}/{total_agents} ...")

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

                    if interp:
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
    area_ids: list,
    people_ids: list,
    sypop_base_path: str,
    sypop_address_path: str,
    syspop_diaries_path: str,
    interp: bool,
):
    """Main function for creating routes

    Args:
        workdir (str): Working directory
        area_ids (list): A list of areas to be used (SA2)
        people_ids (list): A list of people to be used (people ID from syspop)
        sypop_base_path (str): Base syspop data path
        sypop_address_path (str): Syspop address data path
        syspop_diaries_path (str): Syspop diary data path
        interp (bool): run interpolation flag
    """

    try:
        makedirs(workdir)
    except FileExistsError:
        pass

    # people_ids = people_ids[0].split(" ")

    hourly_data = read_data(
        sypop_base_path,
        sypop_address_path,
        syspop_diaries_path,
        area_ids=area_ids,
        people_ids=people_ids,
    )

    domain = get_domain_range(hourly_data)
    G = create_geo_object(domain)
    routes = create_routes(G, hourly_data, interp)

    pickle_dump(routes, open(join(workdir, f"routes_{str(uuid4())[:6]}.pickle"), "wb"))

    print("All jobs done ...")


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
        "--area_ids",
        nargs="+",
        required=True,
        help="SA2 area ID",
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

    parser.add_argument(
        "--interp",
        action="store_true",
        help="Interpolate the data to the minuite level",
    )

    args = parser.parse_args()

    """
    args = parser.parse_args(
        [
            "--workdir",
            "etc/route_model/agents_movement",
            "--area_id",
            "241800",
            "--people_ids",
            # "127114",
            # "127115",
            "127070 127071 127072 127073 127074 127075 127076 127077 127078 127079 127080 127081 127082 127083 127084 127085 127086 127087 127088 127089 127090 127091 127092 127093 127094 127095 127096 127097 127098 127099 127100 127101 127102 127103 127104 127105 127106 127107 127108 127109 127110 127111 127112 127113 127114 127115 127116 127117 127118 127119",
            "--sypop_base_path",
            "/DSC/digital_twin/abm/synthetic_population/v3.0/Wellington/syspop_base.parquet",
            "--sypop_address_path",
            "/DSC/digital_twin/abm/synthetic_population/v3.0/Wellington/syspop_location.parquet",
            "--syspop_diaries_path",
            "/DSC/digital_twin/abm/synthetic_population/v3.0/Wellington/syspop_diaries.parquet",
            "--interp",
        ]
    )
    """

    main(
        args.workdir,
        args.area_ids,
        args.people_ids,
        args.sypop_base_path,
        args.sypop_address_path,
        args.syspop_diaries_path,
        args.interp,
    )
