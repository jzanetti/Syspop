import os
import time
from argparse import ArgumentParser
from os import makedirs
from os.path import exists, join
from pickle import dump as pickle_dump
from pickle import load as pickle_load
from random import randint
from random import uniform
from random import uniform as random_uniform

import folium
import imageio
import matplotlib.pyplot as plt
import networkx as nx
import osmnx as ox
from networkx.classes.multidigraph import MultiDiGraph
from numpy import isnan as numpy_isnan
from pandas import DataFrame
from pandas import read_parquet as pandas_read_parquet
from PIL import Image
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


def read_data(
    sypop_base_path: str,
    sypop_address_path: str,
    syspop_work_and_school_path: str,
    syspop_travel_path: str,
    syspop_household_path: str,
    syspop_diaries_path: str,
    area_ids: list = [100100],
    total_people: int or None = 10 or None,
    people_loc: int or None = None,
    venues: list = ["household", "company", "school"],
):
    synpop_data = pandas_read_parquet(sypop_base_path)
    synpop_address = pandas_read_parquet(sypop_address_path)
    # syspop_work_and_school = pandas_read_parquet(syspop_work_and_school_path)
    # syspop_travel = pandas_read_parquet(syspop_travel_path)
    # syspop_household = pandas_read_parquet(syspop_household_path)
    syspop_diaries = pandas_read_parquet(syspop_diaries_path)

    synpop_data = synpop_data[synpop_data["area"].isin(area_ids)]
    if total_people is not None:
        synpop_data = synpop_data.sample(total_people)
    if people_loc is not None:
        synpop_data = synpop_data.iloc[[people_loc]]

    syspop_diaries = syspop_diaries[syspop_diaries["id"] == synpop_data.id.values[0]]

    all_hours = [int(item) for item in list(syspop_diaries.hour.unique())]

    latlon_data = {}
    for proc_hr in all_hours:
        latlon_data[proc_hr] = []
    latlon_data["id"] = []

    for _, proc_agent in synpop_data.iterrows():

        proc_diary = syspop_diaries[syspop_diaries["id"] == proc_agent.id]

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


def get_domain(address_data: dict):
    # Extract latitudes and longitudes into separate lists

    latitudes = []
    longitudes = []
    for proc_key in ["starts"]:
        latitudes.extend([lat for lat, lon in address_data[proc_key]])
        longitudes.extend([lon for lat, lon in address_data[proc_key]])

    # Find min and max values
    south = min(latitudes) - 0.01
    north = max(latitudes) + 0.01
    west = min(longitudes) - 0.01
    east = max(longitudes) + 0.01
    return {"south": south, "north": north, "west": west, "east": east}


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


def create_routes2(
    G: MultiDiGraph,
    hourly_data: DataFrame,
    frames=12,
):

    all_hours = list(hourly_data.columns)
    all_hours.remove("id")

    routes_data = {}
    for proc_hr in all_hours:
        routes_data[proc_hr] = []
    routes_data["id"] = []

    for _, proc_agent in hourly_data.iterrows():
        routes_data["id"].append(proc_agent.id)
        # proc_speed = (speed_km_h_range * 1000.0 / 3600.0) * (
        #    output_interval_min * 60.0
        # )  # m/output_interval_min
        # proc_speed = 3.0
        for proc_hr in all_hours:
            proc_hr_start = proc_hr
            proc_hr_end = proc_hr + 1

            if proc_hr_end > 23:
                continue

            start = proc_agent[proc_hr_start]
            end = proc_agent[proc_hr_end]

            # calculate shortest path minimizing travel time
            orig = ox.nearest_nodes(G, start[1], start[0])
            dest = ox.nearest_nodes(G, end[1], end[0])

            routes = {}
            try:
                if orig == dest:
                    latlon = None
                    length = 0
                else:
                    route = nx.shortest_path(G, orig, dest, weight="length")
                    length = nx.shortest_path_length(G, source=orig, target=dest)

                    latlon = []
                    for proc_node in route:
                        latlon.append(
                            (G.nodes[proc_node]["y"], G.nodes[proc_node]["x"]),
                        )
                    latlon = interpolate_coordinates2(latlon, frames)

                routes["routes"] = latlon
                routes["length"] = length
            except nx.exception.NetworkXNoPath:
                continue

            routes_data[proc_hr] = routes
    print(routes_data)
    return routes_data


def interpolate_coordinates2(latlon: list, frames: int):
    import numpy as np
    from scipy.interpolate import interp1d

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


def interpolate_coordinates(coords, dist=10):
    import geopy.distance
    import numpy as np

    # Initialize the list with the first coordinate
    res = [coords[0]]
    for i in range(len(coords) - 1):
        # Get the distance between the two points
        distance = geopy.distance.distance(coords[i], coords[i + 1]).m
        # Calculate the number of points to be interpolated
        no_points = int(distance // dist)
        if no_points > 1:
            # Calculate the lat and lon difference between the two points
            lat_diff = coords[i + 1][0] - coords[i][0]
            lon_diff = coords[i + 1][1] - coords[i][1]
            # Interpolate the lat and lon
            for j in range(1, no_points):
                lat = coords[i][0] + j * lat_diff / no_points
                lon = coords[i][1] + j * lon_diff / no_points
                res.append((lat, lon))
        # Add the next coordinate
        res.append(coords[i + 1])
    return res


def plot_map(XX):
    import os

    import geopandas as gpd
    import geoplot as gplt
    import geoplot.crs as gcrs
    import imageio
    import matplotlib.pyplot as plt
    import pandas as pd

    # Assuming XX is your MultiDiGraph
    # Extract lat and lon from the graph
    lat_lon = [
        (data["y"], data["x"])
        for node, data in XX.nodes(data=True)
        if "x" in data and "y" in data
    ]

    # Convert the list of tuples into a DataFrame
    df = pd.DataFrame(lat_lon, columns=["Latitude", "Longitude"])

    # Convert the DataFrame into a GeoDataFrame
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.Longitude, df.Latitude))

    # Set the plot limits to the extent of the points
    minx, miny, maxx, maxy = gdf.geometry.total_bounds

    # Define the world map
    world = gpd.read_file(gpd.datasets.get_path("naturalearth_lowres"))

    # Create a directory to store the frames
    os.makedirs("frames", exist_ok=True)

    average_lat = gdf.geometry.y.mean()
    average_lon = gdf.geometry.x.mean()

    # Create a new Leaflet map
    m = folium.Map(location=[average_lat, average_lon], zoom_start=13)

    for i, point in enumerate(gdf.geometry):
        # Add the point to the map
        folium.Marker([point.y, point.x]).add_to(m)

    # Save the map to an HTML file
    m.save("map.html")
    raise Exception("!23123")

    # Create a GIF
    with imageio.get_writer("map.gif", mode="I") as writer:
        for i, point in enumerate(gdf.geometry):
            # Plot the world
            fig, ax = plt.subplots(figsize=(10, 10))
            world.plot(ax=ax, color="white", edgecolor="black")

            # Plot the points
            gdf.iloc[: i + 1].plot(ax=ax, color="red")

            ax.set_xlim(minx, maxx)
            ax.set_ylim(miny, maxy)

            # Save the figure as a frame
            plt.savefig(f"frames/frame_{i}.png")

            # Add the frame to the GIF
            writer.append_data(imageio.imread(f"frames/frame_{i}.png"))

            # Close the figure
            plt.close()

    # Print a success message
    print("The GIF map is successfully created and saved as map.gif")


def create_frame(G, routes, total_frames, speed=5.0):
    images = []
    last_frame = {}

    all_hours = list(routes.keys())
    all_hours.remove("id")

    agents_routes = {}

    for i_agent in range(len(routes["id"])):  # number of agents

        agents_routes[i_agent] = {}

        for proc_hr in all_hours:

            agents_routes[i_agent][proc_hr] = []

            proc_routes = routes[proc_hr]

            distance = 0  # Start at the beginning of the line

            if proc_routes is not None:
                agents_routes[i_agent][proc_hr].append(
                    (G.nodes[proc_routes[0]]["y"], G.nodes[proc_routes[0]]["x"])
                )
            else:
                plot_map(G)
                raise Exception("123123")
                # for node, data in G.nodes(data=True):
                #    lat = data.get("y")
                #    lon = data.get("x")
                #    print(f"Node: {node}, Latitude: {lat}, Longitude: {lon}")

                for i_leg in range(len(proc_routes) - 1):

                    G_leg = G[proc_routes[i_leg]][proc_routes[i_leg + 1]]

                    for i_g in range(len(G_leg)):
                        proc_G = G_leg[i_g]

                        if "geometry" not in proc_G:
                            break

                        proc_routes_leg_geometry = proc_G["geometry"]
                        proc_routes_leg_length = proc_G["length"]

                        distance = 0.0
                        while distance <= proc_routes_leg_length:
                            point = proc_routes_leg_geometry.interpolate(distance)
                            agents_routes[i_agent][proc_hr].append((point.x, point.y))
                            distance += speed

    return agents_routes


def write_gifs(images: list):
    # Create a GIF from the images
    with imageio.get_writer("agent.gif", mode="I", loop=0, duration=3.0) as writer:
        for filename in images:
            image = imageio.imread(filename)
            writer.append_data(image)

    # Delete the image files
    for filename in images:
        os.remove(filename)


def main(
    workdir,
    people_loc=3,
    area_ids: list = [251400],
    sypop_base_path="/DSC/digital_twin/abm/PHA_report_202405/syspop/NZ/2023/median/syspop_base.parquet",
    sypop_address_path="/DSC/digital_twin/abm/PHA_report_202405/syspop/NZ/2023/median/syspop_location.parquet",
    syspop_work_and_school_path="/DSC/digital_twin/abm/PHA_report_202405/syspop/NZ/2023/median/syspop_work_and_school.parquet",
    syspop_travel_path="/DSC/digital_twin/abm/PHA_report_202405/syspop/NZ/2023/median/syspop_travel.parquet",
    syspop_household_path="/DSC/digital_twin/abm/PHA_report_202405/syspop/NZ/2023/median/syspop_household.parquet",
    syspop_diaries_path="/DSC/digital_twin/abm/PHA_report_202405/syspop/NZ/2023/median/syspop_diaries.parquet",
):

    if not os.path.exists(workdir):
        makedirs(workdir)

    hourly_data = read_data(
        sypop_base_path,
        sypop_address_path,
        syspop_work_and_school_path,
        syspop_travel_path,
        syspop_household_path,
        syspop_diaries_path,
        area_ids=area_ids,
        total_people=None,
        people_loc=int(people_loc),
    )

    domain = get_domain_range(hourly_data)

    # domain = get_domain({"starts": starts, "ends": ends})
    G = create_geo_object(domain)

    # from pickle import load as pickle_load

    # geo_data = {"G": G, "hourly_data": hourly_data}
    # pickle_dump(geo_data, open("geo_data.p", "wb"))

    # geo_data = pickle_load(open("geo_data.p", "rb"))
    routes = create_routes2(G, hourly_data)

    pickle_dump(routes, open(join(workdir, f"routes_{people_loc}.pickle"), "wb"))


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
        "--people_loc",
        type=str,
        required=False,
        default="0",
        help="People location",
    )

    args = parser.parse_args()  # ["--workdir", "etc/data/test_data_wellington_latest"]

    main(args.workdir, args.people_loc)


def test():
    sypop_base_path = "/DSC/digital_twin/abm/PHA_report_202405/syspop/NZ/2023/median/syspop_base.parquet"
    sypop_address_path = "/DSC/digital_twin/abm/PHA_report_202405/syspop/NZ/2023/median/syspop_location.parquet"
    syspop_work_and_school_path = "/DSC/digital_twin/abm/PHA_report_202405/syspop/NZ/2023/median/syspop_work_and_school.parquet"
    syspop_travel_path = "/DSC/digital_twin/abm/PHA_report_202405/syspop/NZ/2023/median/syspop_travel.parquet"
    syspop_household_path = "/DSC/digital_twin/abm/PHA_report_202405/syspop/NZ/2023/median/syspop_household.parquet"
    syspop_diaries_path = "/DSC/digital_twin/abm/PHA_report_202405/syspop/NZ/2023/median/syspop_diaries.parquet"
    total_frames = 40

    hourly_data = read_data(
        sypop_base_path,
        sypop_address_path,
        syspop_work_and_school_path,
        syspop_travel_path,
        syspop_household_path,
        syspop_diaries_path,
        area_ids=[251400],
        total_people=1,
    )
    """
    domain = get_domain_range(hourly_data)

    # domain = get_domain({"starts": starts, "ends": ends})
    G = create_geo_object(domain)
    from pickle import dump as pickle_dump
    from pickle import load as pickle_load

    geo_data = {"G": G, "hourly_data": hourly_data}
    pickle_dump(geo_data, open("geo_data.p", "wb"))
    """
    # geo_data = pickle_load(open("geo_data.p", "rb"))
    # routes = create_routes2(geo_data["G"], geo_data["hourly_data"])
    # images = create_frame(geo_data["G"], routes, total_frames)
    # write_gifs(images)
