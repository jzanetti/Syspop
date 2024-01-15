import os
import time
from pickle import load as pickle_load
from random import randint, uniform

import folium
import imageio
import matplotlib.pyplot as plt
import networkx as nx
import osmnx as ox
from numpy import isnan as numpy_isnan
from pandas import read_csv as pandas_read_csv
from PIL import Image

ox.config(use_cache=True, log_console=True)


def read_data(
    sypop_base_path: str,
    sypop_address_path: str,
    area_ids: list,
    total_people: int = 10,
):
    synpop_data = pandas_read_csv(sypop_base_path)
    synpop_address = pandas_read_csv(sypop_address_path)

    starts = []
    ends = []
    synpop_data = synpop_data[synpop_data["area"].isin(area_ids)]

    synpop_data = synpop_data[~synpop_data["company"].isna()]

    synpop_data = synpop_data.sample(total_people)

    for i, proc_agent in synpop_data.iterrows():
        proc_agent_loc1 = proc_agent["household"]

        proc_agent_loc2 = proc_agent["company"]

        proc_agent_loc1_address = (
            synpop_address[synpop_address["name"] == proc_agent_loc1][
                ["latitude", "longitude"]
            ]
            .drop_duplicates()
            .sample(1)
        )

        proc_agent_loc2_address = (
            synpop_address[synpop_address["name"] == proc_agent_loc2][
                ["latitude", "longitude"]
            ]
            .drop_duplicates()
            .sample(1)
        )

        starts.append(
            (
                proc_agent_loc1_address.iloc[0]["latitude"],
                proc_agent_loc1_address.iloc[0]["longitude"],
            )
        )

        ends.append(
            (
                proc_agent_loc2_address.iloc[0]["latitude"],
                proc_agent_loc2_address.iloc[0]["longitude"],
            )
        )

    return starts, ends


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


def create_routes(
    G, starts, ends, total_frames, speed_km_h_range={"min": 150.0, "max": 300.0}
):
    def _random_color():
        # Generate random values for red, green, and blue components
        red = randint(0, 255)
        green = randint(0, 255)
        blue = randint(0, 255)

        # Convert RGB values to HEX
        hex_color = "#{:02x}{:02x}{:02x}".format(red, green, blue)

        # Return the random color as a tuple (red, green, blue)
        return hex_color

    total_data = len(starts)

    routes = {"routes": [], "speed": [], "color": [], "start_frame": []}
    for i in range(total_data):
        start = starts[i]
        end = ends[i]

        # calculate shortest path minimizing travel time
        orig = ox.nearest_nodes(G, start[1], start[0])
        dest = ox.nearest_nodes(G, end[1], end[0])

        try:
            route = nx.shortest_path(G, orig, dest, weight="length")

            routes["routes"].append(route)
            routes["speed"].append(
                uniform(speed_km_h_range["min"], speed_km_h_range["max"]) * 1000 / 3600
            )
            routes["color"].append(_random_color())
            routes["start_frame"].append(0)
            # routes["start_frame"].append(randint(0, int(total_frames / 2)))
        except nx.exception.NetworkXNoPath:
            continue

    return routes


def create_frame(G, routes, total_frames):
    images = []
    last_frame = {}
    for index, i in enumerate(list(range(total_frames))):
        # Calculate how far along the route the agent should be

        for j in range(len(routes["routes"])):
            print(f"processing {index} / {total_frames}: {j}")

            route = routes["routes"][j]
            if i == 0:
                speed_m_s = 0
            else:
                speed_m_s = routes["speed"][j]
            route_color = routes["color"][j]
            start_frame = routes["start_frame"][j]

            if start_frame > index:
                continue

            distance = i * speed_m_s

            # Find the edge where the agent is currently located
            current_edge = None
            total_length = 0
            for u, v in zip(route[:-1], route[1:]):
                edge_length = G[u][v][0]["length"]
                if total_length + edge_length > distance:
                    current_edge = (u, v)
                    last_frame[j] = {"u": u, "v": v}
                    break
                total_length += edge_length

            # If the agent has reached the end of the route, break the loop
            if current_edge is None:
                current_edge = (last_frame[j]["u"], last_frame[j]["v"])

            # Draw the graph
            if j == 0:
                fig, ax = ox.plot_graph(
                    G,
                    node_size=0,
                    show=False,
                    close=False,
                    node_color="w",
                    bgcolor="#f5f2f2",
                )

            # Draw the route
            # ox.plot_graph_route(G, route, route_color="r", route_linewidth=1, ax=ax)
            # Draw the agent
            print(
                f"{index}, {G.nodes[current_edge[0]]['x']}, {G.nodes[current_edge[0]]['y']}"
            )
            ax.scatter(
                G.nodes[current_edge[0]]["x"],
                G.nodes[current_edge[0]]["y"],
                c=route_color,
            )
        # ax.set_title(f"t = {i}")

        # Save the figure to a file
        filename = f"frame_{i}.png"
        fig.savefig(filename)

        # Add the filename to the list of images
        images.append(filename)

        # Close the figure
        plt.close(fig)

    return images


def write_gifs(images: list):
    # Create a GIF from the images
    with imageio.get_writer("agent.gif", mode="I", loop=0, duration=3.0) as writer:
        for filename in images:
            image = imageio.imread(filename)
            writer.append_data(image)

    # Delete the image files
    for filename in images:
        os.remove(filename)


if __name__ == "__main__":
    sypop_base_path = "/tmp/syspop_test/Wellington/syspop_base.csv"
    sypop_address_path = "/tmp/syspop_test/Wellington/syspop_location.csv"
    total_frames = 40
    starts, ends = read_data(
        sypop_base_path, sypop_address_path, [251400], total_people=30
    )
    domain = get_domain({"starts": starts, "ends": ends})
    G = create_geo_object(domain)
    routes = create_routes(G, starts, ends, total_frames)
    images = create_frame(G, routes, total_frames)
    write_gifs(images)
