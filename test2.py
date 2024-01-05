import osmnx as ox
import folium
import imageio
import time
from PIL import Image
import os
import networkx as nx
from pickle import load as pickle_load
import matplotlib.pyplot as plt

ox.config(use_cache=True, log_console=True)

with open("/tmp/synpop.pickle", "rb") as fid:
    synpop_data = pickle_load(fid)


# start = (-41.1526481, 175.0101646) # 54 field street
# end = (-41.1493786, 175.0097351) # 3 field street

plot_png = False
plot_gif = True

start = (-36.2626, 174.53747)
end = (-36.30278, 174.7824)

north = min([start[0], end[0]]) + 0.01
south = max([start[0], end[0]]) - 0.01

west = min([start[1], end[1]]) - 0.01
east = max([start[1], end[1]]) + 0.01

# Get the graph within the bounding box
G = ox.graph_from_bbox(north, south, east, west, network_type='all')

# impute missing edge speed and add travel times
G = ox.add_edge_speeds(G)
G = ox.add_edge_travel_times(G)

# calculate shortest path minimizing travel time
orig = ox.nearest_nodes(G, start[1], start[0])
dest = ox.nearest_nodes(G, end[1], end[0])
route = nx.shortest_path(G, orig, dest, weight='length')

if plot_png:

    fig, ax = ox.plot_graph_route(G, route, route_color='r', route_linewidth=6, node_size=6)
    fig.savefig("test2.png")

if plot_gif:
    # Create a list to store each frame of the GIF
    images = []

    # Define the speed of the agent (in km/h)
    speed = 30

    # Convert speed to m/s (since the graph's edge lengths are in meters)
    speed_m_s = speed * 1000 / 3600

    # Calculate the total length of the route (in meters)
    route_length = sum(ox.utils_graph.get_route_edge_attributes(G, route, 'length'))

    # Calculate the total time to traverse the route (in seconds)
    time_seconds = route_length / speed_m_s

    # Calculate the number of frames needed for the GIF (one frame per second)
    num_frames = int(time_seconds)

    # For each frame...
    for index, i in enumerate(list(range(num_frames))):
        # Calculate how far along the route the agent should be

        print(f"processing {index} / {num_frames}")

        distance = i * speed_m_s

        # Find the edge where the agent is currently located
        current_edge = None
        total_length = 0
        for u, v in zip(route[:-1], route[1:]):
            edge_length = G[u][v][0]['length']
            if total_length + edge_length > distance:
                current_edge = (u, v)
                break
            total_length += edge_length

        # If the agent has reached the end of the route, break the loop
        if current_edge is None:
            break

        # Draw the graph
        fig, ax = ox.plot_graph(G, node_size=0, show=False, close=False)

        # Draw the route
        ox.plot_graph_route(G, route, route_color='r', route_linewidth=6, ax=ax)

        # Draw the agent
        x = G.nodes[current_edge[0]]['x']
        y = G.nodes[current_edge[0]]['y']
        ax.scatter(x, y, c='blue')

        # Save the figure to a file
        filename = f'frame_{i}.png'
        fig.savefig(filename)

        # Add the filename to the list of images
        images.append(filename)

        # Close the figure
        plt.close(fig)

    # Create a GIF from the images
    with imageio.get_writer('agent.gif', mode='I') as writer:
        for filename in images:
            image = imageio.imread(filename)
            writer.append_data(image)

    # Delete the image files
    for filename in images:
        os.remove(filename)
