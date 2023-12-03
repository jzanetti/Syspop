import osmnx as ox
import folium
import imageio
import time
from PIL import Image
import os
import networkx as nx
ox.config(use_cache=True, log_console=True)

start = (-41.1526481, 175.0101646) # 54 field street
end = (-41.1493786, 175.0097351) # 3 field street

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

fig, ax = ox.plot_graph_route(G, route, route_color='r', route_linewidth=6, node_size=6)
fig.savefig("test2.png")
