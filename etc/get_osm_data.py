
from OSMPythonTools.nominatim import Nominatim
from OSMPythonTools.overpass import overpassQueryBuilder, Overpass
from pandas import DataFrame
from os.path import join
from numpy import NaN

overpass = Overpass()
nominatim = Nominatim()


def query_results(query_keys: dict, region: str, country: str, output_dir: str):
    """Query data from OSM

    Args:
        query_keys (dict): Query keys such as {"amenity": ["restaurant"]}, where
           amenity is the key, and restaurant is the value, the details can be found
           https://wiki.openstreetmap.org/wiki/Map_features
        region (str): region name such as Auckland
        country (str): country name such as New Zealand
        output_dir (str): Output directory
    """
    if region is not NaN:
        areaId = nominatim.query(f"{region},{country}").areaId()
    else:
        areaId = nominatim.query(f"{country}").areaId()

    for proc_key in query_keys:
        
        for proc_value in query_keys[proc_key]:
            query = overpassQueryBuilder(area=areaId, elementType="node", selector=f'"{proc_key}"="{proc_value}"', out="body")
            result = overpass.query(query)

            outputs = {"name": [], "lat": [], "lon": []}
            for i, node in enumerate(result.nodes()):

                outputs["name"].append(f"{proc_value}_{i}")
                outputs["lat"].append(node.lat())
                outputs["lon"].append(node.lon())

                print(f"{node.lat()}, {node.lon()}")

            df = DataFrame.from_dict(outputs)
            df.to_csv(join(output_dir, f"{proc_key}_{proc_value}.csv"), index=False)

if __name__ == "__main__":
    region = NaN # can be NaN, or sth like Auckland
    country = "New Zealand"
    query_keys = {"amenity": ["restaurant"], "shop": ["supermarket"]}
    output_dir = "etc/data/raw_nz"

    query_results(query_keys, region, country, output_dir)