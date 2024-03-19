from json import dump as json_dump
from os import makedirs
from os.path import exists, join

from numpy import NaN
from OSMPythonTools.nominatim import Nominatim
from OSMPythonTools.overpass import Overpass, overpassQueryBuilder
from pandas import DataFrame

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

    actual_and_records_name_mapping = {}
    for proc_key in query_keys:

        actual_and_records_name_mapping[proc_key] = {}

        for proc_value in query_keys[proc_key]:

            actual_and_records_name_mapping[proc_key][proc_value] = {}

            query = overpassQueryBuilder(
                area=areaId,
                elementType="node",
                selector=f'"{proc_key}"="{proc_value}"',
                out="body",
            )
            result = overpass.query(query)

            outputs = {"name": [], "lat": [], "lon": []}
            for i, node in enumerate(result.nodes()):

                recorded_name = f"{proc_value}_{i}"
                outputs["name"].append(recorded_name)
                outputs["lat"].append(node.lat())
                outputs["lon"].append(node.lon())

                print(f"{node.lat()}, {node.lon()}")

                try:
                    actual_name = node.tags()["name"]
                except KeyError:
                    actual_name = "Unknown"

                actual_and_records_name_mapping[proc_key][proc_value][
                    recorded_name
                ] = actual_name

            df = DataFrame.from_dict(outputs)
            df.to_csv(join(output_dir, f"{proc_key}_{proc_value}.csv"), index=False)

    with open(
        join(output_dir, "actual_and_records_name_mapping.json"), "w"
    ) as json_fid:
        json_dump(actual_and_records_name_mapping, json_fid)


if __name__ == "__main__":
    region = NaN  # can be NaN, or sth like Auckland
    country = "New Zealand"
    query_keys = {"amenity": ["restaurant", "pharmacy"], "shop": ["supermarket"]}
    output_dir = "etc/data/raw_nz_latest"

    if not exists(output_dir):
        makedirs(output_dir)

    query_results(query_keys, region, country, output_dir)
