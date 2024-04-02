from json import dump as json_dump
from os import makedirs
from os.path import exists, join

from funcs import RAW_DATA_DIR
from numpy import NaN
from numpy.random import uniform as numpy_uniform
from OSMPythonTools.nominatim import Nominatim
from OSMPythonTools.overpass import Overpass, overpassQueryBuilder
from pandas import DataFrame
from pandas import concat as pandas_concat

ADD_RANDOM_PLACES_SCALER = {
    "restaurant": 0.3,
    "supermarket": 1.5,
    "wholesale": 1.5,
    "fast_food": 3.0,
    "kindergarten": 5.0,
    "pub": 5.0,
    "cafe": 3.0,
}


overpass = Overpass()
nominatim = Nominatim()


def add_random_location(
    df: DataFrame, total_lines: int, data_type: str, buffer: float = 0.3
):
    output = {"name": [], "lat": [], "lon": []}

    for i in range(total_lines):
        proc_row = df.sample(n=1)
        output["name"].append(f"{data_type}_pseudo_{i}")
        output["lat"].append(proc_row.lat.values[0] + numpy_uniform(-buffer, buffer))
        output["lon"].append(proc_row.lon.values[0] + numpy_uniform(-buffer, buffer))

    return DataFrame(output)


def query_results(
    query_keys: dict,
    region: str,
    country: str,
    output_dir: str = RAW_DATA_DIR,
    if_add_random_loc: bool = False,
):
    """Query data from OSM

    Args:
        query_keys (dict): Query keys such as {"amenity": ["restaurant"]}, where
           amenity is the key, and restaurant is the value, the details can be found
           https://wiki.openstreetmap.org/wiki/Map_features
        region (str): region name such as Auckland
        country (str): country name such as New Zealand
        output_dir (str): Output directory
    """

    if not exists(output_dir):
        makedirs(output_dir)

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

                # print(f"{node.lat()}, {node.lon()}")

                try:
                    actual_name = node.tags()["name"]
                except KeyError:
                    actual_name = "Unknown"

                try:
                    parent_name = node.tags()["is_in"]
                    actual_name += f", {parent_name}"
                except KeyError:
                    pass

                actual_and_records_name_mapping[proc_key][proc_value][
                    recorded_name
                ] = actual_name

            df = DataFrame.from_dict(outputs)

            if if_add_random_loc and proc_value in ADD_RANDOM_PLACES_SCALER:
                random_df = add_random_location(
                    df,
                    int((ADD_RANDOM_PLACES_SCALER[proc_value] + 1.0) * len(df)),
                    proc_value,
                )
                df = pandas_concat([df, random_df], axis=0)

            df.to_csv(join(output_dir, f"{proc_key}_{proc_value}.csv"), index=False)

    with open(
        join(output_dir, "actual_and_records_name_mapping.json"), "w"
    ) as json_fid:
        json_dump(actual_and_records_name_mapping, json_fid)


if __name__ == "__main__":
    region = NaN  # can be NaN, or sth like Auckland
    country = "New Zealand"

    query_keys = {
        "amenity": [
            "restaurant",
            # "pharmacy",
            "fast_food",
            "cafe",
            # "events_venue",
            "pub",
            "gym",
            "childcare",
            "kindergarten",
        ],
        "shop": [
            "supermarket",
            "wholesale",
            "bakery",
            # "general",
            "department_store",
            # "convenience",
        ],
        "leisure": ["park"],
        # "tourism": ["museum"],
    }

    query_results(query_keys, region, country, if_add_random_loc=False)

    # query_results(
    #    {"amenity": ["kindergarten"]}, "Wellington", country, output_dir="/tmp/test"
    # )
