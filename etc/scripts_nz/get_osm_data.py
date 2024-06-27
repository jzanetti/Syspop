import logging
from json import dump as json_dump
from os import makedirs
from os.path import exists, join
from time import sleep
from urllib.error import HTTPError

from funcs import RAW_DATA_DIR
from numpy import NaN
from numpy.random import uniform as numpy_uniform
from OSMPythonTools.nominatim import Nominatim
from OSMPythonTools.overpass import Overpass, overpassQueryBuilder
from pandas import DataFrame
from pandas import concat as pandas_concat

# Configure the logger
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

# Create a logger object
logger = logging.getLogger(__name__)


"""
from OSMPythonTools.api import Api

# Initialize the API
api = Api()

# Define the OSM ID
osm_id = 307249609

# Get the OSM object
osm_object = api.query(f'way/{osm_id}')

# Print the OSM object
print(osm_object)


"""


ADD_RANDOM_PLACES_SCALER = {
    "restaurant": 0.3,
    "supermarket": 1.5,
    "wholesale": 1.5,
    "fast_food": 3.0,
    "kindergarten": 5.0,
    "pub": 5.0,
    "cafe": 3.0,
}


QUERY_KEYS = {
    "Tonga": {
        "building": ["yes", "residential", "house"],
    },
    "New Zealand": {
        "building": ["house"],
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
            "school",
        ],
        "shop": [
            "supermarket",
            "wholesale",
            "bakery",
            "general",
            "department_store",
            "convenience",
        ],
        "leisure": ["park"],
        # "tourism": ["museum"],
    },
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
    use_element: bool = False,
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
            # query = overpassQueryBuilder(area=area_id, elementType='way', selector='"amenity"="school"', out='body')
            # proc_value = "school"
            query = overpassQueryBuilder(
                area=areaId,
                elementType=["node", "way"],
                selector=f'"{proc_key}"="{proc_value}"',
                out="body",
            )
            result = overpass.query(query, timeout=300)
            # result.toJSON()
            # elements = result.elements()
            # elements[0].lat() ...
            outputs = {"name": [], "lat": [], "lon": []}

            if use_element:
                all_results = result.elements()
            else:
                all_results = result.nodes()

            if all_results is None:
                continue

            for i, node in enumerate(all_results):

                logger.info(i)

                try:
                    recorded_name = f"{proc_value}_{i}"

                    proc_lat = node.lat()
                    proc_lon = node.lon()
                    if (proc_lat is None) or (proc_lon is None):
                        proc_lat = node.nodes()[0].lat()
                        proc_lon = node.nodes()[0].lon()
                except Exception:
                    sleep(10)
                    logger.info(f"Not able to connect for {i}")
                    continue

                outputs["name"].append(recorded_name)
                outputs["lat"].append(proc_lat)
                outputs["lon"].append(proc_lon)

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
    region = "Tongatapu"  # can be NaN, or sth like Auckland
    country = "Tonga"  # New Zealand, Tonga
    query_results(
        QUERY_KEYS[country],
        region,
        country,
        output_dir="etc/data/raw_tonga_latest",
        if_add_random_loc=False,
        use_element=True,
    )

    # query_results(
    #    {"amenity": ["kindergarten"]}, "Wellington", country, output_dir="/tmp/test"
    # )
