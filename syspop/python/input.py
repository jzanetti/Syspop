from pickle import load as pickle_load
from os.path import join, exists
from os import makedirs
from pandas import concat as pandas_concat
from pandas import read_parquet
from syspop.python import NZ_DATA_DEFAULT

def new_zealand(data_dir: str = NZ_DATA_DEFAULT) -> dict:
    """Get data to create synthentic population

    Args:
        data_dir (str): where the test data located

    Returns:
        dict: test data to be used
    """
    nz_data = {}

    nz_data["geography"] = {}
    for item in [
        "population_structure", 
        "geography_hierarchy", 
        "geography_location", 
        "geography_address",
        "household_composition",
        "commute_travel_to_work",
        "commute_travel_to_school",
        "work_employee",
        "work_employer",
        "school",
        "kindergarten",
        "hospital",
        "shared_space_bakery",
        "shared_space_cafe",
        "shared_space_department_store",
        "shared_space_fast_food",
        "shared_space_park",
        "shared_space_pub",
        "shared_space_restaurant",
        "shared_space_supermarket",
        "shared_space_wholesale"
        ]:
        proc_path = join(data_dir, f"{item}.parquet")
        if exists(proc_path):
            nz_data[item] = read_parquet(proc_path)

    return nz_data


def load_llm_diary(data_dir: str = NZ_DATA_DEFAULT):
    """Load LLM based diary data

    Args:
        data_dir (str, optional): _description_. Defaults to NZ_DATA_DEFAULT.

    Returns:
        _type_: _description_
    """
    with open(f"{data_dir}/llm_diary.pickle", "rb") as fid:
        llm_diary_data = pickle_load(fid)
    return llm_diary_data