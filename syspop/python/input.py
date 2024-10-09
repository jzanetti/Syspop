from pickle import load as pickle_load
from os.path import join, exists
from os import makedirs
from pandas import concat as pandas_concat
from syspop.python import NZ_DATA_DEFAULT

def new_zealand(write_out_file: bool = True, test_data_dir: str = NZ_DATA_DEFAULT) -> dict:
    """Get data to create synthentic population

    Args:
        test_data_dir (str): where the test data located
        write_out_file (bool, optional): If write out the 
            pickle in dataframe (e.g., can be used in R). Defaults to False.

    Returns:
        dict: test data to be used
    """
    test_data = {}
    with open(f"{test_data_dir}/population.pickle", "rb") as fid:
        test_data["pop_data"] = pickle_load(fid)

    try:
        with open(f"{test_data_dir}/geography.pickle", "rb") as fid:
            test_data["geog_data"] = pickle_load(fid)
    except FileNotFoundError:
        test_data["geog_data"] = None

    try:
        with open(f"{test_data_dir}/household.pickle", "rb") as fid:
            test_data["household_data"] = pickle_load(fid)
    except FileNotFoundError:
        test_data["household_data"] = {"household": None}

    try:
        with open(f"{test_data_dir}/commute.pickle", "rb") as fid:
            test_data["commute_data"] = pickle_load(fid)
    except FileNotFoundError:
        test_data["commute_data"] = {"travel_to_work": None}

    try:
        with open(f"{test_data_dir}/work.pickle", "rb") as fid:
            test_data["work_data"] = pickle_load(fid)
    except FileNotFoundError:
        test_data["work_data"] = None

    try:
        with open(f"{test_data_dir}/school.pickle", "rb") as fid:
            test_data["school_data"] = pickle_load(fid)
    except FileNotFoundError:
        test_data["school_data"] = {"school": None}

    try:
        with open(f"{test_data_dir}/kindergarten.pickle", "rb") as fid:
            test_data["kindergarten_data"] = pickle_load(fid)
    except FileNotFoundError:
        test_data["kindergarten_data"] = {"kindergarten": None}

    try:
        with open(f"{test_data_dir}/hospital.pickle", "rb") as fid:
            test_data["hospital_data"] = pickle_load(fid)
    except FileNotFoundError:
        test_data["hospital_data"] = {"hospital": None}

    try:
        with open(f"{test_data_dir}/supermarket.pickle", "rb") as fid:
            test_data["supermarket_data"] = pickle_load(fid)
    except FileNotFoundError:
        test_data["supermarket_data"] = {"supermarket": None}

    try:
        with open(f"{test_data_dir}/department_store.pickle", "rb") as fid:
            test_data["department_store_data"] = pickle_load(fid)
    except FileNotFoundError:
        test_data["department_store_data"] = {"department_store": None}

    try:
        with open(f"{test_data_dir}/wholesale.pickle", "rb") as fid:
            test_data["wholesale_data"] = pickle_load(fid)
    except FileNotFoundError:
        test_data["wholesale_data"] = {"wholesale": None}

    try:
        with open(f"{test_data_dir}/restaurant.pickle", "rb") as fid:
            test_data["restaurant_data"] = pickle_load(fid)
    except FileNotFoundError:
        test_data["restaurant_data"] = {"restaurant": None}

    try:
        with open(f"{test_data_dir}/fast_food.pickle", "rb") as fid:
            test_data["fast_food_data"] = pickle_load(fid)
    except FileNotFoundError:
        test_data["fast_food_data"] = {"fast_food": None}

    try:
        data_list = []
        for proc_data_type in ["cafe", "bakery"]:
            with open(f"{test_data_dir}/{proc_data_type}.pickle", "rb") as fid:
                data_list.append(pickle_load(fid)[proc_data_type])
        test_data["cafe_data"] = {"cafe": pandas_concat(data_list, axis=0)}
    except FileNotFoundError:
        test_data["cafe_data"] = {"cafe": None}

    try:
        with open(f"{test_data_dir}/pub.pickle", "rb") as fid:
            test_data["pub_data"] = pickle_load(fid)
    except FileNotFoundError:
        test_data["pub_data"] = {"pub": None}
    try:
        with open(f"{test_data_dir}/park.pickle", "rb") as fid:
            test_data["park_data"] = pickle_load(fid)
    except FileNotFoundError:
        test_data["park_data"] = {"park": None}

    try:
        with open(f"{test_data_dir}/llm_diary.pickle", "rb") as fid:
            test_data["llm_diary_data"] = pickle_load(fid)
    except FileNotFoundError:
        test_data["llm_diary_data"] = None

    try:
        with open(f"{test_data_dir}/others.pickle", "rb") as fid:
            test_data["others"] = pickle_load(fid)
    except FileNotFoundError:
        test_data["others"] = {"birthplace": None, "mmr": None}

    if write_out_file:
        data_catalog_lines = []
        test_data_input_dir = join(test_data_dir, "input")
        if not exists(test_data_input_dir):
            makedirs(test_data_input_dir)

    for data_type1 in test_data:
        if data_type1 == "llm_diary_data":
            for data_type0 in ["percentage", "data"]:
                for data_type2 in test_data[data_type1][data_type0]:
                    data_path = join(test_data_input_dir, f"{data_type1}_{data_type0}_{data_type2}.parquet")
                    test_data[data_type1][data_type0][data_type2].to_parquet(data_path)
                    data_catalog_lines.append(f"{data_type1},{data_type0},{data_type2},{data_path}")
        else:
            for data_type2 in test_data[data_type1]:
                data_path = join(test_data_input_dir, f"{data_type1}_{data_type2}.parquet")
                test_data[data_type1][data_type2].to_parquet(join(test_data_input_dir, f"{data_type1}_{data_type2}.parquet"))
                data_catalog_lines.append(f"{data_type1},,{data_type2},{data_path}")
    
    with open(join(test_data_input_dir, "data_catalog.txt"), "w") as file:
        for line in data_catalog_lines:
            file.write(line + "\n")

    return test_data