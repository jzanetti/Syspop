from datetime import datetime, timedelta
from functools import reduce as functools_reduce
from logging import INFO, Formatter, StreamHandler, basicConfig, getLogger
from os.path import join
from pickle import load as pickle_load

from pandas import DataFrame
from pandas import concat as pandas_concat
from pandas import merge as pandas_merge
from pandas import read_parquet as pandas_read_parquet
from yaml import safe_load as yaml_load

logger = getLogger()


def setup_logging(
    workdir: str = "/tmp",
    log_type: str = "syspop",
    start_utc: datetime = datetime.utcnow(),
):
    """set up logging system for tasks

    Returns:
        object: a logging object
    """
    formatter = Formatter(
        "%(asctime)s - %(name)s.%(lineno)d - %(levelname)s - %(message)s"
    )
    ch = StreamHandler()
    ch.setLevel(INFO)
    ch.setFormatter(formatter)
    logger_path = join(workdir, f"{log_type}.{start_utc.strftime('%Y%m%d')}")
    basicConfig(filename=logger_path),
    logger = getLogger()
    logger.setLevel(INFO)
    logger.addHandler(ch)

    return logger


def read_cfg(cfg_path: str, key: str = None) -> dict:
    """Read configuration file

    Args:
        cfg_path (str): configuration path

    Returns:
        dict: configuration
    """
    with open(cfg_path, "r") as fid:
        cfg = yaml_load(fid)

    if key is None:
        return cfg

    return cfg[key]


def round_a_list(input: list, sig_figures: int = 3):
    return [round(x, sig_figures) for x in input]


def round_a_datetime(dt):
    # If minutes are 30 or more, round up to the next hour
    if dt.minute >= 30:
        dt += timedelta(hours=1)
    return dt.replace(minute=0, second=0, microsecond=0)


def merge_syspop_data(data_dir: str, data_types: list) -> DataFrame:
    """Merge different sypop data together

    Args:
        data_dir (str): Data directory
        data_types (list): data types to be merged, e.g., [base, travel etc.]

    Returns:
        DataFrame: merged datasets
    """

    proc_data_list = []
    for required_data_type in data_types:
        proc_data_list.append(
            pandas_read_parquet(join(data_dir, f"syspop_{required_data_type}.parquet"))
        )

    return functools_reduce(
        lambda left, right: pandas_merge(left, right, on="id", how="inner"),
        proc_data_list,
    )


def _get_data_for_test(test_data_dir: str) -> dict:
    test_data = {}
    with open(f"{test_data_dir}/population.pickle", "rb") as fid:
        test_data["pop_data"] = pickle_load(fid)

    with open(f"{test_data_dir}/geography.pickle", "rb") as fid:
        test_data["geog_data"] = pickle_load(fid)

    with open(f"{test_data_dir}/household.pickle", "rb") as fid:
        test_data["household_data"] = pickle_load(fid)

    with open(f"{test_data_dir}/commute.pickle", "rb") as fid:
        test_data["commute_data"] = pickle_load(fid)

    with open(f"{test_data_dir}/work.pickle", "rb") as fid:
        test_data["work_data"] = pickle_load(fid)

    with open(f"{test_data_dir}/school.pickle", "rb") as fid:
        test_data["school_data"] = pickle_load(fid)

    with open(f"{test_data_dir}/kindergarten.pickle", "rb") as fid:
        test_data["kindergarten_data"] = pickle_load(fid)

    with open(f"{test_data_dir}/hospital.pickle", "rb") as fid:
        test_data["hospital_data"] = pickle_load(fid)

    with open(f"{test_data_dir}/supermarket.pickle", "rb") as fid:
        test_data["supermarket_data"] = pickle_load(fid)

    with open(f"{test_data_dir}/department_store.pickle", "rb") as fid:
        test_data["department_store_data"] = pickle_load(fid)

    with open(f"{test_data_dir}/wholesale.pickle", "rb") as fid:
        test_data["wholesale_data"] = pickle_load(fid)

    with open(f"{test_data_dir}/restaurant.pickle", "rb") as fid:
        test_data["restaurant_data"] = pickle_load(fid)

    with open(f"{test_data_dir}/fast_food.pickle", "rb") as fid:
        test_data["fast_food_data"] = pickle_load(fid)

    data_list = []
    for proc_data_type in ["cafe", "bakery"]:
        with open(f"{test_data_dir}/{proc_data_type}.pickle", "rb") as fid:
            data_list.append(pickle_load(fid)[proc_data_type])
    test_data["cafe_data"] = {"cafe": pandas_concat(data_list, axis=0)}

    with open(f"{test_data_dir}/pub.pickle", "rb") as fid:
        test_data["pub_data"] = pickle_load(fid)

    with open(f"{test_data_dir}/park.pickle", "rb") as fid:
        test_data["park_data"] = pickle_load(fid)

    with open(f"{test_data_dir}/llm_diary.pickle", "rb") as fid:
        test_data["llm_diary_data"] = pickle_load(fid)

    return test_data
