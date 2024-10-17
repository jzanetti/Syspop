from datetime import datetime, timedelta
from functools import reduce as functools_reduce
from logging import INFO, Formatter, StreamHandler, basicConfig, getLogger
from os.path import exists, join
from pickle import load as pickle_load
from os import makedirs

from numpy import all as numpy_all
from numpy import array as numpy_array
from numpy import isnan as numpy_isnan
from numpy import nan as numpy_nan
from numpy import nanargmin as numpy_nanargmin
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
        proc_path = join(data_dir, f"syspop_{required_data_type}.parquet")

        if exists(proc_path):
            proc_data_list.append(pandas_read_parquet(proc_path))

    return functools_reduce(
        lambda left, right: pandas_merge(left, right, on="id", how="inner"),
        proc_data_list,
    )

