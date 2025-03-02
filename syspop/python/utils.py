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


def select_place_with_contstrain(
    places: DataFrame,
    constrain_name: str,
    constrain_priority: str,
    constrain_options: list,
    constrain_priority_weight: float = 0.85,
    check_constrain_priority: bool = False,
):
    """
    Select a row from a DataFrame based on weighted sampling of a specified column,
    with priority given to a specific value.

    Args:
        places (DataFrame): pandas DataFrame containing the data to sample from
        constrain_name (str): Name of the column to base the sampling weights on
        constrain_priority (str): Value in the constrain_name column to prioritize
        constrain_options (list): List of all possible values in the constrain_name column
        constrain_priority_weight (float, optional): Weight assigned to the priority value,
            between 0 and 1. Defaults to 0.85
        check_constrain_priority (bool, optional): If True, raises an exception if
            constrain_priority isn't in constrain_options. Defaults to False

    Returns:
        DataFrame: A single-row DataFrame sampled based on the weighted probabilities

    Raises:
        Exception: If constrain_name is not a column in places
        Exception: If check_constrain_priority is True and constrain_priority is not
            in constrain_options

    Notes:
        - The remaining weight (1 - constrain_priority_weight) is distributed equally
          among the other values in constrain_options
        - If constrain_priority isn't in constrain_options and check_constrain_priority
          is False, returns a random sample without weights
    """
    if constrain_name not in places:
        raise Exception(
            f"The constrain name: {constrain_name} is not in the place_data"
        )

    if constrain_priority not in constrain_options:
        if check_constrain_priority:
            raise Exception(
                f"The agent constrain priority {constrain_priority} ({constrain_name}) "
                "is not part of the provided constrain_options"
            )
        else:
            return places.sample(n=1)

    constrain_weights_others = (1.0 - constrain_priority_weight) / (
        len(constrain_options) - 1
    )

    constrain_weights = [constrain_weights_others] * len(constrain_options)

    constrain_weights[constrain_options.index(constrain_priority)] = (
        constrain_priority_weight
    )

    # Create a weight mapping
    constrain_weights_map = dict(zip(constrain_options, constrain_weights))

    # Assign weights to each row based on category
    df_weights = places[constrain_name].map(constrain_weights_map)

    # Sample one row using weights
    return places.sample(n=1, weights=df_weights)
