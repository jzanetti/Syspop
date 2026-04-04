from process.data.data import obtain_data
from logging import info as log_info
from etc.sample_data.api_keys import STATS_API
from yaml import safe_load
from pickle import dump as pickle_dump
from pickle import load as pickle_load
from process.model.utils import obtain_all_tasks


def obtain_sample_data_cfg():
    with open("etc/sample_data/sample_data_cfg.yml", "r") as fid:
        cfg = safe_load(fid)

    return cfg["tables"]


def obtain_sample_api_key(api_key: str or None = None):
    if api_key is None:
        api_key = STATS_API

    return api_key


def load_sample_data(
    data_types: list = [
        "seed",
        "industry",
        "occupation",
        "occupation_income",
        "industry_income",
        "travel_to_work",
        "work_hours",
    ],
    refresh: bool = False,
):
    """
    Wrapper function to retrieve data for specified types using an API key.

        cfg_data (dict): Dictionary containing configuration for each data type,
            where keys are data type strings and values are configuration dicts.
        api_key (str or None, optional): API key to use for data retrieval. If None,
            the key will be obtained via `obtain_sample_api_key`. Defaults to None.
        data_types (list, optional): List of data type strings to retrieve.
            Defaults to ["pop"].

        dict: A dictionary mapping each data type (from `data_types`) to its
            corresponding data retrieved using the API.

    Raises:
        KeyError: If a specified data type is not present in `cfg_data`.
        Exception: Propagates exceptions raised during API key retrieval or data fetching.

    Example:
        >>> cfg_data = {"seed": {...}, "income": {...}}
        >>> result = obtain_data_wrapper(cfg_data, api_key="my_key", data_types=["pop", "income"])
        >>> print(result)
        {'seed': <pop_data>, 'income': <income_data>}
    """

    if not refresh:
        data_dict = pickle_load(open("etc/sample_data/sample_data.pkl", "rb"))
    else:
        api_key = obtain_sample_api_key()
        cfg_data = obtain_sample_data_cfg()

        data_dict = {}
        for data_type in data_types:

            log_info(f"Obtaining data for type: {data_type}")

            data_dict[data_type] = obtain_data(cfg_data[data_type], api_key)

        pickle_dump(data_dict, open("etc/sample_data/sample_data.pkl", "wb"))

    with open("etc/sample_data/sample_model_cfg.yml", "r") as fid:
        model_cfg = safe_load(fid)

    task_list = obtain_all_tasks(model_cfg["tasks"], model_cfg["cfg"])

    return data_dict, task_list
