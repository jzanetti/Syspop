from pickle import load as pickle_load
from os.path import join, exists
from os import makedirs
from pandas import concat as pandas_concat
from pandas import read_parquet
from syspop.python import NZ_DATA_DEFAULT
from numpy.random import choice
from pandas import DataFrame


def new_zealand(
    data_dir: str = NZ_DATA_DEFAULT, apply_pseudo_ethnicity: bool = False
) -> dict:
    """Get data to create synthentic population

    Args:
        data_dir (str): where the test data located
        apply_pseudo_ethnicity (bool): add pseudo ethnicity (for testing purpose)

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
        "work_income",
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
        "shared_space_wholesale",
    ]:
        proc_path = join(data_dir, f"{item}.parquet")
        if exists(proc_path):
            nz_data[item] = read_parquet(proc_path)

    if apply_pseudo_ethnicity:
        nz_data["household_composition"] = add_pseudo_hhd_ethnicity(
            nz_data["household_composition"]
        )

    return nz_data


def add_pseudo_hhd_ethnicity(
    household_composition_data: DataFrame,
    ethnicities: list = ["European", "Maori", "Pacific", "Asian", "MELAA"],
    weights: list = [0.6, 0.15, 0.1, 0.12, 0.03],
) -> DataFrame:
    """
    Add a pseudo-ethnicity column to a household composition
    dataframe based on weighted probabilities.

    Parameters
    ----------
    household_composition_data : pandas.DataFrame
        The input dataframe containing household composition data.
    ethnicities : list, optional
        List of ethnicity categories to assign. Defaults to
            ["european", "maori", "asian", "others"].
    weights : list, optional
        List of probabilities corresponding to each ethnicity. Must sum to 1.
        Defaults to [0.7, 0.15, 0.12, 0.03].

    Returns
    -------
    pandas.DataFrame
        The input dataframe with an added or updated "ethnicity" column.

    Examples
    --------
    >>> df = pd.DataFrame({'sa2': [100100, 100100], 'age': [0, 1]})
    >>> add_pseudo_hhd_ethnicity(df)
       sa2  age ethnicity
    0  100100    0  european
    1  100100    1     maori
    """
    household_composition_data["ethnicity"] = choice(
        ethnicities, size=len(household_composition_data), p=weights
    )
    return household_composition_data


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
