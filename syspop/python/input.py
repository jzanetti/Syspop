from pickle import load as pickle_load
from os.path import join, exists
from os import makedirs
from pandas import concat as pandas_concat
from pandas import read_parquet
from syspop.python import NZ_DATA_DEFAULT
from numpy.random import choice
from pandas import DataFrame


def pseudo_animal_in_antarctic(
    TYPES: dict = {
        "penguin": {
            "speed_on_land": 1.2,
            "speed_in_sea": 3.0,
            "on_land": True,
            "on_sea": True,
            "food": "fish",
            "enemy": "seal"
        }, 
        "fish": {
            "speed_on_land": None,
            "speed_in_sea": 2.0,
            "on_land": False,
            "on_sea": True,
            "food": None,
            "enemy": "penguin"
        },
        "seal": {
            "speed_on_land": 1.0,
            "speed_in_sea": 3.5,
            "on_land": True,
            "on_sea": True,
            "food": "penguin",
            "enemy": None
        }
    },
    NUMS = {"penguin": 1, "fish": 3, "seal": 1}):
    """Creates a DataFrame representing a pseudo-animal population in Antarctica.

    Args:
        TYPES (dict, optional): Dictionary of animal types and their characteristics.
            Each animal type should have these keys:
                - speed_on_land (float or None): Speed on land in m/s
                - speed_in_sea (float): Speed in sea in m/s
                - on_land (bool): Whether animal can be on land
                - on_sea (bool): Whether animal can be in sea
                - food (str or None): What the animal eats
                - enemy (str or None): Animal's predator
            Defaults to predefined penguin, fish, and seal characteristics.
        NUMS (dict, optional): Number of each animal type in the population.
            Keys must match TYPES keys. Defaults to {"penguin": 1, "fish": 3, "seal": 1}.

    Returns:
        dict: Dictionary containing a single key "population_structure" with a pandas
            DataFrame value. The DataFrame has columns:
                - type: Animal type name
                - speed_on_land: Speed on land in m/s
                - speed_in_sea: Speed in sea in m/s
                - on_land: Whether animal can be on land
                - on_sea: Whether animal can be in sea
                - food: What the animal eats
                - enemy: Animal's predator

    Raises:
        Exception: If any animal type in TYPES doesn't have all required characteristic keys.

    Example:
        >>> result = pseudo_animal_in_antarctic()
        >>> print(result["population_structure"])
           type  speed_on_land  speed_in_sea  on_land  on_sea    food  enemy
        0  penguin          1.2          3.0     True    True    fish   seal
        1     fish          None         2.0    False    True    None  penguin
        2     fish          None         2.0    False    True    None  penguin
        3     fish          None         2.0    False    True    None  penguin
        4     seal          1.0          3.5     True    True  penguin   None
    """
    animal_data = {
        "type": [],
        "speed_on_land": [],
        "speed_in_sea": [],
        "on_land": [],
        "on_sea": [],
        "food": [],
        "enemy": []
    }

    ref_keys = [x for x in list(animal_data.keys()) if x != "type"]

    for proc_type in TYPES:
        data_keys = list(TYPES[proc_type].keys())

        if not ref_keys == data_keys:
            raise Exception(f"Data type does not match for {proc_type}")

    for proc_type in TYPES:
        for _ in range(NUMS[proc_type]):
            animal_data["type"].append(proc_type)
            for proc_key in ref_keys:
                animal_data[proc_key].append(TYPES[proc_type][proc_key])

    animal_data = DataFrame.from_dict(animal_data)

    return {"population_structure": animal_data}


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
