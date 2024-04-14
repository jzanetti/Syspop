from os.path import join
from pickle import dump as pickle_dump
from random import randint as random_randint

from funcs import DEPENDENT_CHILDREN_COUNT_CODE, RAW_DATA
from pandas import isnull as pandas_isnull
from pandas import read_csv


def create_household_and_dwelling_number(workdir):
    """Create household number

    Args:
        workdir (str): _description_
    """
    data = read_csv(RAW_DATA["household"]["household_number"])

    data = data.rename(
        columns={
            "SA2 Code": "area",
            "Number of people": "people_num",
            "Number of adults": "adults_num",
            "Dwelling type": "dwelling_type",
            "Count": "household_num",
        }
    )

    return data


def create_household_number(workdir):
    """Create household number

    Args:
        workdir (str): _description_
    """
    data = read_csv(RAW_DATA["household"]["household_number"])

    data = data[
        ["SA2 Code", "People Count Code", "Dependent Children Count Code", "Count"]
    ]

    data["Dependent Children Count"] = data["Dependent Children Count Code"].map(
        DEPENDENT_CHILDREN_COUNT_CODE
    )
    data["Dependent Children Count"] = (
        data["Dependent Children Count"]
        .apply(lambda x: random_randint(0, 5) if pandas_isnull(x) else x)
        .astype(int)
    )

    data["Count"] = (
        data["Count"]
        .apply(lambda x: random_randint(1, 5) if x == "s" else x)
        .astype(int)
    )

    data = data.rename(
        columns={
            "SA2 Code": "area",
            "People Count Code": "people_num",
            "Dependent Children Count": "children_num",
            "Count": "household_num",
        }
    )

    data = data[["area", "people_num", "children_num", "household_num"]]

    # remove duplicated household composition
    data = data.groupby(["area", "people_num", "children_num"], as_index=False)[
        "household_num"
    ].sum()

    return data
