from os.path import join

from pandas import read_csv

from funcs import RAW_DATA


def create_household_number():
    """Create household number

    Args:
        workdir (str): _description_
    """
    data = read_csv(RAW_DATA["household"]["household_number"])

    data = data[data["Household composition"] >= 10000]

    data = data.rename(
        columns={
            "Household composition": "output_area",
            "Total households - household composition": ">=0 >=0 >=0 >=0 >=0",
        }
    )

    return data
