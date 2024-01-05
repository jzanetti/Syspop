from os.path import join

from pandas import read_csv

from funcs import RAW_DATA
from pickle import dump as pickle_dump

def create_household_number(workdir):
    """Create household number

    Args:
        workdir (str): _description_
    """
    data = read_csv(RAW_DATA["household"]["household_number"])

    data = data[data["Number of dependent children"] >= 10000]

    data = data.rename(
        columns={
            "Number of dependent children": "area"
        }
    )

    data.columns = ["area"] + [
        int(col) for col in data.columns if col not in ["area"]
    ]

    with open(join(workdir, "household.pickle"), 'wb') as fid:
        pickle_dump({
        "household": data
    }, fid)
