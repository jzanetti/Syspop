from funcs import RAW_DATA
from pandas import read_csv


def add_birthplace():
    birthplace = RAW_DATA["others"]["birthplace"]
    return read_csv(birthplace).drop_duplicates()
