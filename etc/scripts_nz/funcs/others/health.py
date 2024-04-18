from funcs import RAW_DATA
from pandas import read_csv


def add_mmr():
    mmr_vaccine = RAW_DATA["others"]["mmr_vaccine"]
    return read_csv(mmr_vaccine).drop_duplicates()
