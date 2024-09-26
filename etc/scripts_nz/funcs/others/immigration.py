from funcs.preproc import _read_original_csv
from pandas import DataFrame


def add_birthplace(birthplace_path: str) -> DataFrame:
    return _read_original_csv(birthplace_path)
