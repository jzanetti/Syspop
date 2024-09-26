from funcs.preproc import _read_original_csv
from pandas import DataFrame


def add_mmr(mmr_data_path: str) -> DataFrame:
    return _read_original_csv(mmr_data_path)
