from pandas import DataFrame, read_csv

from funcs import POPULATION_CODE


def read_population_structure(population_structure_data_path: str) -> DataFrame:
    """Read population structure data (usually for v2.0)

    Args:
        population_structure_data_path (str): _description_

    Returns:
        _type_: _description_
    """
    df = read_csv(population_structure_data_path)[["sa2", "ethnicity", "age", "gender", "value"]]
    df = df.rename(columns={"sa2": "area"})
    df = df.replace(POPULATION_CODE).reset_index()
    df = df.drop(columns=["index"])
    return df