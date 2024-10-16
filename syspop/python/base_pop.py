from datetime import datetime
from logging import getLogger

from numpy.random import choice
from pandas import DataFrame

logger = getLogger()

def base_pop_wrapper(
    structure_data: DataFrame,
    output_area_filter: list or None,
) -> DataFrame:
    """Create base population

    Args:
        gender_data (DataFrame): Gender data for each age
        ethnicity_data (DataFrame): Ethnicity data for each age
        output_area_filter (list or None): With area ID to be used

    Returns:
        DataFrame: Produced base population

    The output is sth like:
                area  age  gender ethnicity
        0     236800    0  female  European
        1     236800    0    male  European
        2     236800    0    male  European
        3     236800    0  female  European
        4     236800    0    male  European
        ...      ...  ...     ...       ...
    """

    start_time = datetime.utcnow()
    if output_area_filter is not None:
        structure_data = structure_data[structure_data["area"].isin(output_area_filter)]

    if structure_data is not None:
        df_repeated = structure_data.loc[structure_data.index.repeat(structure_data["value"].astype(int))]
        population = df_repeated.reset_index(drop=True).drop(columns=["value"])

    end_time = datetime.utcnow()
    total_mins = (end_time - start_time).total_seconds() / 60.0

    logger.info(f"Processing time (base population): {total_mins}")

    return population
