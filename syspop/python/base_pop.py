from datetime import datetime
from logging import getLogger

from numpy.random import choice
from pandas import DataFrame

logger = getLogger()


def create_pop(output_area, age, df_gender_melt, df_ethnicity_melt, ref_population: str = "gender"):
    population = []
    # Get the gender and ethnicity probabilities for the current output_area and age
    gender_probs = df_gender_melt.loc[
        (df_gender_melt["area"] == output_area) & (df_gender_melt["age"] == age),
        ["gender", "prob", "count"],
    ]
    ethnicity_probs = df_ethnicity_melt.loc[
        (df_ethnicity_melt["area"] == output_area) & (df_ethnicity_melt["age"] == age),
        ["ethnicity", "prob", "count"],
    ]

    # Determine the number of individuals for the current output_area and age
    if ref_population == "gender":
        n_individuals = int(gender_probs["count"].sum())
    elif ref_population == "ethnicity":
        n_individuals = int(ethnicity_probs["count"].sum())
    else:
        raise Exception("Total people must be within [gender, ethnicity]")

    if n_individuals == 0:
        return []

    # Randomly assign gender and ethnicity to each individual
    genders = choice(gender_probs["gender"], size=n_individuals, p=gender_probs["prob"])

    ethnicities = choice(
        ethnicity_probs["ethnicity"],
        size=n_individuals,
        p=ethnicity_probs["prob"],
    )

    for gender, ethnicity in zip(genders, ethnicities):
        individual = {
            "area": output_area,
            "age": age,
            "gender": gender,
            "ethnicity": ethnicity,
        }
        population.append(individual)

    return population


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

    # create an empty address dataset
    base_address = DataFrame(columns=["type", "name", "latitude", "longitude"])

    logger.info(f"Processing time (base population): {total_mins}")

    return population, base_address
