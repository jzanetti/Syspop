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
    gender_data: DataFrame,
    ethnicity_data: DataFrame,
    output_area_filter: list or None,
    ref_population: str = "gender"
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
        try:
            gender_data = gender_data[gender_data["area"].isin(output_area_filter)]
            ethnicity_data = ethnicity_data[ethnicity_data["area"].isin(output_area_filter)]
        except TypeError:
            structure_data = structure_data[structure_data["area"].isin(output_area_filter)]

    if structure_data is not None:
        df_repeated = structure_data.loc[structure_data.index.repeat(structure_data["value"].astype(int))]
        population = df_repeated.reset_index(drop=True).drop(columns=["value"])
    else:
        gender_data = gender_data[gender_data["area"].isin(output_area_filter)]
        ethnicity_data = ethnicity_data[ethnicity_data["area"].isin(output_area_filter)]
        # Assuming df_gender and df_ethnicity are your dataframes
        df_gender_melt = gender_data.melt(
            id_vars=["area", "gender"], var_name="age", value_name="count"
        )
        df_ethnicity_melt = ethnicity_data.melt(
            id_vars=["area", "ethnicity"], var_name="age", value_name="count"
        )

        # Normalize the data
        df_gender_melt["prob"] = df_gender_melt.groupby(["area", "age"])["count"].transform(
            lambda x: x / x.sum()
        )
        df_ethnicity_melt["prob"] = df_ethnicity_melt.groupby(["area", "age"])[
            "count"
        ].transform(lambda x: x / x.sum())

        results = []

        output_areas = list(df_gender_melt["area"].unique())
        total_output_area = len(output_areas)
        for i, output_area in enumerate(output_areas):
            logger.info(f"Base population: {i}/{total_output_area} ({int(i * 100.0/total_output_area)}%)")
            for age in df_gender_melt["age"].unique():

                result = create_pop(
                    output_area, age, df_gender_melt, df_ethnicity_melt, ref_population=ref_population
                )

                results.append(result)

        population = [item for sublist in results for item in sublist]
        population = DataFrame(population)
        population["age"] = population["age"].astype(int)

    end_time = datetime.utcnow()
    total_mins = (end_time - start_time).total_seconds() / 60.0

    # create an empty address dataset
    base_address = DataFrame(columns=["type", "name", "latitude", "longitude"])

    logger.info(f"Processing time (base population): {total_mins}")

    return population, base_address