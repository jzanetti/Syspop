from datetime import datetime
from logging import getLogger
from pickle import dump as pickle_dump

import ray
from numpy.random import choice
from pandas import DataFrame

logger = getLogger()


@ray.remote
def create_base_pop_remote(output_area, age, df_gender_melt, df_ethnicity_melt):
    return create_base_pop(output_area, age, df_gender_melt, df_ethnicity_melt)


def create_base_pop(output_area, age, df_gender_melt, df_ethnicity_melt):
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
    n_individuals = int(gender_probs["count"].sum())

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
    gender_data: DataFrame,
    ethnicity_data: DataFrame,
    output_area_filter: list or None,
    use_parallel: bool = False,
    n_cpu: int = 8,
) -> DataFrame:
    """Create base population

    Args:
        gender_data (DataFrame): Gender data for each age
        ethnicity_data (DataFrame): Ethnicity data for each age
        output_area_filter (list or None): With area ID to be used
        use_parallel (bool, optional): If apply ray parallel processing. Defaults to False.

    Returns:
        DataFrame: Produced base population
    """

    if output_area_filter is not None:
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

    start_time = datetime.utcnow()

    if use_parallel:
        ray.init(num_cpus=n_cpu, include_dashboard=False)

    results = []

    output_areas = list(df_gender_melt["area"].unique())
    total_output_area = len(output_areas)
    for i, output_area in enumerate(output_areas):
        logger.info(f"Processing: {i}/{total_output_area}")
        for age in df_gender_melt["age"].unique():
            if use_parallel:
                result = create_base_pop_remote.remote(
                    output_area, age, df_gender_melt, df_ethnicity_melt
                )
            else:
                result = create_base_pop(
                    output_area, age, df_gender_melt, df_ethnicity_melt
                )
            results.append(result)

    if use_parallel:
        results = ray.get(results)
        ray.shutdown()

    population = [item for sublist in results for item in sublist]

    end_time = datetime.utcnow()
    total_mins = (end_time - start_time).total_seconds() / 60.0

    # create an empty address dataset
    base_address = DataFrame(columns=["type", "name", "latitude", "longitude"])

    logger.info(f"Processing time (base population): {total_mins}")

    # Convert the population to a DataFrame
    return DataFrame(population), base_address
