from copy import deepcopy
from datetime import datetime
from logging import getLogger

from pandas import DataFrame

logger = getLogger()



def assign_place_to_address(
    address_type: str,
    pop_data_input: DataFrame,
    address_data_input: DataFrame,
    proc_area: int,
) -> DataFrame:
    """Randomly assign each household to an address

    Args:
        address_type (str): such as household, company etc.
        pop_data_input (DataFrame): population data for an area
        address_data_input (DataFrame): address data for an area

    Returns:
        DataFrame: Processed population data
    """

    all_address = []
    all_address_names = list(pop_data_input[address_type].unique())

    for proc_address_name in all_address_names:
        if len(address_data_input) > 0:
            proc_address = address_data_input.sample(n=1)
            all_address.append(
                f"{proc_address_name}, {round(proc_address['latitude'].values[0], 5)},{round(proc_address['longitude'].values[0], 5)}, {proc_area}"
            )

    return all_address


def add_random_address(
    base_pop: DataFrame,
    address_data: DataFrame,
    address_type: str
) -> DataFrame:
    """Add address (lat and lon) to each household

    Args:
        base_pop (DataFrame): Base population to be processed
        address_type (str): address_type such as household or company etc.
        address_data (DataFrame): Address data for each area
        use_parallel (bool, optional): If use parallel processing. Defaults to False.
        n_cpu (int, optional): number of CPU to use. Defaults to 16.

    Returns:
        DataFrame: updated population data
    """
    start_time = datetime.utcnow()

    all_areas = list(base_pop["area"].unique())

    results = []

    for i, proc_area in enumerate(all_areas):
        logger.info(f"{i}/{len(all_areas)}: Processing {proc_area}")

        proc_address_data = address_data[address_data["area"] == proc_area]

        area_type = "area"
        if address_type == "company":
            area_type = "area_work"

        proc_pop_data = base_pop[base_pop[area_type] == proc_area]

        processed_address = assign_place_to_address(
            address_type, proc_pop_data, proc_address_data, proc_area
        )

        results.append(processed_address)

    flattened_results = [item for sublist in results for item in sublist]
    results_dict = {"name": [], "latitude": [], "longitude": [], "area": []}
    for proc_result in flattened_results:
        proc_value = proc_result.split(",")
        results_dict["name"].append(proc_value[0])
        results_dict["latitude"].append(float(proc_value[1]))
        results_dict["longitude"].append(float(proc_value[2]))
        results_dict["area"].append(int(proc_value[3]))

    results_df = DataFrame.from_dict(results_dict)
    results_df["type"] = address_type

    results_df["area"] = results_df["area"].astype(int)

    end_time = datetime.utcnow()

    total_mins = round((end_time - start_time).total_seconds() / 60.0, 3)
    logger.info(f"Processing time (address): {total_mins}")

    return results_df
