from pandas import DataFrame
from numpy import NaN
from datetime import datetime
from logging import getLogger
import ray
from copy import deepcopy
logger = getLogger()

@ray.remote
def assign_household_to_address_remote(
        pop_data_input: DataFrame, 
        address_data_input: DataFrame):
    """Randomly assign each household to an address (for parallel processing)

    Args:
        pop_data_input (DataFrame): population data for an area
        address_data_input (DataFrame): address data for an area

    Returns:
        DataFrame: Processed population data
    """
    return assign_household_to_address(
        deepcopy(pop_data_input), 
        address_data_input)

def assign_household_to_address(
        pop_data_input: DataFrame, 
        address_data_input: DataFrame) -> DataFrame:
    """Randomly assign each household to an address

    Args:
        pop_data_input (DataFrame): population data for an area
        address_data_input (DataFrame): address data for an area

    Returns:
        DataFrame: Processed population data
    """

    all_households = list(pop_data_input["household"].unique())

    for proc_household in all_households:

        proc_pop_data = pop_data_input[
            pop_data_input["household"] == proc_household]
        
        if len(address_data_input) > 0:
            proc_address = address_data_input.sample(n=1)
            proc_pop_data["address"] = f"({round(proc_address['latitude'].values[0], 5)}, {round(proc_address['longitude'].values[0], 5)})"
            pop_data_input.loc[proc_pop_data.index] = proc_pop_data
    
    return pop_data_input


def add_household_address(
        base_pop: DataFrame, 
        address_data: DataFrame,
        use_parallel: bool = False, 
        n_cpu: int = 16) -> DataFrame:
    """Add address (lat and lon) to each household

    Args:
        base_pop (DataFrame): Base population to be processed
        address_data (DataFrame): Address data for each area
        use_parallel (bool, optional): If use parallel processing. Defaults to False.
        n_cpu (int, optional): number of CPU to use. Defaults to 16.

    Returns:
        DataFrame: updated population data
    """
    start_time = datetime.utcnow()

    all_areas = list(base_pop["area"].unique())

    base_pop["address"] = NaN


    if use_parallel:
        ray.init(num_cpus=n_cpu, include_dashboard=False)

    results = []

    for i, proc_area in enumerate(all_areas):

        logger.info(f"{i}/{len(all_areas)}: Processing {proc_area}")

        proc_address_data = address_data[address_data["area"] == proc_area]
        proc_pop_data = base_pop[base_pop["area"] == proc_area]

        if use_parallel:
            proc_pop_data = assign_household_to_address_remote.remote(
                proc_pop_data, 
                proc_address_data)
        else:
            proc_pop_data = assign_household_to_address(
                proc_pop_data, 
                proc_address_data)
        
        results.append(proc_pop_data)
    

    if use_parallel:
        results = ray.get(results)
        ray.shutdown()

    for result in results:
        base_pop.loc[result.index] = result

    end_time = datetime.utcnow()

    total_mins = round((end_time - start_time).total_seconds() / 60.0 , 3)
    logger.info(f"Processing time (address): {total_mins}")

    return base_pop