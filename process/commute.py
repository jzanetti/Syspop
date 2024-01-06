from pandas import DataFrame
from numpy import NaN
from logging import getLogger
from datetime import datetime

import ray

logger = getLogger()

def commute_wrapper(
    commute_dataset: DataFrame, 
    base_pop: DataFrame,
    use_parallel: bool = False,
    n_cpu: int = 8,
    work_age: dict = {"min": 18, "max": 65}) -> DataFrame:
    """Adding commute data wrapper

    Args:
        base_pop (DataFrame): Base population data
        commute_dataset (DataFrame): Comute data
        work_age (dict, optional): Working age range. Defaults to {"min": 18, "max": 65}.
    """

    start_time = datetime.utcnow()

    base_pop = home_and_work(commute_dataset, base_pop, work_age, use_parallel, n_cpu)

    end_time = datetime.utcnow()

    total_mins = round((end_time - start_time).total_seconds() / 60.0 , 3)
    logger.info(f"Processing time (household): {total_mins}")

    return base_pop


def home_and_work(
        commute_dataset: DataFrame, 
        base_pop: DataFrame, 
        work_age: dict,
        use_parallel: bool,
        n_cpu: int) -> DataFrame:
    """Assign commute data (home to work) to base population

        - step 1: go through each area (home) from base population
        - step 2: for each combination: 
             - work area from commute dataset
             - home area from base population
            we sample the number of people based on the commute dataset

    Args:
        base_pop (DataFrame): Base population data
        commute_dataset (DataFrame): Comute data

    Returns:
        Dataframe: the population with commute
    """

    if use_parallel:
        ray.init(num_cpus=n_cpu, include_dashboard=False)

    base_pop["area_work"] = -9999
    base_pop["travel_mode_work"] = NaN

    working_age_people = base_pop[
        (base_pop["age"] >= work_age["min"]) & 
        (base_pop["age"] <= work_age["max"])]

    travel_methods = list(
        commute_dataset.drop(columns=["area_home", "area_work", "Total"]))

    all_areas_home = list(base_pop["area"].unique())

    results = []
    for i, proc_home_area in enumerate(all_areas_home):

        logger.info(f"Commute processing at {i}/{len(all_areas_home)}")

        if use_parallel:
            proc_working_age_people = assign_people_between_home_and_work_remote.remote(
                working_age_people,
                commute_dataset,
                proc_home_area,
                travel_methods)
        else:
            proc_working_age_people = assign_people_between_home_and_work(
                working_age_people,
                commute_dataset,
                proc_home_area,
                travel_methods)
        
        results.append(proc_working_age_people)

    if use_parallel:
        results = ray.get(results)
        ray.shutdown()

    for result in results:
        base_pop.loc[result.index] = result

    base_pop["area_work"] = base_pop["area_work"].astype(int)

    return base_pop


@ray.remote
def assign_people_between_home_and_work_remote(    
        working_age_people: DataFrame,
        commute_dataset: DataFrame,
        proc_home_area: int,
        travel_methods: list):
    """Assign/sample people between home and work (for parallel processing)

    Args:
        working_age_people (DataFrame): People at working age
        commute_dataset (DataFrame): Commute dataset
        proc_home_area (int): Home area to be used
        travel_methods (list): All travel methods (e.g., bus, car etc.)

    Returns:
        DataFrame: Updated working age people
    """
    return assign_people_between_home_and_work(
        working_age_people,
        commute_dataset,
        proc_home_area,
        travel_methods)


def assign_people_between_home_and_work(
    working_age_people: DataFrame,
    commute_dataset: DataFrame,
    proc_home_area: int,
    travel_methods: list
) -> DataFrame:
    """Assign/sample people between home and work

    Args:
        working_age_people (DataFrame): People at working age
        commute_dataset (DataFrame): Commute dataset
        proc_home_area (int): Home area to be used
        travel_methods (list): All travel methods (e.g., bus, car etc.)

    Returns:
        DataFrame: Updated working age people
    """
    proc_working_age_people = working_age_people[
        working_age_people["area"] == proc_home_area]

    proc_commute_dataset = commute_dataset[
        commute_dataset["area_home"] == proc_home_area]

    for proc_work_area in list(proc_commute_dataset["area_work"].unique()):

        for proc_travel_method in travel_methods:

            proc_people_num = proc_commute_dataset[
                proc_commute_dataset["area_work"] == proc_work_area][
                    proc_travel_method].values[0]
            
            # try:
            proc_working_age_people_sampled = proc_working_age_people[
                proc_working_age_people["area_work"] == -9999].sample(proc_people_num)

            proc_working_age_people_sampled["area_work"] = int(proc_work_area)
            proc_working_age_people_sampled["travel_mode_work"] = proc_travel_method

            proc_working_age_people.loc[proc_working_age_people_sampled.index] = proc_working_age_people_sampled
            # except ValueError: # just in case if not enough people to sample from
            #    pass
    
    return proc_working_age_people
                


