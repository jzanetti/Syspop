from pandas import DataFrame, merge
from numpy import NaN
from logging import getLogger
from numpy.random import choice as numpy_choice
from math import ceil

import ray

logger = getLogger()


def home_and_work(
    commute_dataset: DataFrame, 
    base_pop: DataFrame, 
    work_age: dict = {"min": 16, "max": 75},
    use_parallel: bool = False,
    n_cpu: int = 8) -> DataFrame:
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
        commute_dataset.drop(columns=["area_home", "area_work", "Total", "Work_at_home"]))

    # Set work_at_home at the end so most people will be assigned to other commute methods
    travel_methods.append("Work_at_home")

    all_areas_home = list(base_pop["area"].unique())

    results = []

    for i, proc_home_area in enumerate(all_areas_home):

        logger.info(f"Commute processing at {i}/{len(all_areas_home)}")

        if i > 50:
            break

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

            unassigned_people = proc_working_age_people[
                proc_working_age_people["area_work"] == -9999]
            
            if len(unassigned_people) == 0:
                continue

            people_num_to_use = min([proc_people_num, len(unassigned_people)])

            proc_working_age_people_sampled = proc_working_age_people[
                proc_working_age_people["area_work"] == -9999].sample(people_num_to_use)

            proc_working_age_people_sampled["area_work"] = int(proc_work_area)
            proc_working_age_people_sampled["travel_mode_work"] = proc_travel_method

            proc_working_age_people.loc[proc_working_age_people_sampled.index] = proc_working_age_people_sampled
    
    return proc_working_age_people


def get_shared_transport_route_area_indicator(geo_level: str, people_data: DataFrame, geog_data: DataFrame) -> tuple:
    """Get the area level (super area or area) for creating the public transportation route

    Args:
        geo_level (str): required geo level (e.g., super area or area)
        geog_data (DataFrame): geography data

    Returns:
        tuple: updated people data (with new geo level such as super_area if necessary), and area_indicator
    """
    route_area_indicator = "area"
    if geo_level == "super_area":
        people_data = merge(people_data, geog_data[["area", "super_area"]], on="area", how="left")
        people_data["area"] = people_data["area"].astype(int)
        route_area_indicator = "super_area"

    people_data["public_transport_trip"] = NaN
    people_data = people_data.reset_index()

    return people_data, route_area_indicator


def add_super_area_to_destination_area(
        people_use_public_transport: DataFrame, 
        geog_data: DataFrame, 
        proc_travel_purpose: str, 
        route_area_indicator: str) -> DataFrame:
    """Add super area to destination if needed, e.g.,
         originally we only have area_work, but in order to add routes at super area, we also need to have super_area_work

    Args:
        people_use_public_transport (DataFrame): People use public transport
        geog_data (DataFrame): Geo data
        proc_travel_purpose (str): work, school etc.
        route_area_indicator (str): whether the route is on area or super_area

    Returns:
        DataFrame: Updated people data
    """
    if route_area_indicator == "super_area":
        people_use_public_transport = merge(
            people_use_public_transport, 
            geog_data[["area", "super_area"]], 
            left_on=f"area_{proc_travel_purpose}", 
            right_on="area", 
            how="left", 
            suffixes=("", f"_{proc_travel_purpose}"))
        
        # after merge, there are two area_work columns, here drop one of them
        people_use_public_transport = people_use_public_transport.loc[
            :, ~people_use_public_transport.columns.duplicated()]
        people_use_public_transport[f"super_area_{proc_travel_purpose}"] = people_use_public_transport[
            f"super_area_{proc_travel_purpose}"].astype(int)
    
    return people_use_public_transport


def shared_transport(
        people_data: DataFrame,
        geog_data: DataFrame,
        geo_level: str = "super_area",
        seat_usage = {
            "work": {"Public_bus": 3.0, "Train": 2.0, "Ferry": 1.5}},
        capacity = {
            "Public_bus": 30, "Train": 100, "Ferry": 75
        }) -> DataFrame:
    """Assign share transport such as bus, ferry or train to 
    people who take public transportation

    Args:
        people_data (DataFrame): People data to be processed (with travel_mode_** such as travel_model_work)
        seat_usage (dict, optional): Average seat occupy ratio (e.g., how many people may sit on the same seat for one day). 
            Defaults to {"Bus": 0.3, "Train": 0.3, "Ferry": 0.9}.
        capacity (dict, optional): Average capacility for each vehicles
            Defaults to {"Public_bus": 30, "Train": 100, "Ferry": 75}.

    Returns:
        DataFrame: Updated people data
    """

    # Step 1: decide at which area we want to make the route plan.
    #         Originally the data only stored at the area level (e.g., area and area_work) but this level
    #         might be too small for route planning (e.g., too many short trips therefore too many buses),
    #         therefore we may want to add another level (e.g., super_area) in the people_data
    people_data, route_area_indicator = get_shared_transport_route_area_indicator(geo_level, people_data, geog_data)

    for proc_travel_purpose in list(seat_usage.keys()): # proc_travel_purpose: work etc.
        
        people_use_public_transport = people_data[people_data[
            f"travel_mode_{proc_travel_purpose}"].isin(
                list(seat_usage[proc_travel_purpose].keys()))]

        # besides the original super_area, we would need to have 
        # super_area_work column as well (for assigning transport later on)
        people_use_public_transport = add_super_area_to_destination_area(
            people_use_public_transport, 
            geog_data, 
            proc_travel_purpose, 
            route_area_indicator)

        all_vehicle_types = list(people_use_public_transport["travel_mode_work"].unique())

        for proc_vehicle_type in all_vehicle_types:

            logger.info(f"Processing share transportation: {proc_travel_purpose}, {proc_vehicle_type}")

            proc_people_use_public_transport = people_use_public_transport[
                people_use_public_transport[
                    f"travel_mode_{proc_travel_purpose}"] == proc_vehicle_type]

            proc_all_routes = proc_people_use_public_transport[[route_area_indicator, f"{route_area_indicator}_{proc_travel_purpose}"]].drop_duplicates()

            for _, proc_route in proc_all_routes.iterrows():

                proc_route_source = proc_route[route_area_indicator]
                proc_route_dest = proc_route[f"{route_area_indicator}_{proc_travel_purpose}"]

                people_on_this_route = proc_people_use_public_transport[
                    (people_use_public_transport[route_area_indicator] == proc_route_source) & 
                    (people_use_public_transport[f"{route_area_indicator}_{proc_travel_purpose}"] == proc_route_dest)
                ]

                number_of_vehicle_trips = ceil(max([1, (len(people_on_this_route) / capacity[proc_vehicle_type]) * seat_usage[
                    proc_travel_purpose][proc_vehicle_type]]))
                
                # create vehicles for this route
                all_vehicle_trips = []
                for veh_id in range(number_of_vehicle_trips):
                    all_vehicle_trips.append(
                        f"{proc_travel_purpose}_{proc_vehicle_type}_{proc_route_source}_{proc_route_dest}_{veh_id}")
                
                # people on this route will be randomly assigned a vehicle
                people_on_this_route["public_transport_trip"] = numpy_choice(
                    all_vehicle_trips, size=len(people_on_this_route))
                
                # set the people_on_this_route back to people_data
                people_on_this_route = people_on_this_route.set_index("index").rename_axis(None)
                people_on_this_route["index"] = people_on_this_route.index

                people_data.loc[people_on_this_route.index] = people_on_this_route[people_data.columns]

    # drop the index column
    people_data = people_data.drop("index", axis=1)
    if geo_level == "super_area":
        people_data = people_data.drop("super_area", axis=1)

    return people_data



    

                


