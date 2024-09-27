from logging import getLogger
from math import ceil

from numpy import nan as numpy_nan
from numpy import mean as numpy_mean
from numpy.random import choice as numpy_choice
from pandas import DataFrame, merge

logger = getLogger()


def travel_between_home_and_work(
    commute_dataset: DataFrame,
    base_pop: DataFrame,
    all_employees: dict,
    work_age: dict = {"min": 16, "max": 75}
) -> DataFrame:
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

    base_pop["area_work"] = -9999
    base_pop["travel_mode_work"] = numpy_nan

    working_age_people = base_pop[
        (base_pop["age"] >= work_age["min"]) & (base_pop["age"] <= work_age["max"])
    ]

    travel_methods = [item for item in list(commute_dataset.columns) if not item.endswith("_percentage")]
    travel_methods = [item for item in travel_methods if item not in ["area_home", "area_work"]]

    all_areas_home = list(base_pop["area"].unique())

    results = []

    for i, proc_home_area in enumerate(all_areas_home):
        logger.info(f"Commute (work): {i}/{len(all_areas_home)} ({int(i * 100.0/len(all_areas_home))}%)")

        proc_working_age_people = working_age_people[working_age_people["area"] == proc_home_area]

        proc_working_age_people = proc_working_age_people.sample(
            min([len(proc_working_age_people), all_employees[proc_home_area]])
        )

        proc_working_age_people = assign_people_between_home_and_work(
            proc_working_age_people, 
            commute_dataset, 
            proc_home_area, 
            travel_methods
        )

        results.append(proc_working_age_people)

    for result in results:
        base_pop.loc[result.index] = result

    base_pop["area_work"] = base_pop["area_work"].astype(int)

    return base_pop


def assign_people_between_home_and_work(
    proc_working_age_people: DataFrame,
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

    def _split_people(df: DataFrame, df_commute: DataFrame, travel_methods: list) -> list:
        """
            Splits a DataFrame into multiple DataFrames based on commuting 
                methods and their respective fractions.

            Parameters:
                df (DataFrame): The original DataFrame containing people data.
                df_commute (DataFrame): DataFrame containing commuting data 
                    with methods and areas of work.
                commute_methods (list): List of commuting methods to consider.
    
            Returns:
                list: A list of DataFrames, each representing a split of the 
                original DataFrame based on the given fractions.
        """

        proc_commute_datasets_total_people = df_commute[travel_methods].values.sum()

        fractions = []

        for i in range(len(list(df_commute["area_work"].unique()))):
            fractions.append(
                df_commute[travel_methods].iloc[i].values.sum() / 
                    proc_commute_datasets_total_people)

        split_dfs = []
        remaining_df = df

        for frac in fractions[:-1]:  # Exclude the last fraction
            sample_df = remaining_df.sample(frac=frac)
            split_dfs.append(sample_df)
            remaining_df = remaining_df.drop(sample_df.index)

        # Add the remaining rows to the last split
        split_dfs.append(remaining_df)

        return split_dfs

    proc_commute_dataset = commute_dataset[
        commute_dataset["area_home"] == proc_home_area
    ]

    proc_commute_dataset_work_areas = list(proc_commute_dataset["area_work"].unique())

    proc_working_age_people_in_work_area = _split_people(
        proc_working_age_people, 
        proc_commute_dataset, 
        travel_methods)

    for i, proc_work_area in enumerate(proc_commute_dataset_work_areas):

        proc_commute = proc_commute_dataset[
                proc_commute_dataset["area_work"] == proc_work_area
            ]
        
        for index, proc_people in proc_working_age_people_in_work_area[i].iterrows():
            
            proc_people["area_work"] = proc_work_area

            travel_methods_prob = proc_commute[[item + "_percentage" for item in travel_methods]].values[0]

            try:
                proc_people["travel_mode_work"] = numpy_choice(travel_methods, p=travel_methods_prob)
            except ValueError: # no record from commute data ...
                proc_people["travel_mode_work"] = "Unknown"

            proc_working_age_people.loc[index] = proc_people

    return proc_working_age_people


def get_shared_transport_route_area_indicator(
    geo_level: str, people_data: DataFrame, geog_data: DataFrame
) -> tuple:
    """Get the area level (super area or area) for creating the public transportation route

    Args:
        geo_level (str): required geo level (e.g., super area or area)
        geog_data (DataFrame): geography data

    Returns:
        tuple: updated people data (with new geo level such as super_area if necessary), and area_indicator
    """
    route_area_indicator = "area"
    if geo_level == "super_area":
        people_data = merge(
            people_data, geog_data[["area", "super_area"]], on="area", how="left"
        )
        people_data["area"] = people_data["area"].astype(int)
        route_area_indicator = "super_area"

    people_data["public_transport_trip"] = numpy_nan
    people_data = people_data.reset_index()

    return people_data, route_area_indicator


def add_super_area_to_destination_area(
    people_use_public_transport: DataFrame,
    geog_data: DataFrame,
    proc_travel_purpose: str,
    route_area_indicator: str,
) -> DataFrame:
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
            suffixes=("", f"_{proc_travel_purpose}"),
        )

        # after merge, there are two area_work columns, here drop one of them
        people_use_public_transport = people_use_public_transport.loc[
            :, ~people_use_public_transport.columns.duplicated()
        ]
        people_use_public_transport[f"super_area_{proc_travel_purpose}"] = (
            people_use_public_transport[f"super_area_{proc_travel_purpose}"].astype(int)
        )

    return people_use_public_transport


def shared_transport(
    people_data: DataFrame,
    geog_data: DataFrame,
    geo_level: str = "super_area",
    seat_usage={"work": {"Public_bus": 3.0, "Train": 2.0, "Ferry": 1.5}},
    capacity={"Public_bus": 30, "Train": 100, "Ferry": 75},
) -> DataFrame:
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
    people_data, route_area_indicator = get_shared_transport_route_area_indicator(
        geo_level, people_data, geog_data
    )

    for proc_travel_purpose in list(
        seat_usage.keys()
    ):  # proc_travel_purpose: work etc.
        people_use_public_transport = people_data[
            people_data[f"travel_mode_{proc_travel_purpose}"].isin(
                list(seat_usage[proc_travel_purpose].keys())
            )
        ]

        # besides the original super_area, we would need to have
        # super_area_work column as well (for assigning transport later on)
        people_use_public_transport = add_super_area_to_destination_area(
            people_use_public_transport,
            geog_data,
            proc_travel_purpose,
            route_area_indicator,
        )

        all_vehicle_types = list(
            people_use_public_transport["travel_mode_work"].unique()
        )

        for proc_vehicle_type in all_vehicle_types:
            logger.info(
                f"Processing share transportation: {proc_travel_purpose}, {proc_vehicle_type}"
            )

            proc_people_use_public_transport = people_use_public_transport[
                people_use_public_transport[f"travel_mode_{proc_travel_purpose}"]
                == proc_vehicle_type
            ]

            proc_all_routes = proc_people_use_public_transport[
                [route_area_indicator, f"{route_area_indicator}_{proc_travel_purpose}"]
            ].drop_duplicates()

            for _, proc_route in proc_all_routes.iterrows():
                proc_route_source = proc_route[route_area_indicator]
                proc_route_dest = proc_route[
                    f"{route_area_indicator}_{proc_travel_purpose}"
                ]

                people_on_this_route = proc_people_use_public_transport[
                    (
                        people_use_public_transport[route_area_indicator]
                        == proc_route_source
                    )
                    & (
                        people_use_public_transport[
                            f"{route_area_indicator}_{proc_travel_purpose}"
                        ]
                        == proc_route_dest
                    )
                ]

                number_of_vehicle_trips = ceil(
                    max(
                        [
                            1,
                            (len(people_on_this_route) / capacity[proc_vehicle_type])
                            * seat_usage[proc_travel_purpose][proc_vehicle_type],
                        ]
                    )
                )

                # create vehicles for this route
                all_vehicle_trips = []
                for veh_id in range(number_of_vehicle_trips):
                    all_vehicle_trips.append(
                        f"{proc_travel_purpose}_{proc_vehicle_type}_{proc_route_source}_{proc_route_dest}_{veh_id}"
                    )

                # people on this route will be randomly assigned a vehicle
                people_on_this_route["public_transport_trip"] = numpy_choice(
                    all_vehicle_trips, size=len(people_on_this_route)
                )

                # set the people_on_this_route back to people_data
                people_on_this_route = people_on_this_route.set_index(
                    "index"
                ).rename_axis(None)
                people_on_this_route["index"] = people_on_this_route.index

                people_data.loc[people_on_this_route.index] = people_on_this_route[
                    people_data.columns
                ]

    # drop the index column
    people_data = people_data.drop("index", axis=1)
    if geo_level == "super_area":
        people_data = people_data.drop("super_area", axis=1)

    return people_data
