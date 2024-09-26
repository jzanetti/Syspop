from datetime import datetime
from logging import getLogger

from numpy import nan as numpy_nan
from pandas import DataFrame, concat, merge

from uuid import uuid4

logger = getLogger()


def create_school_names(school_data: DataFrame) -> DataFrame:
    """Create school name for each school following the pattern:
        {sector}_{area}_{id}

    Args:
        school_data (DataFrame): _description_
        # household_247900_5444c8
    """
    all_areas = list(school_data["area"].unique())
    school_data["school_name"] = None
    for proc_area in all_areas:
        proc_schools = school_data[school_data["area"] == proc_area]
        proc_schools["school_name"] = [str(uuid4())[:6] for _ in range(len(proc_schools))]
        proc_schools["school_name"] = (
            "school_"
            + proc_schools["sector"]
            + "_"
            + proc_schools["area"].astype(str)
            + "_"
            + proc_schools["school_name"]
        )
        school_data.loc[proc_schools.index, "school_name"] = proc_schools.school_name

    return school_data


def obtain_available_schools(
    school_data: DataFrame,
    proc_people_location: dict,
    proc_people_age: int,
    possile_area_levels: list,
    processed_school: list,
) -> list or None:
    """Obtain available schools from area -> super_area -> region

    Args:
        school_data (DataFrame): _description_
        proc_people_location (dict): _description_
        proc_people_age (int): _description_
        possile_area_levels (list): _description_
        processed_school (list): _description_

    Returns:
        list or None: _description_
    """

    def _create_area_list(
        initial_value: int, range_value: int = 100, area_range_interval: int = 10
    ):
        """Return neighbooring areas, e.g., for 4000, we would have 4000, 3999, 4001, 3998, 4002, ...

        Args:
            initial_value (int): initial value, e.g., 400
            range_value (int, optional): _description_. Defaults to 30.
        """
        area_list = [initial_value]

        if range_value is not None:
            for i in range(0, range_value, area_range_interval):
                area_list.append(initial_value + i)
                area_list.append(initial_value - i)

        return area_list[2:]

    for area_key in possile_area_levels:

        if proc_people_age < 12:  # primary, kindergarden etc.
            if area_key == "area":
                area_range_value = 50
                area_range_interval = 10
            elif area_key == "super_area":
                area_range_value = 20
                area_range_interval = 10
            elif area_key == "region":
                area_range_value = None
                area_range_interval = None
        else:
            if area_key == "area":  # high school etc.
                area_range_value = 100
                area_range_interval = 10
            elif area_key == "super_area":
                area_range_value = 50
                area_range_interval = 10
            elif area_key == "region":
                area_range_value = None
                area_range_interval = None

        possible_area_values = _create_area_list(
            proc_people_location[area_key],
            range_value=area_range_value,
            area_range_interval=area_range_interval,
        )

        for _, possible_area_value in enumerate(possible_area_values):

            proc_schools = school_data[
                (
                    (school_data[area_key] == possible_area_value)
                    & (school_data["age_min"] <= proc_people_age)
                    & (school_data["age_max"] >= proc_people_age)
                )
            ]

            proc_schools = proc_schools[
                ~proc_schools["school_name"].isin(processed_school)
            ]

            if len(proc_schools) > 0:
                return proc_schools

    return None


def get_a_school(
    schools_to_choose: DataFrame, school_assigned_people: dict, use_random: bool = False
) -> DataFrame:
    """Select a school depending on the occupancy ~
        the school with the smallest occupancy will be selected

    Args:
        schools_to_choose (DataFrame): School to choose from
        school_assigned_people (dict): The number of people in each school
        use_random (bool, optional): _description_. Defaults to False.

    Returns:
        DataFrame: _description_
    """

    if use_random:
        return schools_to_choose.sample(n=1)

    proc_school_ratio = {}
    for _, proc_school in schools_to_choose.iterrows():
        proc_school_name = proc_school["school_name"]
        proc_school_ratio[proc_school_name] = (
            school_assigned_people[proc_school_name] / proc_school["max_students"]
        )
    selected_school_name = min(proc_school_ratio, key=proc_school_ratio.get)
    return schools_to_choose[schools_to_choose["school_name"] == selected_school_name]


def school_and_kindergarten_wrapper(
    data_type: str,  # school or kindergarten
    school_data: DataFrame,
    pop_data: DataFrame,
    address_data: DataFrame,
    geography_hierarchy_data: DataFrame,
    assign_address_flag: bool = False,
    possile_area_levels: list = ["area", "super_area", "region"],
    max_students_factor: int = 1.5,
) -> DataFrame:
    """Wrapper to assign school to individuals (Note this is a very slow process)
    We are not able to use multiprocessing since the school data
    (e.g., how many people already in the school) is dynamically updated

    Args:
        school_data (DataFrame): School data
        pop_data (DataFrame): Base population data
        geography_hierarchy_data (DataFrame): Geography hierarchy data

    Returns:
        DataFrame: Updated population
    """
    start_time = datetime.utcnow()
    pop_data[data_type] = numpy_nan

    # pop_data = pop_data.drop(columns=["super_area", "region"])
    school_data = create_school_names(school_data)

    # ----------------------------------
    # School data update
    # ----------------------------------
    # make sure that the min age for kindergarden is 2
    school_data.loc[school_data["sector"] == "kindergarten", "age_min"] = 2
    school_data = school_data[school_data["max_students"] > 0]

    school_data["max_students"] = school_data["max_students"] * max_students_factor

    # make sure that the max students for a school is not less than 50
    school_data["max_students"] = school_data["max_students"].apply(
        lambda x: max(x, 50)
    )
    school_data["max_students"] = school_data["max_students"].astype(int)
    # make sure that the primary school always has enough capacity
    # school_data.loc[
    #    school_data["sector"].isin(["primary", "kindergarten"]), "max_students"
    # ] = 9999

    age_range = {
        "min": school_data["age_min"].min(),
        "max": school_data["age_max"].max(),
    }
    school_population = pop_data[
        (pop_data["age"] > age_range["min"]) & (pop_data["age"] < age_range["max"])
    ]
    school_population = school_population.reset_index()

    # attach super area to school population
    school_population = merge(
        school_population,
        geography_hierarchy_data[["area", "super_area", "region"]],
        on="area",
        how="left",
    )
    school_data = merge(
        school_data,
        geography_hierarchy_data[["area", "super_area", "region"]],
        on="area",
        how="left",
    )

    pop_data = merge(
        pop_data,
        geography_hierarchy_data[["area", "super_area", "region"]],
        on="area",
        how="left",
    )

    total_school_people = len(school_population)

    school_assigned_people = {}
    for proc_school_name in list(school_data["school_name"].unique()):
        school_assigned_people[proc_school_name] = 0

    processed_people = []
    full_school = []

    if assign_address_flag:
        school_address = {"name": [], "latitude": [], "longitude": [], "area": []}

    batch_missing_people = 0
    total_missing_people = 0
    for i in range(total_school_people):
        proc_people = school_population.sample(n=1)

        proc_people_location = {
            "area": proc_people["area"].values[0],
            "super_area": proc_people["super_area"].values[0],
            "region": proc_people["region"].values[0],
        }
        proc_people_age = proc_people["age"].values[0]
        if i % 1000 == 0.0:
            logger.info(
                f"{data_type} processing: finshed: {i}/{total_school_people}: "
                + f"{round(100*i/total_school_people, 3)}%"
                + f"(batch missing {round(batch_missing_people * 100.0/1000.0, 3)}%, "
                + f"total missing {round(total_missing_people * 100.0/total_school_people, 3)}%)"
            )
            batch_missing_people = 0

        while True:

            proc_schools = obtain_available_schools(
                school_data,
                proc_people_location,
                proc_people_age,
                possile_area_levels,
                full_school,
            )

            if proc_schools is None:
                school_population = school_population[
                    school_population["index"] != proc_people["index"].values[0]
                ]
                # proc_people_area = proc_people.area.values[0]
                # proc_people_id = proc_people.index.values[0]
                # logger.info(
                #    f"Not able to find any {data_type} data in {proc_people_area}, "
                #    + f"people {proc_people_id} missed out ..."
                # )
                batch_missing_people += 1
                total_missing_people += 1
                break

            proc_school = get_a_school(proc_schools, school_assigned_people)

            proc_school_name = proc_school["school_name"].values[0]
            students_in_this_school = school_assigned_people[proc_school_name]

            if students_in_this_school < proc_school["max_students"].values[0]:
                if assign_address_flag and (proc_school_name not in school_address):
                    school_address["name"].append(proc_school_name)
                    school_address["latitude"].append(
                        float(proc_school["latitude"].values[0])
                    )
                    school_address["longitude"].append(
                        float(proc_school["longitude"].values[0])
                    )
                    school_address["area"].append(float(proc_school["area"].values[0]))

                proc_people[data_type] = proc_school_name
                processed_people.append(proc_people)
                school_population = school_population[
                    school_population["index"] != proc_people["index"].values[0]
                ]

                school_assigned_people[proc_school_name] += 1
                if (
                    school_assigned_people[proc_school_name]
                    == proc_school["max_students"].values[0]
                ):
                    full_school.append(proc_school_name)
                break

    logger.info(f"Combining {data_type} dataset ...")

    if len(processed_people) > 0:
        processed_school_population = concat(processed_people, ignore_index=True)
        processed_school_population.set_index("index", inplace=True)
        processed_school_population.index.name = None

        pop_data.loc[processed_school_population.index] = processed_school_population[
            pop_data.columns
        ]

    pop_data = pop_data.drop(columns=["super_area", "region"])

    if assign_address_flag:
        school_address_df = DataFrame.from_dict(school_address)
        school_address_df["type"] = data_type
        address_data = concat([address_data, school_address_df.drop_duplicates()])

    logger.info(
        f"{data_type} processing runtime: {round(((datetime.utcnow() - start_time).total_seconds()) / 60.0, 3)}"
    )

    address_data["area"] = address_data["area"].astype(int)
    return pop_data, address_data
