from pandas import DataFrame, merge, concat
from numpy import NaN
from logging import getLogger
from datetime import datetime


logger = getLogger()
def create_school_names(school_data: DataFrame) -> DataFrame:
    """Create school name for each school following the pattern:
        {sector}_{area}_{id}

    Args:
        school_data (DataFrame): _description_
    """
    school_data["school_name"] = school_data.groupby("area").cumcount().astype(str)
    school_data['school_name'] = school_data["area"].astype(str) + "_" + school_data["sector"] + "_" + school_data["school_name"]

    return school_data


def school_wrapper(        
        school_data: DataFrame, 
        pop_data: DataFrame,
        address_data: DataFrame,
        geography_hierarchy_data: DataFrame,
        assign_address_flag: bool = False) -> DataFrame:
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
    pop_data["school"] = NaN

    # pop_data = pop_data.drop(columns=["super_area", "region"])
    school_data = create_school_names(school_data)

    age_range = {"min": school_data["age_min"].min(), "max": school_data["age_max"].max()}
    school_population = pop_data[
        (pop_data["age"] >= age_range["min"]) & (pop_data["age"] <= age_range["max"])]
    school_population = school_population.reset_index()

    # attach super area to school population
    school_population = merge(school_population, geography_hierarchy_data[
        ["area", "super_area", "region"]], on="area", how="left")
    school_data = merge(school_data, geography_hierarchy_data[
        ["area", "super_area", "region"]], on="area", how="left")
    
    pop_data = merge(pop_data, geography_hierarchy_data[
        ["area", "super_area", "region"]], on="area", how="left")

    total_school_people = len(school_population)

    school_assigned_people = {}
    for proc_school_name in list(school_data["school_name"].unique()):
        school_assigned_people[proc_school_name] = 0

    processed_people = []

    if assign_address_flag:
        school_address = {"name": [], "latitude": [], "longitude": []}

    for i in range(total_school_people):

        proc_people = school_population.iloc[[i]]
 
        proc_people_location = {
            "area": proc_people["area"].values[0], 
            "super_area": proc_people["super_area"].values[0], 
            "region": proc_people["region"].values[0]}
        proc_people_age = proc_people["age"].values[0]
        if i % 1000 == 0.0:
            logger.info(f"School processing: finshed: {i}/{total_school_people}: {round(100*i/total_school_people, 3)}%")

        tries_num = 0
    
        while tries_num < 5:
            
            for area_key in ["area", "super_area", "region"]:
                proc_schools = school_data[
                    (
                        (school_data[area_key] == proc_people_location[area_key]) & 
                        (school_data["age_min"] <= proc_people_age) &
                        (school_data["age_max"] >= proc_people_age)
                    )]
                if len(proc_schools) > 0:
                    break

            proc_school = proc_schools.sample(n=1)
            proc_school_name = proc_school["school_name"].values[0]
            students_in_this_school = school_assigned_people[proc_school_name]
            
            if students_in_this_school <= proc_school["max_students"].values[0]:

                # store the address of a school
                if assign_address_flag and (proc_school_name not in school_address):
                    school_address["name"].append(proc_school_name)
                    school_address["latitude"].append(float(proc_school["latitude"].values[0]))
                    school_address["longitude"].append(float(proc_school["longitude"].values[0]))

                proc_people["school"] = proc_school_name
                processed_people.append(proc_people)
                break

    logger.info("Combining school dataset ...")
    processed_school_population = concat(processed_people, ignore_index=True)
    processed_school_population.set_index("index", inplace=True)
    processed_school_population.index.name = None
    pop_data.loc[processed_school_population.index] = processed_school_population[pop_data.columns]

    pop_data = pop_data.drop(columns=["super_area", "region"])

    if assign_address_flag:
        school_address_df = DataFrame.from_dict(school_address)
        school_address_df["type"] = "school"
        address_data = concat([address_data, school_address_df])

    logger.info(f"School processing runtime: {round(((datetime.utcnow() - start_time).total_seconds()) / 60.0, 3)}")

    return pop_data, address_data