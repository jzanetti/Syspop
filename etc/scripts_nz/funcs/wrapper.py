
from os import makedirs
from os.path import exists
from funcs.business.business import (
    create_employee_by_gender_by_sector,
    create_employers_by_employees_number,
    write_employers_by_sector,
)
from funcs.geography.geography import (
    create_geography_hierarchy,
    create_geography_location_area,
    create_address,
    create_geography_location_super_area,
    create_geography_name_super_area,
)
from funcs.household.household import create_household_number
from funcs.population.population import (
    create_age,
    create_ethnicity_and_age,
    create_female_ratio,
    create_population,
    create_socialeconomic,
    create_gender_percentage_for_each_age,
    map_feature_percentage_data_with_age_population_data,
    create_ethnicity_percentage_for_each_age
)
from funcs.postproc import postproc

from funcs.venue.venue import create_hospital, create_school, write_leisures, create_shared_space
from funcs.utils import sort_column_by_names

from os.path import join

from pickle import dump as pickle_dump
from pickle import load as pickle_load
from funcs.commute.commute import create_home_to_work


def create_shared_space_wrapper(workdir: str, space_names: list = ["supermarket", "restaurant"]):
    """Create shared space such as supermarket or restaurant
         Compared to school/hospital, shared space is ther locations where
         people may randomly interact.
         Also, people may also just visit shared space nearby

    Args:
        workdir (str): Working directory
    """
    with open(join(workdir, "geography.pickle"), "rb") as fid:
        geography_data = pickle_load(fid)

    for space_name in space_names:
        proc_data = create_shared_space(space_name, geography_data["location"])
        with open(join(workdir, f"{space_name}.pickle"), 'wb') as fid:
            pickle_dump({space_name: proc_data}, fid)

def create_hospital_wrapper(workdir: str):
    """Create hospital wrapper (e.g., where is the hospital etc.)

    Args:
        workdir (str): Working directory
    """
    with open(join(workdir, "geography.pickle"), "rb") as fid:
        geography_data = pickle_load(fid)

    hopital_data = create_hospital(geography_data["location"])

    with open(join(workdir, "hospital.pickle"), 'wb') as fid:
        pickle_dump({"hospital": hopital_data}, fid)

def create_school_wrapper(workdir: str):
    """Create school wrapper (e.g., where is the school etc.)

    Args:
        workdir (str): Working directory
    """

    with open(join(workdir, "geography.pickle"), "rb") as fid:
        geography_data = pickle_load(fid)

    school_data = create_school(geography_data["location"])
    with open(join(workdir, "school.pickle"), 'wb') as fid:
        pickle_dump({"school": school_data}, fid)


def create_work_wrapper(workdir: str):
    """Create work wrapper (e.g., employees etc.)

    Args:
        workdir (str): Working directory
    """
    with open(join(workdir, "population.pickle"), "rb") as fid:
        population_data = pickle_load(fid)

    with open(join(workdir, "geography.pickle"), "rb") as fid:
        geography_data = pickle_load(fid)

    # employers_by_employees_num = create_employers_by_employees_number(
    #   population_data["age"], 
    #   geography_data["hierarchy"]
    # )

    employers_employees_by_sector = create_employee_by_gender_by_sector(
        population_data["age"], 
        geography_data["hierarchy"]
    )

    with open(join(workdir, "work.pickle"), 'wb') as fid:
        pickle_dump({
        "employee": employers_employees_by_sector[[
            "area", 
            "business_code", 
            "employee_number", 
            "employee_male_ratio",
            "employee_female_ratio"]],
        "employer": employers_employees_by_sector[["area", "business_code", "employer_number"]]
    }, fid)

def create_travel_wrapper(workdir: str):
    create_home_to_work(workdir)

def create_population_wrapper(workdir: str):
    total_population_data = create_population()
    age_data = create_age(total_population_data)

    # get gender data
    female_ratio_data = create_female_ratio()
    gender_data_percentage = create_gender_percentage_for_each_age(
        age_data, female_ratio_data
    )
    gender_data = map_feature_percentage_data_with_age_population_data(
        age_data, gender_data_percentage, check_consistency=True
    )

    # get ethnicity data
    ethnicity_data = create_ethnicity_and_age(total_population_data)
    ethnicity_data_percentage = create_ethnicity_percentage_for_each_age(
        age_data, ethnicity_data
    )
    ethnicity_data = map_feature_percentage_data_with_age_population_data(
        age_data, ethnicity_data_percentage, check_consistency=True
    )

    with open(join(workdir, "population.pickle"), 'wb') as fid:
        pickle_dump({
        "age": age_data,
        "gender": gender_data,
        "ethnicity": ethnicity_data
    }, fid)

def create_geography_wrapper(workdir: str):
    address_data = create_address()
    geography_hierarchy_data = create_geography_hierarchy()
    # geography_location_super_area_data = create_geography_location_super_area(
    #    geography_hierarchy_data
    #)
    #geography_name_super_area_data = create_geography_name_super_area()
    geography_location_area_data = create_geography_location_area()
    socialeconomic_data = create_socialeconomic(geography_hierarchy_data)
    with open(join(workdir, "geography.pickle"), 'wb') as fid:
        pickle_dump({
        "hierarchy": geography_hierarchy_data,
        "location": geography_location_area_data,
        "socialeconomic": socialeconomic_data,
        "address": address_data
    }, fid)