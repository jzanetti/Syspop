

from pandas import DataFrame
from process.base_pop import base_pop_wrapper
from process.household import household_wrapper
from process.social_economic import social_economic_wrapper
from process.work import work_and_commute_wrapper
from process.school import school_wrapper
from process.hospital import hospital_wrapper
from process.shared_space import shared_space_wrapper

from os.path import exists, join

from os import makedirs

from copy import deepcopy

from pickle import load as pickle_load
from pickle import dump as pickle_dump


def create_base_pop(
        tmp_data_path: str, 
        pop_gender: DataFrame,
        pop_ethnicity: DataFrame,
        syn_areas: list,
        use_parallel: bool,
        ncpu: int):
    """Creating base population, e.g., the population only contains
        - id
        - area
        - age
        - gender
        - ethnicity

    Args:
        tmp_data_path (str): where to save the population data
        rewrite_base_pop (bool): if the base population exists, if we rewrite it
        pop_gender (DataFrame): gender census
        pop_ethnicity (DataFrame): ethnicity census
        syn_areas (list): area to be processed
        use_parallel (bool): If use parallel processing
        ncpu (int): Number of CPUs
    """
    synpop, synadd = base_pop_wrapper(
        pop_gender, 
        pop_ethnicity,
        syn_areas,
        use_parallel=use_parallel,
        n_cpu=ncpu)

    with open(tmp_data_path, 'wb') as fid:
        pickle_dump({"synpop": synpop, "synadd": synadd}, fid)

def create_household(
        tmp_data_path: str, 
        household_data: DataFrame, 
        geo_address_data: DataFrame, 
        use_parallel: bool, 
        n_cpu: int):
    """Create household data, and if required, create household address as well

    Args:
        tmp_data_path (str): where to save the population data
        household_data (DataFrame): household census
        geo_address_data (DataFrame): geo address data (for every entity in an area)
        use_parallel (bool): If use parallel processing
        ncpu (int): Number of CPUs
    """
    with open(tmp_data_path, "rb") as fid:
        base_pop = pickle_load(fid)

    base_pop, base_address = household_wrapper(
        household_data, 
        base_pop["synpop"],
        base_pop["synadd"],
        geo_address_data=geo_address_data,
        use_parallel=use_parallel, 
        n_cpu=n_cpu)

    with open(tmp_data_path, 'wb') as fid:
        pickle_dump({
            "synpop": base_pop, 
            "synadd": base_address}, fid)

def create_socialeconomics(tmp_data_path: str, socialeconomic_data: DataFrame):
    """Create social ecnomics for each area

    Args:
        tmp_data_path (str): where to save the population data
        socialeconomic_data (DataFrame): social economics data to be used
    """
    with open(tmp_data_path, "rb") as fid:
        base_pop = pickle_load(fid)

    base_pop_out = social_economic_wrapper(
        deepcopy(base_pop["synpop"]), 
        socialeconomic_data)

    with open(tmp_data_path, 'wb') as fid:
        pickle_dump({"synpop": base_pop_out, "synadd": base_pop["synadd"]}, fid)

def create_work(
        tmp_data_path: str, 
        work_data: DataFrame, 
        home_to_work_commute_data: DataFrame, 
        geo_hierarchy_data: DataFrame, 
        geo_address_data: DataFrame, 
        use_parallel: bool, 
        n_cpu: int):
    """Create work/company and if required, create the company address as well

    Args:
        tmp_data_path (str): where to save the population data
        work_data (DataFrame): Work related data, e.g, employer number of each business etc.
        home_to_work_commute_data (DataFrame): How people travel between work and home
        geo_hierarchy_data (DataFrame): geo hirarchy data, e.g., region -> super_area -> area
        geo_address_data (DataFrame): geo address data (address for every entity in an area)
        use_parallel (bool): If use parallel processing
        ncpu (int): Number of CPUs
    """
    with open(tmp_data_path, "rb") as fid:
        base_pop = pickle_load(fid)

    base_pop, base_address = work_and_commute_wrapper(
        work_data,
        base_pop["synpop"],
        base_pop["synadd"],
        home_to_work_commute_data,
        geo_hierarchy_data,
        geo_address_data=geo_address_data,
        use_parallel=use_parallel,
        n_cpu=n_cpu)

    with open(tmp_data_path, 'wb') as fid:
        pickle_dump({"synpop": base_pop, "synadd": base_address}, fid)

def create_school(
        tmp_data_path: str, 
        school_data: DataFrame, 
        geo_hierarchy_data: DataFrame, 
        assign_address_flag: bool):
    """Create school information, if required, school address as well

    Args:
        tmp_data_path (str): where to save the population data
        school_data (DataFrame): school census such as the school location, capacity
        geo_hierarchy_data (DataFrame): geo hirarchy data, e.g., region -> super_area -> area
        assign_address_flag (bool): if write school address
    """
    with open(tmp_data_path, "rb") as fid:
        base_pop = pickle_load(fid)

    base_pop, base_address = school_wrapper(
        school_data,
        base_pop["synpop"],
        base_pop["synadd"], 
        geo_hierarchy_data,
        assign_address_flag=assign_address_flag)

    with open(tmp_data_path, 'wb') as fid:
        pickle_dump({
            "synpop": base_pop, 
            "synadd": base_address}, fid)

def create_hospital(tmp_data_path: str, hospital_data: DataFrame, geo_location: DataFrame, assign_address_flag: bool):
    """Create hospital data

    Args:
        tmp_data_path (str): where to save the population data
        hospital_data (DataFrame): hospital data to be used
        geo_location (DataFrame): geo location data, e.g., lat/lon for each area
        assign_address_flag (bool): if write hospital address
    """
    with open(tmp_data_path, "rb") as fid:
        base_pop = pickle_load(fid)

    base_pop, base_address = hospital_wrapper(
        hospital_data,
        base_pop["synpop"],
        base_pop["synadd"],
        geo_location,
        assign_address_flag=assign_address_flag)

    with open(tmp_data_path, 'wb') as fid:
        pickle_dump(
            {
                "synpop": base_pop, 
                "synadd": base_address
            }, fid)

def create_supermarket(
        tmp_data_path: str, 
        supermarket_data: DataFrame, 
        geo_location: DataFrame, 
        assign_address_flag: bool):
    """Create supermarket data, if required, address of each supermarket as well

    Args:
        tmp_data_path (str): where to save the population data
        supermarket_data (DataFrame): supermarket data to be used
        geo_location (DataFrame): geo location data, e.g., lat/lon for each area
        assign_address_flag (bool): if write hospital address
    """
    with open(tmp_data_path, "rb") as fid:
        base_pop = pickle_load(fid)

    base_pop, address_data = shared_space_wrapper(
        "supermarket",
        supermarket_data, 
        base_pop["synpop"],
        base_pop["synadd"],
        geo_location,
        num_nearest=2,
        assign_address_flag=assign_address_flag)

    with open(tmp_data_path, 'wb') as fid:
        pickle_dump(
            {"synpop": base_pop, 
             "synadd": address_data}, fid)
        
def create_restauraunt(tmp_data_path: str, restaurant_data: DataFrame, geo_location: DataFrame, assign_address_flag: bool):
    """Create synthetic restaurant, if requred, address as well

    Args:
        tmp_data_path (str): where to save the population data
        restaurant_data (DataFrame): restaurant_data to be used
        geo_location (DataFrame): geo location data, e.g., lat/lon for each area
        assign_address_flag (bool): if write hospital address
    """
    with open(tmp_data_path, "rb") as fid:
        base_pop = pickle_load(fid)

    base_pop, address_data = shared_space_wrapper(
        "restaurant",
        restaurant_data, 
        base_pop["synpop"],
        base_pop["synadd"],
        geo_location,
        num_nearest=4,
        assign_address_flag=assign_address_flag)

    with open(tmp_data_path, 'wb') as fid:
        pickle_dump({"synpop": base_pop, "synadd": address_data}, fid)
