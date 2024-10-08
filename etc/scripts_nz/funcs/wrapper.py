from logging import getLogger
from os import makedirs
from os.path import exists, join
from pickle import dump as pickle_dump
from pickle import load as pickle_load

# from funcs.business.business import create_employee_by_gender_by_sector
# from funcs.commute.commute import create_home_to_work
"""
from funcs.geography.geography import (
    create_address,
    create_geography_hierarchy,
    create_geography_location_area,
    create_geography_location_super_area,
    create_geography_name_super_area,
)
"""
from funcs.household.household import (
    create_household_and_dwelling_number
)
from funcs.others.health import add_mmr
from funcs.others.immigration import add_birthplace
from funcs.population.population import (
    create_age_based_on_scaler,
    create_ethnicity_ratio,
    create_ethnicity_percentage_for_each_age,
    create_female_ratio,
    create_gender_percentage_for_each_age,
    create_base_population,
    create_nzdep,
    map_feature_percentage_data_with_age_population_data,
    read_population_structure
)
from funcs.postproc import postproc
from funcs.utils import sort_column_by_names
from funcs.venue.venue import (
    create_hospital,
    create_kindergarten,
    create_school,
    create_osm_space
)

from pickle import load as pickle_load

from funcs.preproc import (
    _read_raw_address, 
    _read_raw_geography_hierarchy, 
    _read_raw_geography_location_area, 
    _read_raw_travel_to_work, 
    _read_raw_employer_employee_data)

logger = getLogger()


def create_household_wrapper(workdir: str, input_cfg: dict):
    """
    Creates a household dataset based on the provided household composition and saves it to a file.

    Parameters:
        workdir (str): The working directory where the output file will be saved.
        input_cfg (dict): The configuration dictionary containing household parameters.

    The function performs the following steps:
    1. Generates household and dwelling number data based on the household composition specified in the configuration.
    2. Saves the generated household data to a pickle file in the specified working directory.

    The generated household data includes:
    - Household composition and dwelling numbers

    Returns:
        None
    """

    hhd_data = create_household_and_dwelling_number(
        input_cfg["household"]["household_composition"])
    hhd_data['percentage'] = hhd_data.groupby("area")["num"].transform(
        lambda x: x / x.sum())

    with open(join(workdir, "household.pickle"), "wb") as fid:
        pickle_dump({"household": hhd_data}, fid)


def create_shared_space_wrapper(
    workdir: str, input_cfg: dict, space_names: list = ["supermarket", "restaurant", "pharmacy"]
):
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
        print(f"Shared space: {space_name} ...")
        proc_data = create_osm_space(input_cfg["venue"][space_name], geography_data["location"])
        with open(join(workdir, f"{space_name}.pickle"), "wb") as fid:
            pickle_dump({space_name: proc_data}, fid)


def create_others_wrapper(workdir: str, input_cfg: dict):
    """Createing others

    Args:
        workdir (str): Working directory
    """
    mmr_data = add_mmr(input_cfg["others"]["mmr_vaccine"])
    birthplace_data = add_birthplace(input_cfg["others"]["birthplace"])
    with open(join(workdir, "others.pickle"), "wb") as fid:
        pickle_dump({"mmr": mmr_data, "birthplace": birthplace_data}, fid)


def create_hospital_wrapper(workdir: str, input_cfg: dict):
    """Create hospital wrapper (e.g., where is the hospital etc.)

    Args:
        workdir (str): Working directory
    """
    with open(join(workdir, "geography.pickle"), "rb") as fid:
        geography_data = pickle_load(fid)

    hopital_data = create_hospital(input_cfg["venue"]["hospital"], geography_data["location"])

    with open(join(workdir, "hospital.pickle"), "wb") as fid:
        pickle_dump({"hospital": hopital_data}, fid)


def create_kindergarten_wrapper(workdir: str, input_cfg: dict):
    """Create kindergarten wrapper (e.g., where is the kindergarten etc.)

    Args:
        workdir (str): Working directory
    """
    kindergarten_data = create_kindergarten(input_cfg["venue"]["kindergarten"])
    with open(join(workdir, "kindergarten.pickle"), "wb") as fid:
        pickle_dump({"kindergarten": kindergarten_data}, fid)


def create_school_wrapper(workdir: str, input_cfg: dict):
    """Create school wrapper (e.g., where is the school etc.)

    Args:
        workdir (str): Working directory
    """

    with open(join(workdir, "geography.pickle"), "rb") as fid:
        geography_data = pickle_load(fid)

    school_data = create_school(input_cfg["venue"]["school"], geography_data["location"])
    with open(join(workdir, "school.pickle"), "wb") as fid:
        pickle_dump({"school": school_data}, fid)


def create_work_wrapper(workdir: str, input_cfg: dict):
    """Create work wrapper (e.g., employees etc.)

    The output looks like:
                area business_code  employer_number  employee_number  employee_number_percentage
        0       100100             A               93              190                    0.429864
        1       100200             A              138              190                    0.287879
        2       100300             A                6               25                    0.438596
        3       100400             A               57               50                    0.287356
        4       100500             A               57               95                    0.242347
        ......
    For each area it is sth like:
                area business_code  employer_number  employee_number  employee_number_percentage
        0       100100             A               93              190                    0.429864
        20082   100100             C                0                6                    0.013575
        56927   100100             E               12                6                    0.013575
        ....
        132366  100100             I                6                9                    0.020362
        159241  100100             K                6                0                    0.000000

    Args:
        workdir (str): Working directory
    """
    data = _read_raw_employer_employee_data(input_cfg["employment"]["employer_employee_num"])
    
    # Calculate the total number of employees per area
    total_employees_per_area = data.groupby('area')['employee_number'].transform('sum')

    # Calculate the employee number percentage
    data['employee_number_percentage'] = data['employee_number'] / total_employees_per_area

    with open(join(workdir, "work.pickle"), "wb") as fid:
        pickle_dump(
            {
                "employee": data[
                    [
                        "area",
                        "business_code",
                        "employee_number",
                        "employee_number_percentage"
                    ]
                ],
                "employer": data[
                    ["area", "business_code", "employer_number"]
                ],
            },
            fid,
        )

def create_travel_wrapper(workdir: str, input_cfg: dict):
    """
    Reads raw travel-to-work/school data from the input configuration, processes it, 
    and saves it as a pickle file in the specified working directory.

    Args:
        workdir (str): The directory where the output pickle file will be saved.
        input_cfg (dict): Configuration dictionary containing the path to the 
                          raw travel-to-work data under the key "commute".

    The output looks like:
       area_home  area_work  Work_at_home  Drive_a_private_car_truck_or_van  Drive_a_company_car_truck_or_van  ... 
         100100     100100           144                               117                                33  ...  
         100200     100200           210                                93                                24  ...
         100400     100400           102                                54                                 6  ...
         100500     100500           123                                30                                 9  ...
         100600     100600            63                                18                                 0  ...
        ......
    
    Each row is sth like:
        area_home                                                 100100.000000
        area_work                                                 100100.000000
        Work_at_home                                                 144.000000
        ....
        Ferry                                                          0.000000
        Other                                                          6.000000
        Work_at_home_percentage                                        0.436364
        ...
        Public_bus_percentage                                          0.000000
        Train_percentage                                               0.000000
        Bicycle_percentage                                             0.000000
    
    Returns:
        None
    """

    trave_to_work_data = _read_raw_travel_to_work(
        input_cfg["commute"]["travel_to_work"])

    travel_methods = [item for item in list(
        trave_to_work_data.columns) if item not in ["area_home", "area_work"]]

    trave_to_work_data["total_people"] = trave_to_work_data[
        travel_methods].sum(axis=1)

    for method in travel_methods:
        trave_to_work_data[f"{method}_percentage"] = trave_to_work_data[
            method] / trave_to_work_data['total_people']

    trave_to_work_data.drop(columns=["total_people"], inplace=True)

    with open(join(workdir, "commute.pickle"), "wb") as fid:
        pickle_dump({"travel_to_work": trave_to_work_data}, fid)


def create_population_wrapper(workdir: str, input_cfg: dict):
    """
    Creates a population dataset based on age, ethnicity, and gender distributions, and saves it to a file.

    Parameters:
        workdir (str): The working directory where the output file will be saved.
        input_cfg (str): The configuration file containing population parameters.

    The function performs the following steps:
    1. Creates a base population using the total population specified in the configuration.
    2. Generates age distribution data based on the base population and age distribution parameters.
    3. Generates ethnicity distribution data based on the age distribution and ethnicity ratio parameters.
    4. Generates gender distribution data based on the age distribution and gender ratio parameters.
    5. Saves the generated population data (age, gender, ethnicity) to a pickle file in the specified working directory.

    The generated population data includes:
    - Gender ~ Age distribution
    - Ethnicity ~ Age distribution

    The function ensures consistency between the age distribution and the generated gender and ethnicity distributions.

    Output is sth like below:

    Gender data:
                 area  gender          0          1          2          3          4          5  ...        
        0     100100  female   7.280005   7.280005   7.280005   7.280005   7.280005  10.010008  ...  
        1     100100    male   8.736007   8.736007   8.736007   8.736007   8.736007  12.012009  ... 
        2     100200  female  19.352681  19.352681  19.352681  19.352681  19.352681  21.287949  ...
        3     100200    male  20.687349  20.687349  20.687349  20.687349  20.687349  22.756084  ...
        4     100400  female   7.048714   7.048714   7.048714   7.048714   7.048714   9.759757  ...
        ......

    Ethnicity data:

                 area ethnicity         0         1         2         3         4          5  ...
        0      100100  European  5.700708  5.700708  5.700708  5.700708  5.700708   7.838473  ...
        1      100100     Maori  8.901105  8.901105  8.901105  8.901105  8.901105  12.239019  ...
        2      100100   Pacific  1.000124  1.000124  1.000124  1.000124  1.000124   1.375171  ...
        3      100100     Asian  0.300037  0.300037  0.300037  0.300037  0.300037   0.412551  ...
        4      100100     MELAA  0.100012  0.100012  0.100012  0.100012  0.100012   0.137517  ...
        ......

    Or if it is for the version 2.0, the population structure data looks like:
                sa2  ethnicity  age  gender  value
        0       100100          1    0       1    6.0
        1       100100          1    1       1    9.0
        2       100100          1    2       1    9.0
        3       100100          1    2       2    9.0
        4       100100          1    3       1    9.0
        ...        ...        ...  ...     ...    ...

    Depreviation data:
                area  deprivation
        0     100100         10.0
        1     100200          9.0
        2     100300          7.0
        3     100400          8.0
        4     100500          9.0
        ......
    
    Returns:
        None
    """
    # ----------------------------
    # get index of deprivation
    # ----------------------------
    nzdep_data = create_nzdep(input_cfg["population"]["nzdep"])


    # ----------------------------
    # get population data (v1.0)
    # ----------------------------
    if input_cfg["version"] == 1.0:
        base_population = create_base_population(
            input_cfg["population"]["total_population"])

        # ----------------------------
        # get age data
        # ----------------------------
        age_data = create_age_based_on_scaler(
            base_population, input_cfg["population"]["population_by_age"])

        # ----------------------------
        # get ethnicity data
        # ----------------------------
        ethnicity_ratio_data = create_ethnicity_ratio(input_cfg["population"]["population_by_age_by_ethnicity"])
        ethnicity_data_percentage = create_ethnicity_percentage_for_each_age(
            age_data, ethnicity_ratio_data
        )
        ethnicity_data = map_feature_percentage_data_with_age_population_data(
            age_data, ethnicity_data_percentage, check_consistency=True
        )

        # ----------------------------
        # get gender data
        # ----------------------------
        female_ratio_data = create_female_ratio(
            input_cfg["population"]["population_by_age_by_gender"])
        gender_data_percentage = create_gender_percentage_for_each_age(
            age_data, female_ratio_data
        )
        gender_data = map_feature_percentage_data_with_age_population_data(
            age_data, gender_data_percentage, check_consistency=True
        )


        with open(join(workdir, "population.pickle"), "wb") as fid:
            pickle_dump(
                {
                    "gender": gender_data, 
                    "ethnicity": ethnicity_data,
                    "deprivation": nzdep_data}, fid
            )
    
    # ----------------------------
    # get population data (v2.0)
    # ----------------------------
    if input_cfg["version"] == 2.0:
        population_structure = read_population_structure(
            input_cfg["population"]["population_structure"])
        with open(join(workdir, "population.pickle"), "wb") as fid:
            pickle_dump(
                {
                    "population_structure": population_structure, 
                    "deprivation": nzdep_data}, fid
            )

def create_geography_wrapper(workdir: str, input_cfg: dict, include_address: bool = True):
    """
    Creates a geography data wrapper and saves it as a pickle file.

    Parameters:
        workdir (str): The directory where the pickle file will be saved.
        input_cfg (dict): A dictionary containing configuration paths for the geography data.

    The function performs the following steps:
        1. Reads the raw geography hierarchy data from the specified path in input_cfg.
        2. Reads the raw geography location area data from the specified path in input_cfg.
        3. Reads the raw address data from the specified paths in input_cfg.
        4. Combines the read data into a dictionary.
        5. Saves the combined data as a pickle file in the specified workdir.

    The resulting pickle file contains:
        - 'hierarchy': Processed geography hierarchy data.
        - 'location': Processed geography location area data.
        - 'address': Processed address data.
    """
    output= {
                "hierarchy": _read_raw_geography_hierarchy(input_cfg["geography"]["geography_hierarchy"]),
                "location": _read_raw_geography_location_area(input_cfg["geography"]["geography_location"])
            }
    if include_address:
        output["address"] = _read_raw_address(input_cfg["geography"]["sa2_area_data"], input_cfg["geography"]["address_data"])

    with open(join(workdir, "geography.pickle"), "wb") as fid:
        pickle_dump(output, fid)
