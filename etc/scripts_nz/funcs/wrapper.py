from logging import getLogger
from os.path import join

from funcs.household.household import (
    create_household_and_dwelling_number
)
from funcs.others.health import add_mmr
from funcs.others.immigration import add_birthplace
from funcs.population.population import read_population_structure
from funcs.venue.venue import (
    create_hospital,
    create_kindergarten,
    create_school,
    create_osm_space
)

from pandas import read_parquet, read_csv

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

    The generated household data looks like:
                area  adults  children value
        0      100100       0         1    3
        1      100100       0         2    4
        2      100100       1         0  142
        3      100100       1         1   14
        4      100100       1         2   15
        ...       ...     ...       ...  ...

    Returns:
        None
    """

    hhd_data = create_household_and_dwelling_number(
        input_cfg["household"]["household_composition"])
    # hhd_data['percentage'] = hhd_data.groupby("area")["num"].transform(
    #    lambda x: x / x.sum())
    hhd_data.to_parquet(join(
        workdir, "household_composition.parquet"))


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
    geography_location = read_parquet(join(workdir, "geography_location.parquet"))

    for space_name in space_names:
        print(f"Shared space: {space_name} ...")
        proc_data = create_osm_space(input_cfg["venue"][space_name], geography_location)
        proc_data.to_parquet(join(workdir, f"shared_space_{space_name}.parquet"))


def create_others_wrapper(workdir: str, input_cfg: dict):
    """Createing others

    Args:
        workdir (str): Working directory
    """
    mmr_data = add_mmr(input_cfg["others"]["mmr_vaccine"])
    birthplace_data = add_birthplace(input_cfg["others"]["birthplace"])
    mmr_data.to_parquet(join(workdir, "mmr_vaccine.parquet"))
    birthplace_data.to_parquet(join(workdir, "birthplace.parquet"))


def create_hospital_wrapper(workdir: str, input_cfg: dict):
    """Create hospital wrapper (e.g., where is the hospital etc.)

    The output looks like:
            area   latitude   longitude  beds
    2     100800 -35.119186  173.260926    32
    4     350400 -45.858787  170.473064    90
    5     229800 -40.337130  175.616683    11
    6     233300 -40.211906  176.098154    11
    7     125500 -36.779884  174.756511    35
    ...      ...        ...         ...   ...

    Args:
        workdir (str): Working directory
    """
    geography_location = read_parquet(join(workdir, "geography_location.parquet"))
    hopital_data = create_hospital(input_cfg["venue"]["hospital"], geography_location)    
    hopital_data.to_parquet(join(workdir, "hospital.parquet"))


def create_kindergarten_wrapper(workdir: str, input_cfg: dict):
    """Create kindergarten wrapper (e.g., where is the kindergarten etc.)

    The output looks like:
            area  max_students   latitude   longitude        sector  age_min  age_max
    0     100800            30 -35.118228  173.258565  kindergarten        0        5
    1     101100            30 -34.994478  173.464730  kindergarten        0        5
    2     100700            30 -35.116080  173.270685  kindergarten        0        5
    3     103500            30 -35.405553  173.796409  kindergarten        0        5
    4     103900            30 -35.278121  174.081808  kindergarten        0        5
    .....
    Args:
        workdir (str): Working directory
    """
    kindergarten_data = create_kindergarten(input_cfg["venue"]["kindergarten"])    
    kindergarten_data.to_parquet(join(workdir, "kindergarten.parquet"))


def create_school_wrapper(workdir: str, input_cfg: dict):
    """Create school wrapper (e.g., where is the school etc.)

    The output is sth like:
                area  max_students             sector   latitude   longitude  age_min  age_max
        0     133400             0          secondary -36.851138  174.760643       14       19
        1     167100          1087  primary_secondary -36.841742  175.696738        5       19
        3     358500            28            primary -46.207408  168.541883        5       13
        8     101100           296  primary_secondary -34.994245  173.463766        5       19
        9     106600          1728          secondary -35.713358  174.318881       14       19
        .....

    Args:
        workdir (str): Working directory
    """

    geography_location = read_parquet(join(workdir, "geography_location.parquet"))

    school_data = create_school(input_cfg["venue"]["school"], geography_location)
    
    school_data.to_parquet(join(workdir, "school.parquet"))


def create_work_wrapper(workdir: str, input_cfg: dict):
    """Create work wrapper (e.g., employees etc.)

    The output (employer/employee)looks like:
                  area business_code         employer         employee
        0       100100             A               93              190                    
        1       100200             A              138              190                   
        2       100300             A                6               25
        3       100400             A               57               50
        4       100500             A               57               95
        ......
    For each area it is sth like:
                  area business_code         employer         employee
        0       100100             A               93              190                    
        20082   100100             C                0                6                    
        56927   100100             E               12                6                   
        ....
        132366  100100             I                6                9                    
        159241  100100             K                6                0        

    In addition, the income data looks like:
            business_code  sex     age  ethnicity  value
        137488             A    1   15-19          1    166
        137498             A    1   60-64          1   1215
        137508             A    1  65-999          1   1247
        137513             A    1  65-999          2    945
        137523             A    1   20-24          1   1036
        ...              ...  ...     ...        ...    ...            

    Args:
        workdir (str): Working directory
    """
    data = _read_raw_employer_employee_data(input_cfg["work"]["employer_employee_num"])
    
    data_income = read_csv(input_cfg["work"]["income"]).reset_index(drop=True)

    employee_data = data[
        [
            "area",
            "business_code",
            "employee"
        ]
    ].reset_index(drop=True)

    employer_data = data[
        ["area", "business_code", "employer"]
    ].reset_index(drop=True)
    
    employee_data.to_parquet(join(workdir, "work_employee.parquet"))
    employer_data.to_parquet(join(workdir, "work_employer.parquet"))
    data_income.to_parquet(join(workdir, "work_income.parquet"))


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
    
    Returns:
        None
    """

    trave_to_work_data = _read_raw_travel_to_work(
        input_cfg["commute"]["travel_to_work"], data_type="work")

    trave_to_school_data = _read_raw_travel_to_work(
        input_cfg["commute"]["travel_to_school"], data_type="school")
    
    trave_to_work_data.to_parquet(join(workdir, "commute_travel_to_work.parquet"))
    trave_to_school_data.to_parquet(join(workdir, "commute_travel_to_school.parquet"))


def create_population_wrapper(workdir: str, input_cfg: dict):
    """
    Creates a population dataset based on age, ethnicity, and gender distributions, and saves it to a file.

    Parameters:
        workdir (str): The working directory where the output file will be saved.
        input_cfg (str): The configuration file containing population parameters.

    Output is sth like below:


    Or if it is for the version 2.0, the population structure data looks like:
                  area  ethnicity  age  gender  value
        0       100100          1    0       1    6.0
        1       100100          1    1       1    9.0
        2       100100          1    2       1    9.0
        3       100100          1    2       2    9.0
        4       100100          1    3       1    9.0
        ...        ...        ...  ...     ...    ...
    """
    population_structure = read_population_structure(
        input_cfg["population"]["population_structure"])
    population_structure.to_parquet(
        join(workdir, "population_structure.parquet"))


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
    
    The output looks like:

    - hierarchy:
                    region  super_area    area
        0        Northland       50010  100100
        3        Northland       50010  100200
        6        Northland       50010  100600
        8        Northland       50030  100400
        10       Northland       50030  101000
        ...            ...         ...     ...
    
    - location:
                area   latitude   longitude
        0     100100 -34.505453  172.775550
        1     100200 -34.916277  173.137443
        2     100300 -35.218501  174.158249
        3     100400 -34.995278  173.378738
        4     100500 -35.123147  173.218604
        ...      ...        ...         ...
    
    - address:
                   area   latitude   longitude
        0        136000 -36.865148  174.755962
        1        136000 -36.865394  174.756203
        2        354700 -45.896419  170.506439
        3        331400 -43.569101  172.682428
        4        331400 -43.568808  172.682646
        ...         ...        ...         ...
    """
    output= {
        "hierarchy": _read_raw_geography_hierarchy(
            input_cfg["geography"]["geography_hierarchy"]),
        "location": _read_raw_geography_location_area(
            input_cfg["geography"]["geography_location"])
        }
    if include_address:
        output["address"] = _read_raw_address(
            input_cfg["geography"]["sa2_area_data"], 
            input_cfg["geography"]["address_data"])
    
    output["hierarchy"].to_parquet(join(workdir, "geography_hierarchy.parquet"))
    output["location"].to_parquet(join(workdir, "geography_location.parquet"))
    output["address"].to_parquet(join(workdir, "geography_address.parquet"))

