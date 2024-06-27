from copy import deepcopy
from os import makedirs
from os.path import exists, join
from pickle import dump as pickle_dump
from pickle import load as pickle_load

from numpy import nan as numpy_nan
from numpy import repeat as numpy_repeat
from numpy import round as numpy_round
from numpy.random import choice as numpy_choice
from pandas import DataFrame
from pandas import Series as pandas_series
from pandas import concat as pandas_concat

from syspop.process.base_pop import base_pop_wrapper
from syspop.process.hospital import hospital_wrapper
from syspop.process.household import household_wrapper
from syspop.process.school import school_and_kindergarten_wrapper
from syspop.process.shared_space import shared_space_wrapper
from syspop.process.social_economic import social_economic_wrapper
from syspop.process.work import work_and_commute_wrapper


def create_base_pop(
    tmp_data_path: str,
    pop_gender: DataFrame,
    pop_ethnicity: DataFrame,
    syn_areas: list,
    use_parallel: bool,
    ncpu: int,
):
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
        pop_gender, pop_ethnicity, syn_areas, use_parallel=use_parallel, n_cpu=ncpu
    )

    with open(tmp_data_path, "wb") as fid:
        pickle_dump({"synpop": synpop, "synadd": synadd}, fid)


def create_household(
    tmp_data_path: str,
    household_data: DataFrame,
    geo_address_data: DataFrame,
    use_parallel: bool,
    n_cpu: int,
):
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
        n_cpu=n_cpu,
    )

    with open(tmp_data_path, "wb") as fid:
        pickle_dump({"synpop": base_pop, "synadd": base_address}, fid)


def create_socialeconomics(tmp_data_path: str, socialeconomic_data: DataFrame):
    """Create social ecnomics for each area

    Args:
        tmp_data_path (str): where to save the population data
        socialeconomic_data (DataFrame): social economics data to be used
    """
    with open(tmp_data_path, "rb") as fid:
        base_pop = pickle_load(fid)

    base_pop_out = social_economic_wrapper(
        deepcopy(base_pop["synpop"]), socialeconomic_data
    )

    with open(tmp_data_path, "wb") as fid:
        pickle_dump({"synpop": base_pop_out, "synadd": base_pop["synadd"]}, fid)


def create_work(
    tmp_data_path: str,
    work_data: DataFrame,
    home_to_work_commute_data: DataFrame,
    geo_hierarchy_data: DataFrame,
    geo_address_data: DataFrame,
    use_parallel: bool,
    n_cpu: int,
):
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
        n_cpu=n_cpu,
    )

    with open(tmp_data_path, "wb") as fid:
        pickle_dump({"synpop": base_pop, "synadd": base_address}, fid)


def create_school_and_kindergarten(
    data_type: str,
    tmp_data_path: str,
    school_data: DataFrame,
    geo_hierarchy_data: DataFrame,
    assign_address_flag: bool,
    possile_area_levels: list = ["area", "super_area", "region"],
):
    """Create school information, if required, school address as well

    Args:
        tmp_data_path (str): where to save the population data
        school_data (DataFrame): school census such as the school location, capacity
        geo_hierarchy_data (DataFrame): geo hirarchy data, e.g., region -> super_area -> area
        assign_address_flag (bool): if write school address
    """
    with open(tmp_data_path, "rb") as fid:
        base_pop = pickle_load(fid)

    base_pop, base_address = school_and_kindergarten_wrapper(
        data_type,
        school_data,
        base_pop["synpop"],
        base_pop["synadd"],
        geo_hierarchy_data,
        assign_address_flag=assign_address_flag,
        possile_area_levels=possile_area_levels,
    )

    with open(tmp_data_path, "wb") as fid:
        pickle_dump({"synpop": base_pop, "synadd": base_address}, fid)


def create_hospital(
    tmp_data_path: str,
    hospital_data: DataFrame,
    geo_location: DataFrame,
    assign_address_flag: bool,
):
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
        assign_address_flag=assign_address_flag,
    )

    with open(tmp_data_path, "wb") as fid:
        pickle_dump({"synpop": base_pop, "synadd": base_address}, fid)


def create_shared_space(
    tmp_data_path: str,
    shared_space_data: DataFrame,
    shared_space_type: str,
    geo_location: DataFrame,
    assign_address_flag: bool,
    area_name_keys_and_selected_nums: list = {"area": 2},
):
    """Create synthetic restaurant/supermarket/pharmacy, if requred, address as well

    Args:
        tmp_data_path (str): where to save the population data
        shared_space_data (DataFrame): shared space data to be used (e.g., restaurant, supermarket, pharmacy etc.)
        shared_space_type (str): name of the shared space (e.g., restaurant etc.)
        selected_num (int): number of shared space to visit for each agent
        geo_location (DataFrame): geo location data, e.g., lat/lon for each area
        assign_address_flag (bool): if write hospital address
    """
    with open(tmp_data_path, "rb") as fid:
        base_pop = pickle_load(fid)

    base_pop_data = base_pop["synpop"]
    address_data = base_pop["synadd"]
    for area_name_key in area_name_keys_and_selected_nums:
        base_pop_data, address_data = shared_space_wrapper(
            shared_space_type,  # "restaurant",
            shared_space_data,  # restaurant_data,
            base_pop_data,
            address_data,
            geo_location,
            num_nearest=area_name_keys_and_selected_nums[area_name_key],  # 4,
            assign_address_flag=assign_address_flag,
            area_name_key=area_name_key,
        )

    with open(tmp_data_path, "wb") as fid:
        pickle_dump({"synpop": base_pop_data, "synadd": address_data}, fid)


def create_birthplace(tmp_data_path: str, birthplace_data: DataFrame):
    """Create birthplace for the data

    Args:
        tmp_data_path (str): _description_
        birthplace_data (DataFrame): _description_
    """
    with open(tmp_data_path, "rb") as fid:
        base_pop = pickle_load(fid)

    base_pop_data = base_pop["synpop"]

    base_pop_data["birthplace"] = numpy_nan

    all_areas = base_pop_data["area"].unique()

    data_list = []
    for proc_area in all_areas:
        proc_birthplace_data = birthplace_data[birthplace_data["area"] == proc_area]
        proc_base_pop_data = base_pop_data[base_pop_data["area"] == proc_area]

        if len(proc_birthplace_data) == 0:
            proc_base_pop_data["birthplace"] = 9999

        else:
            proc_birthplace_data["num"] = numpy_round(
                proc_birthplace_data["percentage"] * len(proc_base_pop_data)
            ).astype(int)

            proc_birthplace_data_processed = (
                proc_birthplace_data.loc[
                    numpy_repeat(
                        proc_birthplace_data.index.values, proc_birthplace_data["num"]
                    )
                ]
                .reset_index()[["birthplace"]]
                .sample(frac=1)
                .reset_index(drop=True)
            )
            proc_birthplace_data_processed_length = len(proc_birthplace_data_processed)
            proc_base_pop_data_length = len(proc_base_pop_data)

            if proc_birthplace_data_processed_length >= proc_base_pop_data_length:

                if proc_birthplace_data_processed_length > proc_base_pop_data_length:
                    proc_birthplace_data_processed = (
                        proc_birthplace_data_processed.sample(
                            n=proc_base_pop_data_length, replace=False
                        )
                    )

                proc_base_pop_data["birthplace"] = proc_birthplace_data_processed[
                    "birthplace"
                ].values
            else:
                random_indices = numpy_choice(
                    proc_base_pop_data.index,
                    size=len(proc_birthplace_data_processed),
                    replace=False,
                )
                proc_base_pop_data.loc[random_indices, "birthplace"] = (
                    proc_birthplace_data_processed["birthplace"].values
                )

                # assign the missing rows
                proc_base_pop_data.loc[
                    proc_base_pop_data["birthplace"].isnull(), "birthplace"
                ] = numpy_choice(
                    proc_birthplace_data_processed["birthplace"],
                    size=proc_base_pop_data["birthplace"].isnull().sum(),
                )

        data_list.append(proc_base_pop_data)

    base_pop_data.update(pandas_concat(data_list, axis=0))

    base_pop_data["birthplace"] = base_pop_data["birthplace"].astype(int)

    with open(tmp_data_path, "wb") as fid:
        pickle_dump({"synpop": base_pop_data, "synadd": base_pop["synadd"]}, fid)


def create_vaccine(
    tmp_data_path: str,
    vaccine_data: DataFrame,
    data_year: int or None,
    data_percentile: str or None,
    fill_missing_adults_data_flag: bool = False,
    full_imms_age: int or None = 60,
) -> DataFrame:
    """Create vaccination data

    Args:
        tmp_data_path (str): temp directory
        vaccine_data (DataFrame): Vaccination data such as MMR
        full_imms_age (int, optional): Full immunisation age. Defaults to 60.

    Returns:
        DataFrame: _description_
    """
    if data_percentile is not None:
        vaccine_data = vaccine_data[vaccine_data["percentile"] == data_percentile]
    else:
        vaccine_data = vaccine_data[vaccine_data["percentile"] == "median"]

    if data_year is not None:
        vaccine_data = vaccine_data[vaccine_data["year"] == data_year]
    else:
        vaccine_data = vaccine_data[
            vaccine_data["year"] == max(vaccine_data["year"].unique())
        ]

    with open(tmp_data_path, "rb") as fid:
        base_pop = pickle_load(fid)

    base_pop_data = base_pop["synpop"]
    base_pop_data.reset_index(inplace=True)
    base_pop_data = base_pop_data.rename(columns={"index": "id"})

    base_pop_data["mmr"] = None

    if fill_missing_adults_data_flag:
        base_pop_data["mmr_age"] = base_pop_data["age"].apply(
            lambda x: 17 if 17 <= x <= full_imms_age else x
        )
    else:
        base_pop_data["mmr_age"] = base_pop_data["age"]

    base_pop_data["mmr_ethnicity"] = base_pop_data["ethnicity"].replace(
        ["European", "MELAA"], "Others"
    )
    vaccine_data = vaccine_data[vaccine_data["sa2"].isin(base_pop_data["area"])]
    vaccine_data[["age_min", "age_max"]] = vaccine_data["age"].str.split(
        "-", expand=True
    )
    vaccine_data["age_max"] = vaccine_data["age_max"].fillna(vaccine_data["age_min"])

    # -----------------------------
    # Assign imms to people for different ethnicity/age groups
    # -----------------------------
    data_list = []
    for i in range(len(vaccine_data)):
        row = vaccine_data.iloc[[i]]

        filtered_base_pop = base_pop_data[
            (base_pop_data["area"] == row.sa2.values[0])
            & (base_pop_data["mmr_ethnicity"] == row.ethnicity.values[0])
            & (base_pop_data["mmr_age"] >= int(row.age_min.values[0]))
            & (base_pop_data["mmr_age"] <= int(row.age_max.values[0]))
        ]

        fully_imms_base_pop = filtered_base_pop.sample(frac=row.fully_imms.values[0])
        filtered_base_pop.loc[fully_imms_base_pop.index, "mmr"] = "fully_imms"

        partial_imms_base_pop = filtered_base_pop.drop(
            fully_imms_base_pop.index
        ).sample(frac=row.partial_imms.values[0])
        filtered_base_pop.loc[partial_imms_base_pop.index, "mmr"] = "partial_imms"

        no_imms_base_pop = filtered_base_pop[filtered_base_pop["mmr"].isna()]
        filtered_base_pop.loc[no_imms_base_pop.index, "mmr"] = "no_imms"

        data_list.append(filtered_base_pop)

    # -----------------------------
    # Set imms status for age of people > 60 (if needed) and people == 0
    # -----------------------------

    base_pop_data.update(pandas_concat(data_list, axis=0))

    if fill_missing_adults_data_flag:
        base_pop_data.loc[base_pop_data.age > full_imms_age, "mmr"] = "nature_imms"
    base_pop_data.loc[base_pop_data.age == 0, "mmr"] = "no_imms"

    # -----------------------------
    # Set imms status for rest people
    # -----------------------------
    remained_base_pop = base_pop_data[base_pop_data["mmr"].isna()]

    data_list = []
    for i in range(len(remained_base_pop)):
        proc_pop = remained_base_pop.iloc[[i]]
        proc_pop_ethnicity = proc_pop.mmr_ethnicity.values[0]
        ave_fully_imms = vaccine_data[vaccine_data["ethnicity"] == proc_pop_ethnicity][
            "fully_imms"
        ].mean()
        ave_partial_imms = vaccine_data[
            vaccine_data["ethnicity"] == proc_pop_ethnicity
        ]["partial_imms"].mean()

        proc_pop["mmr"] = numpy_choice(
            ["fully_imms", "partial_imms", "no_imms"],
            p=[
                ave_fully_imms,
                ave_partial_imms,
                1.0 - (ave_fully_imms + ave_partial_imms),
            ],
        )

        data_list.append(proc_pop)

    base_pop_data.update(pandas_concat(data_list, axis=0))

    with open(tmp_data_path, "wb") as fid:
        pickle_dump({"synpop": base_pop_data, "synadd": base_pop["synadd"]}, fid)
