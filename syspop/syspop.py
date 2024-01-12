from logging import getLogger
from os import makedirs
from os.path import exists, join
from pickle import load as pickle_load

from pandas import DataFrame
from pandas import read_csv as pandas_read_csv
from process.utils import setup_logging
from process.validate import (
    validate_base_pop_and_age,
    validate_commute_area,
    validate_commute_mode,
    validate_household,
    validate_work,
)
from wrapper_pop import (
    create_base_pop,
    create_hospital,
    create_household,
    create_restauraunt,
    create_school,
    create_socialeconomics,
    create_supermarket,
    create_work,
)

logger = getLogger()


def validate(
    output_dir: str = "",
    pop_gender: DataFrame = None,  # census
    pop_ethnicity: DataFrame = None,  # census
    household: DataFrame or None = None,  # census
    work_data: DataFrame or None = None,  # census
    home_to_work: DataFrame or None = None,  # census
):
    """Doding the validation of synthetic population

    Args:
        output_dir (str, optional): Output drirectory. Defaults to "".
        pop_gender (DataFrame, optional): synthetic population. Defaults to None.
    """
    syn_pop_path = join(output_dir, "syspop_base.csv")
    synpop_data = pandas_read_csv(syn_pop_path)

    val_dir = join(output_dir, "val")
    if not exists(val_dir):
        makedirs(val_dir)

    logger.info("Valdating commute (area) ...")
    validate_commute_area(val_dir, synpop_data, home_to_work)

    logger.info("Valdating commute (travel_mode) ...")
    validate_commute_mode(val_dir, synpop_data, home_to_work)

    logger.info("Valdating work ...")
    validate_work(val_dir, synpop_data, work_data)

    logger.info("Validating household ...")
    validate_household(val_dir, synpop_data, household)

    logger.info("Validating base population (gender) ...")
    validate_base_pop_and_age(
        val_dir, synpop_data, pop_gender, "gender", ["male", "female"]
    )

    logger.info("Validating base population (ethnicity) ...")
    validate_base_pop_and_age(
        val_dir,
        synpop_data,
        pop_ethnicity,
        "ethnicity",
        ["European", "Maori", "Pacific", "Asian", "MELAA"],
    )


def create(
    syn_areas: list or None = None,
    output_dir: str = "",
    pop_gender: DataFrame = None,
    pop_ethnicity: DataFrame = None,
    geo_hierarchy: DataFrame = None,
    geo_location: DataFrame = None,
    geo_address: DataFrame = None,
    household: DataFrame = None,
    socialeconomic: DataFrame = None,
    work_data: DataFrame = None,
    home_to_work: DataFrame = None,
    school_data: DataFrame = None,
    hospital_data: DataFrame = None,
    supermarket_data: DataFrame = None,
    restaurant_data: DataFrame = None,
    assign_address_flag: bool = False,
    rewrite_base_pop: bool = False,
    use_parallel: bool = False,
    ncpu: int = 8,
):
    """Create synthetic population

    Args:
        syn_areas (listorNone, optional): Areas to be processed. Defaults to None.
        output_dir (str, optional): Where the data will be written. Defaults to "".
        pop_gender (DataFrame, optional): population gender data. Defaults to None.
        pop_ethnicity (DataFrame, optional): population ethnicity data. Defaults to None.
        geo_hierarchy (DataFrame, optional): geography hierarchy data. Defaults to None.
        geo_location (DataFrame, optional): geography location data. Defaults to None.
        geo_address (DataFrame, optional): geography address data. Defaults to None.
        household (DataFrame, optional): household data. Defaults to None.
        socialeconomic (DataFrame, optional): socialeconomic data. Defaults to None.
        work_data (DataFrame, optional): employer/eomplyee data. Defaults to None.
        home_to_work (DataFrame, optional): work commute data. Defaults to None.
        school_data (DataFrame, optional): school data. Defaults to None.
        hospital_data (DataFrame, optional): hospital data. Defaults to None.
        supermarket_data (DataFrame, optional): supermarket data. Defaults to None.
        restaurant_data (DataFrame, optional): restaurant data. Defaults to None.
        assign_address_flag (bool, optional): if assign lat/lon to different venues. Defaults to False.
        rewrite_base_pop (bool, optional): if re-write base population. Defaults to False.
        use_parallel (bool, optional): use parallel processing. Defaults to False.
        ncpu (int, optional): number of CPUs. Defaults to 8.

    Raises:
        Exception: missing depedancies
    """

    args_dict = locals()

    def _check_dependancies(
        key_item: str, deps_list: list = [], address_deps: list = []
    ):
        """Check if all dependancies are met

        Args:
            dependcancies_list (list, optional): a list of items to be checked. Defaults to [].
        """
        for item_to_check in deps_list:
            if args_dict[item_to_check] is None:
                raise Exception(
                    f"{key_item} is presented/required, but its dependancy {item_to_check} is not here ..."
                )

        if assign_address_flag:
            for item_to_check in address_deps:
                if args_dict[item_to_check] is None:
                    raise Exception(
                        f"address data is required for {key_item}, but its address dep {item_to_check} is not here ..."
                    )

    tmp_dir = join(output_dir, "tmp")
    if not exists(tmp_dir):
        makedirs(tmp_dir)

    tmp_data_path = join(tmp_dir, "synpop.pickle")

    logger = setup_logging(workdir=output_dir)

    if (not exists(tmp_data_path)) or rewrite_base_pop:
        logger.info("Creating base population ...")
        _check_dependancies(
            "base_pop", deps_list=["pop_gender", "pop_ethnicity", "syn_areas"]
        )
        create_base_pop(
            tmp_data_path, pop_gender, pop_ethnicity, syn_areas, use_parallel, ncpu
        )

    if household is not None:
        logger.info("Adding household ...")
        _check_dependancies("household", address_deps=["geo_address"])
        create_household(tmp_data_path, household, geo_address, use_parallel, ncpu)

    if socialeconomic is not None:
        create_socialeconomics(tmp_data_path, socialeconomic)

    if work_data is not None:
        _check_dependancies(
            "work",
            deps_list=["home_to_work", "geo_hierarchy"],
            address_deps=["geo_address"],
        )
        logger.info("Adding work ...")
        create_work(
            tmp_data_path,
            work_data,
            home_to_work,
            geo_hierarchy,
            geo_address,
            use_parallel,
            ncpu,
        )

    if school_data is not None:
        _check_dependancies("school", deps_list=["geo_hierarchy"], address_deps=[])
        logger.info("Adding school ...")
        create_school(tmp_data_path, school_data, geo_hierarchy, assign_address_flag)

    if hospital_data is not None:
        _check_dependancies("hospital", deps_list=["geo_hierarchy"], address_deps=[])
        logger.info("Adding hospital ...")
        create_hospital(tmp_data_path, hospital_data, geo_location, assign_address_flag)

    if supermarket_data is not None:
        _check_dependancies("supermarket", deps_list=["geo_location"], address_deps=[])
        logger.info("Adding supermarket ...")
        create_supermarket(
            tmp_data_path, supermarket_data, geo_location, assign_address_flag
        )

    if restaurant_data is not None:
        _check_dependancies("restauraunt", deps_list=["geo_location"], address_deps=[])
        logger.info("Adding restauraunt ...")
        create_restauraunt(
            tmp_data_path, restaurant_data, geo_location, assign_address_flag
        )

    output_syn_pop_path = join(output_dir, "syspop_base.csv")
    output_loc_path = join(output_dir, "syspop_location.csv")

    with open(tmp_data_path, "rb") as fid:
        synpop_data = pickle_load(fid)

    synpop_data["synpop"].to_csv(output_syn_pop_path)
    synpop_data["synadd"].to_csv(output_loc_path)
