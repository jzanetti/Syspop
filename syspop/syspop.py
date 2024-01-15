from logging import getLogger
from os import makedirs
from os.path import exists, join
from pickle import load as pickle_load

from pandas import DataFrame
from pandas import concat as pandas_concat
from pandas import merge as pandas_merge
from pandas import read_csv as pandas_read_csv
from process.utils import setup_logging
from process.validate import (
    validate_base_pop_and_age,
    validate_commute_area,
    validate_commute_mode,
    validate_household,
    validate_work,
)
from process.vis import plot_map_html, plot_pie_charts, plot_travel_html
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


def vis(
    output_dir: str = "",
    plot_distribution: bool = True,
    plot_travel: bool = True,
    plot_location: bool = True,
):
    """Syntheric population visualization

    Args:
        output_dir (str, optional): _description_. Defaults to "".
        plot_distribution (bool, optional): _description_. Defaults to True.
        plot_travel (bool, optional): _description_. Defaults to True.
        plot_location (bool, optional): _description_. Defaults to True.
        travel_sample_size (intorNone, optional): _description_. Defaults to 250.
    """
    vis_dir = join(output_dir, "vis")
    if not exists(vis_dir):
        makedirs(vis_dir)

    syn_pop_path = join(output_dir, "syspop_base.csv")
    synpop_data = pandas_read_csv(syn_pop_path)

    # ---------------------------
    # 1. plot distributions
    # ---------------------------
    if plot_distribution:
        synpop_data["children_number_in_household"] = (
            synpop_data["household"].str.split("_").str[1]
        )
        plot_pie_charts(
            vis_dir,
            synpop_data[
                [
                    "age",
                    "gender",
                    "ethnicity",
                    "children_number_in_household",
                    "social_economics",
                    "travel_mode_work",
                ]
            ],
        )
    # ---------------------------
    # 2. plot locations
    # ---------------------------
    sys_address_path = join(output_dir, "syspop_location.csv")
    if not exists(sys_address_path):
        return
    address_data = pandas_read_csv(sys_address_path)

    # -----------------
    # 2.1 plot travel: work - home
    # -----------------
    if plot_travel:
        most_common_area = synpop_data["area"].value_counts().idxmax()
        household_company_data = synpop_data[synpop_data["area"] == most_common_area][
            ["household", "company"]
        ]
        household_company_data = household_company_data[
            household_company_data["company"].notna()
        ]
        start_df = pandas_merge(
            household_company_data,
            address_data,
            left_on="household",
            right_on="name",
            how="left",
        ).rename(columns={"latitude": "start_lat", "longitude": "start_lon"})
        end_df = pandas_merge(
            household_company_data,
            address_data,
            left_on="company",
            right_on="name",
            how="left",
        ).rename(columns={"latitude": "end_lat", "longitude": "end_lon"})
        df = pandas_concat([start_df, end_df], axis=1)[
            ["start_lat", "start_lon", "end_lat", "end_lon"]
        ]
        df = df.dropna()
        # if travel_sample_size is not None:
        #    df = df.sample(travel_sample_size)
        plot_travel_html(vis_dir, df, "home_to_work")

    # -----------------
    # 2.2 plot location heat map
    # -----------------
    if plot_location:
        for data_name in list(address_data["type"].unique()):
            if data_name == "school":
                proc_data = address_data[address_data["type"] == data_name]
                proc_data["school_types"] = proc_data["name"].apply(
                    lambda x: x.split("_", 1)[1].rsplit("_", 1)[0]
                )
                for proc_school_type in list(proc_data["school_types"].unique()):
                    proc_data2 = proc_data[
                        proc_data["school_types"] == proc_school_type
                    ][["latitude", "longitude"]]
                    plot_map_html(vis_dir, proc_data2, f"school_{proc_school_type}")
            else:
                proc_data = address_data[address_data["type"] == data_name][
                    ["latitude", "longitude"]
                ]
                plot_map_html(vis_dir, proc_data, data_name)


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

    synpop_base_data = synpop_data["synpop"]
    if "Unnamed: 0" in list(synpop_base_data.columns):
        synpop_base_data = synpop_base_data.drop("Unnamed: 0", axis=1)

    synpop_base_data.to_csv(output_syn_pop_path)
    synpop_data["synadd"].to_csv(output_loc_path)
