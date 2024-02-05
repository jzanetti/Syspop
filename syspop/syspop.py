from datetime import datetime
from os import makedirs
from os.path import exists, join
from pickle import load as pickle_load

import ray
from pandas import DataFrame
from pandas import concat as pandas_concat
from pandas import cut as pandas_cut
from pandas import merge as pandas_merge
from pandas import read_parquet as pandas_read_parquet
from process.diary import create_diary, create_diary_remote
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
    create_school,
    create_shared_space,
    create_socialeconomics,
    create_work,
)

logger = setup_logging(workdir="")


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

    syn_pop_path = join(output_dir, "syspop_base.parquet")
    synpop_data = pandas_read_parquet(syn_pop_path)

    # ---------------------------
    # 1. plot distributions
    # ---------------------------
    if plot_distribution:
        synpop_data["adults_number_in_household"] = (
            synpop_data["household"].str.split("_").str[1]
        )
        synpop_data["children_number_in_household"] = (
            synpop_data["household"].str.split("_").str[2]
        )
        plot_pie_charts(
            vis_dir,
            synpop_data[
                [
                    "age",
                    "gender",
                    "ethnicity",
                    "adults_number_in_household",
                    "children_number_in_household",
                    "social_economics",
                    "travel_mode_work",
                ]
            ],
        )
    # ---------------------------
    # 2. plot locations
    # ---------------------------
    sys_address_path = join(output_dir, "syspop_location.parquet")
    if not exists(sys_address_path):
        return
    address_data = pandas_read_parquet(sys_address_path)

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
    syn_pop_path = join(output_dir, "syspop_base.parquet")
    synpop_data = pandas_read_parquet(syn_pop_path)

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


def diary(output_dir: str, n_cpu: int = 1, activities_cfg: dict or None = None):
    """Create diary data from synthetic population

    Args:
        output_dir (str): Output directory
        ncpu (int): Number of CPU to be used
    """

    start_t = datetime.utcnow()

    logger.info(f"Diary: reading synthetic population")
    syspop_data = pandas_read_parquet(join(output_dir, "syspop_base.parquet"))

    syspop_data_partitions = [
        df for _, df in syspop_data.groupby(pandas_cut(syspop_data.index, n_cpu))
    ]

    logger.info(f"Diary: initiating Ray [cpu: {n_cpu}]...")
    if n_cpu > 1:
        ray.init(num_cpus=n_cpu, include_dashboard=False)

    logger.info("Diary: start processing diary ...")
    outputs = []
    for i, proc_syspop_data in enumerate(syspop_data_partitions):
        if n_cpu == 1:
            outputs.append(
                create_diary(
                    proc_syspop_data,
                    n_cpu,
                    print_log=True,
                    activities_cfg=activities_cfg,
                )
            )
        else:
            outputs.append(
                create_diary_remote.remote(
                    proc_syspop_data,
                    n_cpu,
                    print_log=i == 0,
                    activities_cfg=activities_cfg,
                )
            )

    if n_cpu > 1:
        outputs = ray.get(outputs)
        ray.shutdown()

    outputs = pandas_concat(outputs, axis=0, ignore_index=True)
    end_t = datetime.utcnow()

    processing_mins = round((end_t - start_t).total_seconds() / 60.0, 2)

    outputs.to_parquet(join(output_dir, "diaries.parquet"))

    logger.info(f"Diary: created within {processing_mins} minutes ...")


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
    pharmacy_data: DataFrame = None,
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
        pharmacy_data (DataFrame, optional): pharmacy data. Defaults to None.
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
        create_shared_space(
            tmp_data_path,
            supermarket_data,
            "supermarket",
            2,
            geo_location,
            assign_address_flag,
        )

    if restaurant_data is not None:
        _check_dependancies("restaurant", deps_list=["geo_location"], address_deps=[])
        logger.info("Adding restaurant ...")
        create_shared_space(
            tmp_data_path,
            restaurant_data,
            "restaurant",
            4,
            geo_location,
            assign_address_flag,
        )

    if pharmacy_data is not None:
        _check_dependancies("pharmacy", deps_list=["geo_location"], address_deps=[])
        logger.info("Adding pharmacy ...")
        create_shared_space(
            tmp_data_path,
            pharmacy_data,
            "pharmacy",
            2,
            geo_location,
            assign_address_flag,
        )

    output_syn_pop_path = join(output_dir, "syspop_base.parquet")
    output_loc_path = join(output_dir, "syspop_location.parquet")

    with open(tmp_data_path, "rb") as fid:
        synpop_data = pickle_load(fid)

    synpop_data["synpop"]["id"] = synpop_data["synpop"].index
    synpop_data["synpop"].insert(0, "id", synpop_data["synpop"].pop("id"))

    synpop_data["synpop"].to_parquet(output_syn_pop_path, index=False)
    synpop_data["synadd"].to_parquet(output_loc_path, index=False)
