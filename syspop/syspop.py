from datetime import datetime
from functools import reduce as functools_reduce
from os import makedirs
from os.path import exists, join
from pickle import load as pickle_load

import ray
from numpy import NaN as numpy_nan
from numpy import unique as numpy_unique
from numpy import zeros as numpy_zeros
from pandas import DataFrame
from pandas import concat as pandas_concat
from pandas import cut as pandas_cut
from pandas import merge as pandas_merge
from pandas import read_parquet as pandas_read_parquet
from process.diary import (
    create_diary,
    create_diary_remote,
    map_loc_to_diary,
    quality_check_diary,
)
from process.utils import merge_syspop_data, setup_logging
from process.validate import (
    validate_base_pop_and_age,
    validate_commute_area,
    validate_commute_mode,
    validate_household,
    validate_mmr,
    validate_work,
)
from process.vis import (
    plot_average_occurence_charts,
    plot_location_occurence_charts_by_hour,
    plot_location_timeseries_charts,
    plot_map_html,
    plot_pie_charts,
    plot_travel_html,
)
from wrapper_pop import (
    create_base_pop,
    create_birthplace,
    create_hospital,
    create_household,
    create_school_and_kindergarten,
    create_shared_space,
    create_socialeconomics,
    create_vaccine,
    create_work,
)

logger = setup_logging(workdir="")


def vis(
    output_dir: str = "",
    plot_distribution: bool = True,
    plot_travel: bool = True,
    plot_location: bool = True,
    plot_diary: bool = True,
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

    synpop_data = merge_syspop_data(output_dir, ["base", "household", "travel"])

    # ---------------------------
    # 1. plot distributions
    # ---------------------------
    if plot_distribution:
        logger.info("Creating plots: plot_distribution")
        synpop_data["adults_number_in_household"] = (
            synpop_data["household"].str.split("_").str[1]
        )
        synpop_data["children_number_in_household"] = (
            synpop_data["household"].str.split("_").str[2]
        )

        all_params = [
            "age",
            "gender",
            "ethnicity",
            "adults_number_in_household",
            "children_number_in_household",
            "social_economics",
            "travel_mode_work",
        ]

        plot_params = []

        for proc_params in all_params:
            if proc_params in list(synpop_data.columns):
                plot_params.append(proc_params)

        plot_pie_charts(
            vis_dir,
            synpop_data[plot_params],
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
        logger.info("Creating plots: plot_travel")
        synpop_data = merge_syspop_data(
            output_dir, ["base", "household", "travel", "work_and_school"]
        )
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
        logger.info("Creating plots: plot_location")
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

    # ---------------------------
    # 3. plot diary
    # ---------------------------
    if plot_diary:
        logger.info("Creating plots: plot_diary (general)")
        # ---------------------------
        # 3.1 plot diary distribution in general
        # ---------------------------
        sys_diary_path = join(output_dir, "syspop_diaries.parquet")
        if not exists(sys_diary_path):
            return
        diary_data = pandas_read_parquet(sys_diary_path)
        # diary_data = diary_data.drop(columns=["id"])
        # Create a dictionary to store the counts

        location_counts = {}
        for proc_place in numpy_unique(diary_data["type"]):
            location_counts[proc_place] = {}
            for proc_hr in range(24):
                location_counts[proc_place][proc_hr] = 0

        # Iterate through each location
        for proc_hr in range(24):
            value_counts = (
                diary_data[diary_data["hour"] == str(proc_hr)]["type"]
                .value_counts()
                .to_dict()
            )
            for proc_place in value_counts:
                location_counts[proc_place][proc_hr] += value_counts[proc_place]

        plot_location_timeseries_charts(vis_dir, location_counts)

        # ---------------------------
        # 3.2. plot diary/place distribution
        # ---------------------------
        # sys_all_data_path = join(output_dir, "syspop_diaries.parquet")
        # if not exists(sys_diary_path):
        #    return
        # syspop_and_diary = pandas_read_parquet(sys_all_data_path)
        logger.info("Creating plots: plot_diary (diary/place distribution)")
        vis_dir_syspop_and_diary = join(vis_dir, "syspop_diaries")
        if not exists(vis_dir_syspop_and_diary):
            makedirs(vis_dir_syspop_and_diary)

        average_counts = {}
        for proc_key in location_counts.keys():
            average_counts[proc_key] = list(numpy_zeros(24))

        for proc_hr in range(24):

            all_types = list(
                numpy_unique(
                    diary_data[diary_data["hour"] == str(proc_hr)]["type"].values
                )
            )
            all_locs = diary_data[diary_data["hour"] == str(proc_hr)]["location"]

            for proc_type in all_types:
                logger.info(
                    f"Creating plots: plot_diary (diary/place distribution): Hour: {proc_hr} - {proc_type}"
                )
                proc_mask_type = (
                    diary_data[diary_data["hour"] == str(proc_hr)]["type"] == proc_type
                )
                proc_loc = all_locs[proc_mask_type]

                value_counts = proc_loc.value_counts().to_dict()
                keys_counts = list(value_counts.keys())

                plot_location_occurence_charts_by_hour(
                    vis_dir_syspop_and_diary, value_counts, proc_hr, proc_type
                )

                try:
                    average_counts[proc_type][proc_hr] = round(
                        sum(value_counts.values()) / len(keys_counts),
                        3,
                    )
                except ZeroDivisionError:
                    average_counts[proc_type][proc_hr] = numpy_nan

        for proc_data_type in average_counts:
            logger.info(
                f"Creating plots: plot average occurence (diary/place distribution): Hour: {proc_data_type}"
            )
            plot_average_occurence_charts(
                vis_dir_syspop_and_diary,
                average_counts[proc_data_type],
                proc_data_type,
            )


def validate(
    output_dir: str = "",
    pop_gender: DataFrame = None,  # census
    pop_ethnicity: DataFrame = None,  # census
    household: DataFrame or None = None,  # census
    work_data: DataFrame or None = None,  # census
    home_to_work: DataFrame or None = None,  # census
    mmr_data: DataFrame or None = None,  # imms data
    data_year: int or None = None,  # data year if it is used
    data_percentile: str or None = None,  # lower, upper, and median
):
    """Doding the validation of synthetic population

    Args:
        output_dir (str, optional): Output drirectory. Defaults to "".
        pop_gender (DataFrame, optional): synthetic population. Defaults to None.
    """
    # syn_pop_path = join(output_dir, "syspop_base.parquet")
    # synpop_data = pandas_read_parquet(syn_pop_path)

    val_dir = join(output_dir, "val")
    if not exists(val_dir):
        makedirs(val_dir)

    logger.info("Validating MMR ...")
    validate_mmr(
        val_dir,
        merge_syspop_data(output_dir, ["base", "healthcare"]),
        mmr_data,
        data_year,
        data_percentile,
    )

    logger.info("Valdating commute (area) ...")
    validate_commute_area(
        val_dir,
        merge_syspop_data(output_dir, ["base", "travel", "work_and_school"]),
        home_to_work,
    )

    logger.info("Valdating commute (travel_mode) ...")
    validate_commute_mode(
        val_dir, merge_syspop_data(output_dir, ["base", "travel"]), home_to_work
    )

    logger.info("Valdating work ...")
    validate_work(
        val_dir,
        merge_syspop_data(output_dir, ["base", "work_and_school"]),
        work_data,
        exlcuded_business_code=["L", "Q", "P", "O", "C"],
    )

    logger.info("Validating household ...")
    validate_household(
        val_dir,
        merge_syspop_data(output_dir, ["base", "household", "others"]),
        household,
    )

    logger.info("Validating base population (gender) ...")
    validate_base_pop_and_age(
        val_dir,
        merge_syspop_data(output_dir, ["base"]),
        pop_gender,
        "gender",
        ["male", "female"],
    )

    logger.info("Validating base population (ethnicity) ...")
    validate_base_pop_and_age(
        val_dir,
        merge_syspop_data(output_dir, ["base"]),
        pop_ethnicity,
        "ethnicity",
        ["European", "Maori", "Pacific", "Asian", "MELAA"],
    )


def diary(
    output_dir: str,
    n_cpu: int = 1,
    llm_diary_data: dict or None = None,
    activities_cfg: dict or None = None,
):
    """Create diary data from synthetic population

    Args:
        output_dir (str): Output directory
        ncpu (int): Number of CPU to be used
    """

    start_t = datetime.now()

    logger.info(f"Diary: reading synthetic population")
    syspop_data = merge_syspop_data(output_dir, ["base", "work_and_school"])

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
                    llm_diary_data=llm_diary_data,
                )
            )
        else:
            outputs.append(
                create_diary_remote.remote(
                    proc_syspop_data,
                    n_cpu,
                    print_log=i == 0,
                    activities_cfg=activities_cfg,
                    llm_diary_data=llm_diary_data,
                )
            )

    if n_cpu > 1:
        outputs = ray.get(outputs)
        ray.shutdown()

    outputs = pandas_concat(outputs, axis=0, ignore_index=True)

    logger.info(f"Diary: quality check ...")

    outputs = quality_check_diary(syspop_data, outputs)

    end_t = datetime.now()

    processing_mins = round((end_t - start_t).total_seconds() / 60.0, 2)

    outputs.to_parquet(join(output_dir, "tmp", "syspop_diaries_type.parquet"))

    logger.info(f"Diary: start mapping location to diary ...")
    map_loc_to_diary(output_dir)

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
    department_store_data: DataFrame = None,
    wholesale_data: DataFrame = None,
    fast_food_data: DataFrame = None,
    pub_data: DataFrame = None,
    park_data: DataFrame = None,
    cafe_data: DataFrame = None,
    kindergarten_data: DataFrame = None,
    mmr_data: DataFrame = None,
    birthplace_data: DataFrame = None,
    assign_address_flag: bool = False,
    rewrite_base_pop: bool = False,
    use_parallel: bool = False,
    ncpu: int = 8,
    data_year: int = None,
    data_percentile: str = "median",
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
        data_year (int, optional): the Year that data to be used (if available). Defaults to None

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
        create_school_and_kindergarten(
            "school",
            tmp_data_path,
            school_data,
            geo_hierarchy,
            assign_address_flag,
            possile_area_levels=["area", "super_area", "region"],
        )

    if kindergarten_data is not None:
        _check_dependancies(
            "kindergarten", deps_list=["geo_hierarchy"], address_deps=[]
        )
        logger.info("Adding kindergarten ...")
        create_school_and_kindergarten(
            "kindergarten",
            tmp_data_path,
            kindergarten_data,
            geo_hierarchy,
            assign_address_flag,
            possile_area_levels=["area", "super_area"],
        )

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
            geo_location,
            assign_address_flag,
            area_name_keys_and_selected_nums={"area": 2, "area_work": 1},
        )

    if restaurant_data is not None:
        _check_dependancies("restaurant", deps_list=["geo_location"], address_deps=[])
        logger.info("Adding restaurant ...")
        create_shared_space(
            tmp_data_path,
            restaurant_data,
            "restaurant",
            geo_location,
            assign_address_flag,
            area_name_keys_and_selected_nums={"area": 3, "area_work": 1},
        )

    if pharmacy_data is not None:
        _check_dependancies("pharmacy", deps_list=["geo_location"], address_deps=[])
        logger.info("Adding pharmacy ...")
        create_shared_space(
            tmp_data_path,
            pharmacy_data,
            "pharmacy",
            geo_location,
            assign_address_flag,
            area_name_keys_and_selected_nums={"area": 2},
        )

    if cafe_data is not None:
        _check_dependancies("cafe", deps_list=["geo_location"], address_deps=[])
        logger.info("Adding cafe_data ...")
        create_shared_space(
            tmp_data_path,
            cafe_data,
            "cafe",
            geo_location,
            assign_address_flag,
            area_name_keys_and_selected_nums={"area": 2},
        )

    if department_store_data is not None:
        _check_dependancies(
            "department_store", deps_list=["geo_location"], address_deps=[]
        )
        logger.info("Adding department_store ...")
        create_shared_space(
            tmp_data_path,
            department_store_data,
            "department_store",
            geo_location,
            assign_address_flag,
            area_name_keys_and_selected_nums={"area": 2},
        )

    if wholesale_data is not None:
        _check_dependancies("wholesale", deps_list=["geo_location"], address_deps=[])
        logger.info("Adding wholesale ...")
        create_shared_space(
            tmp_data_path,
            wholesale_data,
            "wholesale",
            geo_location,
            assign_address_flag,
            area_name_keys_and_selected_nums={"area": 2},
        )

    if fast_food_data is not None:
        _check_dependancies("fast_food", deps_list=["geo_location"], address_deps=[])
        logger.info("Adding fast_food ...")
        create_shared_space(
            tmp_data_path,
            fast_food_data,
            "fast_food",
            geo_location,
            assign_address_flag,
            area_name_keys_and_selected_nums={"area": 1, "area_work": 1},
        )

    if pub_data is not None:
        _check_dependancies("pub", deps_list=["geo_location"], address_deps=[])
        logger.info("Adding pub ...")
        create_shared_space(
            tmp_data_path,
            pub_data,
            "pub",
            geo_location,
            assign_address_flag,
            area_name_keys_and_selected_nums={"area": 1, "area_work": 1},
        )

    if park_data is not None:
        _check_dependancies("park", deps_list=["geo_location"], address_deps=[])
        logger.info("Adding park ...")
        create_shared_space(
            tmp_data_path,
            park_data,
            "park",
            geo_location,
            assign_address_flag,
            area_name_keys_and_selected_nums={"area": 1, "area_work": 1},
        )

    if mmr_data is not None:
        logger.info("Adding MMR data ...")
        create_vaccine(tmp_data_path, mmr_data, data_year, data_percentile)

    if birthplace_data is not None:
        logger.info("Adding birthplace data ...")
        create_birthplace(tmp_data_path, birthplace_data)

    # ---------------------------
    # Export output
    # ---------------------------
    with open(tmp_data_path, "rb") as fid:
        synpop_data = pickle_load(fid)

    output_files = {
        "syspop_base": ["area", "age", "gender", "ethnicity"],
        "syspop_household": [
            "household",
            "dwelling_type",
            "social_economics",
        ],
        "syspop_travel": ["travel_mode_work", "public_transport_trip"],
        "syspop_work_and_school": ["area_work", "company", "school", "kindergarten"],
        "syspop_healthcare": ["primary_hospital", "secondary_hospital", "mmr"],
        "syspop_lifechoice": [
            "supermarket",
            "restaurant",
            "cafe",
            "department_store",
            "wholesale",
            "fast_food",
            "pub",
            "park",
        ],
        "syspop_others": ["hhd_src"],
        "syspop_immigration": ["birthplace"],
    }

    synpop_data["synpop"]["id"] = synpop_data["synpop"].index
    for name, cols in output_files.items():
        output_path = join(output_dir, f"{name}.parquet")
        try:
            synpop_data["synpop"][["id"] + cols].to_parquet(output_path, index=False)
        except KeyError:
            pass

    synpop_data["synadd"].to_parquet(
        join(output_dir, "syspop_location.parquet"), index=False
    )
