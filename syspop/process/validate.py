from numpy import NaN
from pandas import DataFrame, merge

from syspop.process.vis import (
    validate_vis_barh,
    validate_vis_movement,
    validate_vis_plot,
)


def get_overlapped_areas(area1: DataFrame, area2: DataFrame) -> list:
    """Get overlapped areas between area1 and area2

    Args:
        area1 (DataFrame): area1 dataset
        area2 (DataFrame): area2 dataset

    Returns:
        list: the list of overlapped areas
    """
    area1 = set(area1)
    area2 = set(area2)

    # Find the intersection of sets
    return list(area1.intersection(area2))


def validate_mmr(
    val_dir: str,
    synpop_data: DataFrame,
    mmr_census_data: DataFrame,
    data_year: int or None,
    data_percentile: str or None,
):
    """Validating MMR coverage

    Args:
        val_dir (str): _description_
        synpop_data (DataFrame): _description_
        mmr_census_data (DataFrame): _description_
        data_year (intorNone): _description_
        data_percentile (strorNone): _description_
    """
    if data_year is None:
        data_year = max(list(mmr_census_data["year"].unique()))

    if data_percentile is None:
        data_percentile = "median"

    mmr_census_data = mmr_census_data[mmr_census_data["year"] == data_year]
    mmr_census_data = mmr_census_data[mmr_census_data["percentile"] == data_percentile]
    mmr_census_data = mmr_census_data[mmr_census_data["sa2"].isin(synpop_data.area)]

    mmr_ages = mmr_census_data["age"].unique()
    mmr_ethnicity = mmr_census_data["ethnicity"].unique()

    err = {
        "fully_imms": {"model": {}, "truth": {}},
        "partial_imms": {"model": {}, "truth": {}},
    }

    err_ratio = {"fully_imms": {}, "partial_imms": {}}
    for proc_mmr_age in mmr_ages:

        for imms_ethnicity in mmr_ethnicity:

            proc_mmr_age_min = int(proc_mmr_age.split("-")[0])
            try:
                proc_mmr_age_max = int(proc_mmr_age.split("-")[1])
            except IndexError:
                proc_mmr_age_max = proc_mmr_age_min

            proc_synpop_data = synpop_data[
                (synpop_data["age"] >= proc_mmr_age_min)
                & (synpop_data["age"] <= proc_mmr_age_max)
            ]

            if imms_ethnicity == "Others":
                proc_synpop_data = proc_synpop_data[
                    proc_synpop_data["ethnicity"].isin(["MELAA", "European"])
                ]
            else:
                proc_synpop_data = proc_synpop_data[
                    proc_synpop_data["ethnicity"] == imms_ethnicity
                ]

            proc_mmr_census_data = mmr_census_data[
                (mmr_census_data["age"] == proc_mmr_age)
                & (mmr_census_data["ethnicity"] == imms_ethnicity)
            ]

            for imms_type in ["fully_imms", "partial_imms"]:

                if imms_type == "partial_imms" and proc_mmr_age_min > 17:
                    continue

                try:
                    proc_model = len(
                        proc_synpop_data[proc_synpop_data["mmr"] == imms_type]
                    ) / len(proc_synpop_data)
                except ZeroDivisionError:
                    continue

                proc_truth = proc_mmr_census_data[imms_type].mean()

                proc_mmr_age_str = proc_mmr_age
                if proc_mmr_age == "50-999":
                    proc_mmr_age_str = " >=50"

                err[imms_type]["truth"][
                    f"{imms_ethnicity}: {proc_mmr_age_str} years"
                ] = proc_truth

                if imms_type == "partial_imms":
                    proc_model *= 3.0

                err[imms_type]["model"][
                    f"{imms_ethnicity}: {proc_mmr_age_str} years"
                ] = proc_model

                try:
                    err_ratio[imms_type][
                        f"{imms_ethnicity}: {proc_mmr_age_str} years"
                    ] = (100.0 * (proc_model - proc_truth) / proc_truth)
                except ZeroDivisionError:
                    err_ratio[imms_type][
                        f"{imms_ethnicity}: {proc_mmr_age_str} years"
                    ] = None

    for imms_type in ["fully_imms", "partial_imms"]:
        validate_vis_barh(
            val_dir,
            err[imms_type],
            f"Validation: immunisation ({imms_type})",
            f"validation_{imms_type}_err",
            "Immunisation percentgae",
            # f"Error: Model and Truth \n Model: {round(sum(err['model'].values()), 2)}; Truth: {round(sum(err['truth'].values()), 2)}",
            "Age and imms status",
            plot_ratio=False,
        )
        validate_vis_barh(
            val_dir,
            err_ratio[imms_type],
            f"Validation: immunisation ({imms_type})",
            f"validation_{imms_type}",
            "Error (%): (model - truth) / truth",
            "Age and imms status",
        )


def validate_commute_area(
    val_dir: str,
    synpop_data: DataFrame,
    commute_census_data: DataFrame,
):
    """Validate commute (work-home area)

    Args:
        val_dir (str): Validation directory
        synpop_data (DataFrame): Synthetic population
        commute_census_data (DataFrame): Commute census dataset
    """
    all_areas = get_overlapped_areas(
        synpop_data["area"], commute_census_data["area_home"]
    )

    model_data = synpop_data[synpop_data["area"].isin(all_areas)][["area", "area_work"]]
    model_data = model_data[model_data["area_work"] != -9999]
    model_data = (
        model_data.groupby(["area", "area_work"]).size().reset_index(name="total")
    )
    model_data = model_data.rename(columns={"area": "area_home"})

    truth_data = commute_census_data[commute_census_data["area_home"].isin(all_areas)]
    truth_data["total"] = truth_data[
        [col for col in truth_data.columns if col not in ["area_home", "area_work"]]
    ].sum(axis=1)
    truth_data = truth_data[["area_home", "area_work", "total"]]

    validate_vis_movement(
        val_dir,
        model_data,
        truth_data,
        merge_method="left",  # left, inner
    )

    """
    from matplotlib.pyplot import scatter, xlim, ylim, savefig, close, colorbar, clim, xlabel, ylabel, title, plot
    x = model_data[model_data["total"] > 50]
    y = truth_data[truth_data["total"] > 50]
    x = x[x["area_home"] != x["area_work"]]
    y = y[y["area_home"] != y["area_work"]]

    merged_df = pd.merge(x, y, on=['area_home', 'area_work'], suffixes=('_x', '_y'), how='inner')

    factor = merged_df["total_x"].sum() / merged_df["total_y"].sum()
    merged_df["total_y"] = merged_df["total_y"] * 1.2

    min_value = min(merged_df[["area_home", "area_work"]].min())
    max_value = min(merged_df[["area_home", "area_work"]].max())

    plot([min_value, max_value], [min_value, max_value], "k")

    scatter(
        merged_df["area_home"],
        merged_df["area_work"],
        c=merged_df["total_x"],
        s=merged_df["total_x"],
        cmap='jet',
        alpha=0.5,
    )
    title("Synthetic population")
    colorbar()
    xlim(min_value - 1000, max_value + 1000)
    ylim(min_value - 1000, max_value+ 1000)
    clim([50, 450])
    xlabel("SA2")
    ylabel("SA2")
    savefig("test2.png", bbox_inches='tight')
    close()

    plot([min_value, max_value], [min_value, max_value], "k")
    scatter(
        merged_df["area_home"],
        merged_df["area_work"],
        c=merged_df["total_y"],
        s=merged_df["total_y"],
        cmap='jet',
        alpha=0.5,
    )
    title("Census 2018")
    colorbar()
    xlim(min_value - 1000, max_value + 1000)
    ylim(min_value - 1000, max_value+ 1000)
    clim([50, 450])
    xlabel("SA2")
    ylabel("SA2")
    savefig("test1.png", bbox_inches='tight')
    close()

    """
    """
    # get all unique home <-> work
    unique_area_combinations = truth_data[["area_home", "area_work"]].drop_duplicates()
    err_ratio = []
    err = {"truth": [], "model": []}
    for i, proc_combination in unique_area_combinations.iterrows():
        proc_truth = truth_data[
            (truth_data["area_home"] == proc_combination["area_home"])
            & (truth_data["area_work"] == proc_combination["area_work"])
        ]["total"].values[0]

        proc_model = model_data[
            (model_data["area_home"] == proc_combination["area_home"])
            & (model_data["area_work"] == proc_combination["area_work"])
        ]

        if len(proc_model) == 0:
            continue

        proc_model = proc_model["total"].values[0]

        err_ratio.append(100.0 * (proc_model - proc_truth) / proc_truth)

        err["truth"].append(proc_truth)
        err["model"].append(proc_model)

    validate_vis_plot(
        val_dir,
        err,
        "Home and work commute",
        "validation_work_commute_err",
        f"Travel areas (between home and work) \n Model: {sum(err['model'])}; Truth: {sum(err['truth'])}",
        "Commute people",
        plot_ratio=False,
    )

    validate_vis_plot(
        val_dir,
        err_ratio,
        "Home and work commute",
        "validation_work_commute",
        "Travel areas (between home and work)",
        "Error (Commute people, %): (model - truth) / truth",
    )
    """


def validate_commute_mode(
    val_dir: str,
    synpop_data: DataFrame,
    commute_census_data: DataFrame,
):
    """Validate commute (commute mode)

    Args:
        val_dir (str): Validation directory
        synpop_data (DataFrame): Synthetic population
        commute_census_data (DataFrame): Commute census dataset
    """
    all_areas = get_overlapped_areas(
        synpop_data["area"], commute_census_data["area_home"]
    )

    model_data = synpop_data[synpop_data["area"].isin(all_areas)][["travel_mode_work"]]
    model_data = model_data.dropna()
    truth_data = commute_census_data[
        (commute_census_data["area_home"].isin(all_areas))
        & (commute_census_data["area_work"].isin(all_areas))
    ]

    all_travel_methods = list(model_data["travel_mode_work"].unique())

    err_ratio = {}
    err = {"model": {}, "truth": {}}
    for proc_travel_method in all_travel_methods:
        proc_model_data = len(
            model_data[model_data["travel_mode_work"] == proc_travel_method]
        )
        proc_truth_data = truth_data[proc_travel_method].sum()
        err_ratio[proc_travel_method] = (
            100.0 * (proc_model_data - proc_truth_data) / proc_truth_data
        )
        err["truth"][proc_travel_method] = proc_truth_data
        err["model"][proc_travel_method] = proc_model_data  # / 2.0

    validate_vis_barh(
        val_dir,
        err,
        f"Validation: travel modes",
        f"validation_work_travel_modes_err",
        f"Error: Model and Truth \n Model: {sum(err['model'].values())}; Truth: {sum(err['truth'].values())}",
        "Business code",
        plot_ratio=False,
    )
    validate_vis_barh(
        val_dir,
        err_ratio,
        f"Validation: travel modes",
        f"validation_work_travel_modes",
        "Error (%): (model - truth) / truth",
        "Travel methods",
    )


def validate_work(
    val_dir: str,
    synpop_data: DataFrame,
    work_census_data: DataFrame,
    exlcuded_business_code: list = [],
):
    """Validate work data

    Args:
        val_dir (str): Validation directory
        synpop_data (DataFrame): Synthetic population
        work_census_data (DataFrame): Work census dataset
    """
    for work_type in ["employer", "employee"]:
        census_data = work_census_data[work_type]

        # Find the intersection of areas
        all_areas = get_overlapped_areas(synpop_data["area"], census_data["area"])
        truth_data = census_data[census_data["area"].isin(all_areas)]
        model_data = synpop_data[synpop_data["area"].isin(all_areas)][
            ["area", "company"]
        ]
        model_data = model_data[model_data["company"].notna()]
        model_data["business_code"] = model_data["company"].str.split("_").str[0]

        all_business_code = list(census_data["business_code"].unique())

        err_ratio = {}
        err = {"truth": {}, "model": {}}
        for proc_code in all_business_code:

            if proc_code in exlcuded_business_code:
                continue

            total_truth = truth_data[truth_data["business_code"] == proc_code][
                f"{work_type}_number"
            ].sum()

            if total_truth == 0:
                err_ratio[proc_code] = NaN

            if work_type == "employee":
                total_model = len(model_data[model_data["business_code"] == proc_code])
            elif work_type == "employer":
                total_model = len(
                    model_data[model_data["business_code"] == proc_code][
                        "company"
                    ].unique()
                )

            err["truth"][proc_code] = total_truth
            err["model"][proc_code] = total_model
            if total_truth == 0:
                err_ratio[proc_code] = NaN
            else:
                err_ratio[proc_code] = 100.0 * (total_model - total_truth) / total_truth

        validate_vis_barh(
            val_dir,
            err,
            f"Validation: number of {work_type} for different sectors",
            f"validation_work_{work_type}_err",
            f"Number of {work_type}",
            # f"Error: Model and Truth \n Model: {sum(err['model'].values())}; Truth: {sum(err['truth'].values())}",
            "Business code",
            plot_ratio=False,
        )

        validate_vis_barh(
            val_dir,
            err_ratio,
            f"Validation: number of {work_type} for different sectors",
            f"validation_work_{work_type}",
            "Error (%): (model - truth) / truth",
            "Business code",
        )


def validate_household(
    val_dir: str,
    synpop_data: DataFrame,
    household_census_data: DataFrame,
    valid_thres: int = 1000,
):
    """Validate household (e.g., The number of household based on
    the number of children)

    Args:
        val_dir (str): Validation directory
        synpop_data (DataFrame): Synthetic population
        household_census_data (DataFrame): Census data
    """
    overlapping_areas = get_overlapped_areas(
        synpop_data["area"], household_census_data["area"]
    )
    synpop_data = synpop_data[synpop_data["hhd_src"] == "hhd"]

    # get model:
    data_model = synpop_data[["area", "household"]]
    data_model = data_model[data_model["area"].isin(overlapping_areas)]
    data_model["household_composition"] = (
        data_model["household"].str.split("_").apply(lambda x: "_".join(x[1:-1]))
    )
    # get truth:
    data_truth = household_census_data[
        household_census_data["area"].isin(overlapping_areas)
    ]

    data_truth = data_truth[data_truth["adults_num"] != "unknown"]

    data_truth["adults_num"] = data_truth["adults_num"].astype(int)

    data_truth["children_num"] = data_truth["people_num"] - data_truth["adults_num"]
    data_truth["adults_num"] = data_truth["adults_num"].clip(lower=0)
    data_truth["household_composition"] = (
        data_truth["adults_num"].astype(str)
        + "_"
        + data_truth["children_num"].astype(str)
    )

    # Convert the extracted values to numeric if needed
    all_household_composition = set(
        list(data_model["household_composition"].unique())
        + list(data_truth["household_composition"].unique())
    )

    err_ratio = {}
    err = {"truth": {}, "model": {}}
    for proc_household_composition in all_household_composition:
        proc_truth = data_truth[
            data_truth["household_composition"] == proc_household_composition
        ]["household_num"].sum()
        proc_model = len(
            data_model[
                data_model["household_composition"] == proc_household_composition
            ]["household"].unique()
        )

        if proc_model > valid_thres or proc_truth > valid_thres:
            adult_num, children_num = proc_household_composition.split("_")
            proc_household_composition_updated = (
                f"Adult: {adult_num}; Children: {children_num}"
            )

            err["model"][proc_household_composition_updated] = proc_model
            err["truth"][proc_household_composition_updated] = proc_truth

            err_ratio[proc_household_composition_updated] = (
                100.0 * (proc_model - proc_truth) / proc_truth
            )

    validate_vis_barh(
        val_dir,
        err,
        f"Validation: household composition",
        f"validation_household_err",
        f"Error: Model and Truth \n Model: {sum(err['model'].values())}; Truth: {sum(err['truth'].values())}",
        "Household composition",
        plot_ratio=False,
        figure_size=(6, 15),
    )

    validate_vis_barh(
        val_dir,
        err_ratio,
        f"Validation: household composition",
        f"validation_household",
        "Error (%): (model - truth) / truth",
        "Household composition",
    )


def validate_base_pop_and_age(
    val_dir: str,
    synpop_data: DataFrame,
    census_data: DataFrame,
    key_to_verify: str,
    values_to_verify: list,
    age_interval: int = 10,
):
    """Validate the data for age/ethnicity and age/gender

    Args:
        val_dir (str): validation directory
        synpop_data (DataFrame): synthetic population data
        census_data (DataFrame): census gender/ethnicity data
        key_to_verify (str): gender or ethnicity
        values_to_verify (list): such as [male, female] or [European, Maori, Pacific, Asian, MELAA]
    """
    census_truth_all = census_data[
        census_data["area"].isin(list(synpop_data["area"].unique()))
    ]

    for proc_value in values_to_verify:
        # ---------------------------
        # Step 1: Get truth data
        # ---------------------------
        if proc_value == "total":
            proc_census_truth = census_truth_all
        else:
            proc_census_truth = census_truth_all[
                census_truth_all[key_to_verify] == proc_value
            ]

        all_ages_list = [
            item
            for item in list(proc_census_truth.columns)
            if item not in ["area", key_to_verify]
        ]

        proc_census_truth = {col: proc_census_truth[col].sum() for col in all_ages_list}

        # ---------------------------
        # Step 2: Get model data
        # ---------------------------
        if proc_value == "total":
            proc_pop_model = synpop_data.groupby("age").size().to_dict()
        else:
            proc_pop_model = (
                synpop_data[synpop_data[key_to_verify] == proc_value]
                .groupby("age")
                .size()
                .to_dict()
            )

        proc_pop_model = {key: proc_pop_model.get(key, 0) for key in all_ages_list}

        # ---------------------------
        # Step 3: sum the number of people from the age of
        # interval of 1 years to age_interval
        # ---------------------------
        min_age = min(all_ages_list)
        max_age = max(all_ages_list)
        proc_census_truth_sum = {}
        proc_pop_model_sum = {}
        for i in range(min_age, max_age + 1, age_interval):
            proc_census_truth_sum[i] = sum(
                proc_census_truth[j]
                for j in range(
                    i,
                    i + age_interval if i + age_interval <= max_age else max_age + 1,
                )
            )

            proc_pop_model_sum[i] = sum(
                proc_pop_model[j]
                for j in range(
                    i,
                    i + age_interval if i + age_interval <= max_age else max_age + 1,
                )
            )

        # ---------------------------
        # step 4: Get error ratio
        # ---------------------------
        proc_err_ratio = {}
        proc_err = {"model": {}, "truth": {}}
        for proc_key in range(min_age, max_age + 1, age_interval):
            proc_err_ratio[proc_key] = (
                100.0
                * (proc_pop_model_sum[proc_key] - proc_census_truth_sum[proc_key])
                / proc_census_truth_sum[proc_key]
            )
            proc_err["model"][proc_key] = proc_pop_model_sum[proc_key]
            proc_err["truth"][proc_key] = proc_census_truth_sum[proc_key]

        # ---------------------------
        # step 5: Vis
        # ---------------------------

        validate_vis_barh(
            val_dir,
            proc_err,
            f"Validation {key_to_verify} (distribution) and age {proc_value}",
            f"validation_age_{key_to_verify}_{proc_value}_err",
            f"Error: Model and Truth \n Model: {int(sum(proc_err['model'].values()))}; Truth: {int(sum(proc_err['truth'].values()))}",
            "Age",
            plot_ratio=False,
        )

        validate_vis_barh(
            val_dir,
            proc_err_ratio,
            f"Validation {key_to_verify} (distribution) and age {proc_value}",
            f"validation_age_{key_to_verify}_{proc_value}",
            "Error (%): (model - truth) / truth",
            "Age",
        )
