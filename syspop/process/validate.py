from pandas import DataFrame, merge
from process.vis import validate_vis_barh


def validate_household(
    val_dir: str, synpop_data: DataFrame, household_census_data: DataFrame
):
    """Validate household (e.g., The number of household based on
    the number of children)

    Args:
        val_dir (str): Validation directory
        synpop_data (DataFrame): Synthetic population
        household_census_data (DataFrame): Census data
    """
    synpop_area = set(synpop_data["area"])
    census_area = set(household_census_data["area"])

    # Find the intersection of sets
    overlapping_areas = list(synpop_area.intersection(census_area))

    # get model:
    data_model = synpop_data[["area", "household"]]
    data_model = data_model[data_model["area"].isin(overlapping_areas)]
    data_model["children_num"] = data_model["household"].str.split("_").str[1]

    # get truth:
    data_truth = household_census_data[
        household_census_data["area"].isin(overlapping_areas)
    ]

    # Convert the extracted values to numeric if needed
    data_model["children_num"] = data_model["children_num"].astype(int)

    err_ratio = {}

    for proc_children_num in [0, 1, 2, 3, 4]:
        data_truth[proc_children_num] = data_truth[proc_children_num].astype(int)
        proc_truth = data_truth[proc_children_num].sum()
        proc_model = data_model["children_num"].value_counts().get(proc_children_num, 0)

        proc_model = data_model.loc[data_model["children_num"] == proc_children_num]

        # Extract unique households from the filtered DataFrame
        proc_model = len(proc_model["household"].unique())

        err_ratio[proc_children_num] = 100.0 * (proc_model - proc_truth) / proc_truth

    validate_vis_barh(
        val_dir,
        err_ratio,
        f"Validation: number of children in a household",
        f"validation_household",
        "Error (%): (model - truth) / truth",
        "Number of children",
    )


def validate_base_pop_and_age(
    val_dir: str,
    synpop_data: DataFrame,
    census_data: DataFrame,
    key_to_verify: str,
    values_to_verify: list,
    age_interval: int = 10,
):
    """Validate the data for age/ethnicity and age

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
        for proc_key in range(min_age, max_age + 1, age_interval):
            proc_err_ratio[proc_key] = (
                100.0
                * (proc_pop_model_sum[proc_key] - proc_census_truth_sum[proc_key])
                / proc_census_truth_sum[proc_key]
            )

        # ---------------------------
        # step 5: Vis
        # ---------------------------
        validate_vis_barh(
            val_dir,
            proc_err_ratio,
            f"Validation {key_to_verify} (distribution) and age {proc_value}",
            f"validation_age_{key_to_verify}_{proc_value}",
            "Error (%): (model - truth) / truth",
            "Age",
        )
