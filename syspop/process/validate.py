from pandas import DataFrame
from process.vis import validate_vis_barh


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


def validate_gender_and_age(
    val_dir: str, synpop_data: DataFrame, pop_gender: DataFrame, age_interval: int = 10
) -> dict:
    """Validate the data for gender and age

    Args:
        synpop_data (DataFrame): synthetic population data
        pop_gender (DataFrame): census gender data
        age_interval (int, optional): age interval to be used. Defaults to 10.

    Returns:
        dict: error information
    """

    pop_gender_truth = pop_gender[
        pop_gender["area"].isin(list(synpop_data["area"].unique()))
    ]

    for proc_gender in ["male", "female", "total"]:
        # ---------------------------
        # Step 1: Get truth data
        # ---------------------------
        if proc_gender == "total":
            pop_gender_truth_verif = pop_gender_truth
        else:
            pop_gender_truth_verif = pop_gender_truth[
                pop_gender_truth["gender"] == proc_gender
            ]

        all_ages_list = [
            item
            for item in list(pop_gender_truth_verif.columns)
            if item not in ["area", "gender"]
        ]

        pop_gender_truth_verif = {
            col: pop_gender_truth_verif[col].sum() for col in all_ages_list
        }

        # ---------------------------
        # Step 2: Get model data
        # ---------------------------
        if proc_gender == "total":
            pop_gender_model_verif = synpop_data.groupby("age").size().to_dict()
        else:
            pop_gender_model_verif = (
                synpop_data[synpop_data["gender"] == proc_gender]
                .groupby("age")
                .size()
                .to_dict()
            )

        pop_gender_model_verif = {
            key: pop_gender_model_verif.get(key, 0) for key in all_ages_list
        }

        # ---------------------------
        # Step 3: sum the number of people from the age of
        # interval of 1 years to age_interval
        # ---------------------------
        min_age = min(all_ages_list)
        max_age = max(all_ages_list)
        pop_gender_truth_verif_sum = {}
        pop_gender_model_verif_sum = {}
        for i in range(min_age, max_age + 1, age_interval):
            pop_gender_truth_verif_sum[i] = sum(
                pop_gender_truth_verif[j]
                for j in range(
                    i,
                    i + age_interval if i + age_interval <= max_age else max_age + 1,
                )
            )

            pop_gender_model_verif_sum[i] = sum(
                pop_gender_model_verif[j]
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
                * (
                    pop_gender_model_verif_sum[proc_key]
                    - pop_gender_truth_verif_sum[proc_key]
                )
                / pop_gender_truth_verif_sum[proc_key]
            )

        # ---------------------------
        # step 5: Vis
        # ---------------------------
        validate_vis_barh(
            val_dir,
            proc_err_ratio,
            f"Validation gender (distribution) and age {proc_gender}",
            f"validation_age_{proc_gender}",
            "Error (%): (model - truth) / truth",
            "Age",
        )
