from collections import Counter as collections_counter
from copy import deepcopy
from datetime import datetime, timedelta
from logging import getLogger

import ray
from numpy import array as numpy_array
from numpy.random import choice as numpy_choice
from numpy.random import normal as numpy_normal
from pandas import DataFrame
from process import DIARY_CFG, MAPING_DIARY_CFG_LLM_DIARY
from process.utils import round_a_datetime

logger = getLogger()


def _get_updated_weight(target_value: int, target_weight: dict):
    """Get updated weight from age_weight and time_weight

    Args:
        target_value (int): For example, age like 13
        target_weight (dict): age update weight such as:
            {0-3: 123, 23-123: 123, ...}
    Returns:
        _type_: _description_
    """
    if target_weight is None:
        return 1.0

    for key in target_weight:
        start_target_weight, end_target_weight = map(int, key.split("-"))
        if start_target_weight <= target_value <= end_target_weight:
            return target_weight[key]
    return 1.0


def create_diary_single_person(
    ref_time: datetime = datetime(1970, 1, 1, 0),
    time_var: numpy_array or None = numpy_normal(0.0, 1.5, 100),
    activities: dict = DIARY_CFG["default"],
) -> dict:
    """Create diary for one single person

    Args:
        ref_time (datetime, optional): Reference time. Defaults to datetime(1970, 1, 1, 0).
        time_var (numpy_array, optional): randomrized hours range for selecting an activity.
            Defaults to numpy_normal(0.0, 2.0, 100).
        activities (dict, optional): Activity to be chosen from. Defaults to DIARY_CFG["default"].

    Returns:
        dict: _description_
    """
    ref_time_start = ref_time
    ref_time_end = ref_time + timedelta(hours=24)

    output = {}
    ref_time_proc = ref_time_start
    while ref_time_proc < ref_time_end:
        # Get all activities that can be chosen at this time
        available_activities = []
        for activity in activities:

            if activity == "random_seeds":
                continue

            for start, end in activities[activity]["time_ranges"]:
                time_choice = abs(numpy_choice(time_var)) if time_var is not None else 0
                start2 = round_a_datetime(
                    ref_time + timedelta(hours=start - time_choice)
                )
                end2 = round_a_datetime(ref_time + timedelta(hours=end + time_choice))

                if start2 <= ref_time_proc < end2:

                    if activities[activity]["max_occurrence"] is None:
                        available_activities.append(activity)
                    else:
                        activity_counts = dict(collections_counter(output.values()))
                        if activity not in activity_counts:
                            available_activities.append(activity)
                        else:
                            if (
                                activity_counts[activity]
                                <= activities[activity]["max_occurrence"]
                            ):
                                available_activities.append(activity)

        if available_activities:
            # Choose an activity based on the probabilities
            available_probabilities = [
                activities[proc_activity]["weight"]
                * _get_updated_weight(
                    ref_time_proc.hour, activities[proc_activity]["time_weight"]
                )
                for proc_activity in available_activities
            ]

            total_p = sum(available_probabilities)

            if total_p < 1.0:
                available_activities.extend(activities["random_seeds"])
                remained_p = 1.0 - total_p
                remained_p = remained_p / (len(activities["random_seeds"]))
                remained_p = len(activities["random_seeds"]) * [remained_p]
                available_probabilities.extend(remained_p)
                total_p = sum(available_probabilities)

            # scale up the probability to 1.0
            available_probabilities = numpy_array(available_probabilities)
            available_probabilities /= total_p

            activity = numpy_choice(available_activities, p=available_probabilities)

            # Add the activity to the diary
            output[ref_time_proc.hour] = activity

        else:
            try:
                activity_list = list(activities.keys())
                activity_list.remove("random_seeds")
            except ValueError:
                pass
            output[ref_time_proc.hour] = numpy_choice(activity_list)

        ref_time_proc += timedelta(hours=1)

    return output


@ray.remote
def create_diary_remote(
    syspop_data: DataFrame,
    ncpu: int,
    print_log: bool,
    activities: dict or None = None,
    llm_diary_data: dict or None = None,
) -> DataFrame:
    """Create diaries in parallel processing

    Args:
        workdir (str): Working directory
        syspop_data (DataFrame): Synthetic population
        ncpu (int): Number of CPUs in total
            (this is just for displaying the progress)
    """
    return create_diary(
        syspop_data,
        ncpu,
        print_log,
        activities_cfg=activities,
        llm_diary_data=llm_diary_data,
    )


def update_weight_by_age(activities_input: dict, age: int) -> dict:
    """Update the activity weight

    Args:
        activities_input (dict): activity configuration, e.g.,
            {'weight': 0.0001, 'time_ranges': [(...)],
            'age_weight': {'0-5': 0.1, '60-70': 0.1, '70-80': 0.01, '80-999': 1e-05}}
        age (int): such as 13

    Returns:
        dict: Updated activity
    """
    activities_output = deepcopy(activities_input)

    for proc_activity_name in activities_output:
        if proc_activity_name == "random_seeds":
            continue
        activities_output[proc_activity_name]["weight"] *= _get_updated_weight(
            age, activities_output[proc_activity_name]["age_weight"]
        )
    return activities_output


def create_diary(
    syspop_data: DataFrame,
    ncpu: int,
    print_log: bool,
    activities_cfg: dict or None = None,
    llm_diary_data: dict or None = None,
) -> DataFrame:
    """Create diaries

    Args:
        workdir (str): Working directory
        syspop_data (DataFrame): Synthetic population
        ncpu (int): Number of CPUs in total
            (this is just for displaying the progress)
    """

    if activities_cfg is None:
        activities_cfg = DIARY_CFG

    all_diaries = {proc_hour: [] for proc_hour in range(24)}
    all_diaries["id"] = []
    total_people = len(syspop_data)
    for i in range(total_people):

        proc_people = syspop_data.iloc[i]
        if print_log:
            logger.info(
                f"Processing [{i}/{total_people}]x{ncpu}: {100.0 * round(i/total_people, 2)}x{ncpu} %"
            )

        if llm_diary_data is None:
            proc_activities = activities_cfg.get(
                "worker"
                if isinstance(proc_people["company"], str)
                else "student" if isinstance(proc_people["school"], str) else "default"
            )

            proc_activities_updated = update_weight_by_age(
                proc_activities, proc_people.age
            )

            output = create_diary_single_person(activities=proc_activities_updated)
        else:
            output = create_diary_single_person_llm(
                llm_diary_data, proc_people.age, proc_people.company, proc_people.school
            )

        all_diaries["id"].append(proc_people.id)

        for j in output:
            all_diaries[j].append(output[j])

    all_diaries = DataFrame.from_dict(all_diaries)

    return all_diaries


def create_diary_single_person_llm(
    llm_diary_data: dict, people_age: int, people_company: str, people_school: str
) -> dict:
    """Create diary from LLM_diary

    Args:
        llm_diary_data (dict): LLM diary data
        people_age (int): agent's age
        people_company (str): agent' company (can be None)
        people_school (str): agent's school (can be None)

    Returns:
        dict: People's diary
    """

    if people_age < 6:
        proc_llm_data = llm_diary_data["toddler"]
    elif people_age > 65:
        proc_llm_data = llm_diary_data["retiree"]
    elif people_company is not None:
        proc_llm_data = llm_diary_data["worker"]
    elif people_school is not None:
        proc_llm_data = llm_diary_data["student"]
    else:
        proc_llm_data = llm_diary_data["not_in_employment"]

    output = {}
    for hour in proc_llm_data.index:
        probabilities = proc_llm_data.loc[hour]
        location = numpy_choice(probabilities.index, p=probabilities.values)
        output[hour] = location

    """
    all_unique_locs = []
    for proc_key_loc in list(DIARY_CFG.keys()):
        if proc_key_loc == "random_seeds":
            continue
        all_unique_locs.extend(list(DIARY_CFG[proc_key_loc].keys()))

    all_unique_locs = list(set(all_unique_locs))
    """
    # Initialize an empty dictionary to store the converted values
    updated_output = {}

    # Iterate through the items in dict B
    for key, value in output.items():
        # Iterate through the items in dict A to find the key
        for a_key, a_value in MAPING_DIARY_CFG_LLM_DIARY.items():
            if value in a_value:
                # Assign the key from dict A to the converted dictionary
                updated_output[key] = a_key
                break
            updated_output[key] = value

    return updated_output
