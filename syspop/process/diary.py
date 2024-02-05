from datetime import datetime, timedelta
from logging import getLogger

import ray
from numpy import array as numpy_array
from numpy.random import choice as numpy_choice
from numpy.random import normal as numpy_normal
from pandas import DataFrame

DIARY_CFG = {
    "worker": {
        "household": {"weight": 0.3, "time_ranges": [(0, 8), (15, 24)]},
        "travel": {"weight": 0.15, "time_ranges": [(7, 9), (16, 19)]},
        "company": {"weight": 0.4, "time_ranges": [(9, 17)]},
        "supermarket": {"weight": 0.1, "time_ranges": [(8, 9), (17, 20)]},
        "restaurant": {"weight": 0.1, "time_ranges": [(7, 8), (18, 20)]},
        "pharmacy": {"weight": 0.00001, "time_ranges": [(9, 17)]},
    },
    "student": {
        "household": {"weight": 0.3, "time_ranges": [(0, 8), (15, 24)]},
        "school": {"weight": 0.5, "time_ranges": [(9, 15)]},
        "supermarket": {"weight": 0.1, "time_ranges": [(8, 9), (17, 20)]},
        "restaurant": {"weight": 0.1, "time_ranges": [(7, 8), (18, 20)]},
        "pharmacy": {"weight": 0.00001, "time_ranges": [(9, 17)]},
    },
    "default": {
        "household": {"weight": 0.6, "time_ranges": [(0, 24)]},
        "supermarket": {"weight": 0.2, "time_ranges": [(8, 9), (17, 20)]},
        "restaurant": {"weight": 0.2, "time_ranges": [(7, 8), (18, 20)]},
        "pharmacy": {"weight": 0.0001, "time_ranges": [(9, 17)]},
    },
}


logger = getLogger()


def create_diary_single_person(
    ref_time: datetime = datetime(1970, 1, 1, 0),
    time_var: numpy_array = numpy_normal(0.0, 2.0, 100),
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
    while ref_time_proc <= ref_time_end:
        # Get all activities that can be chosen at this time
        available_activities = []
        for activity in activities:
            for start, end in activities[activity]["time_ranges"]:
                start2 = ref_time_start + timedelta(
                    hours=(start - abs(numpy_choice(time_var)))
                )
                end2 = ref_time_start + timedelta(
                    hours=(end + abs(numpy_choice(time_var)))
                )

                if start2 <= ref_time_proc < end2:
                    available_activities.append(activity)

        if available_activities:
            # Choose an activity based on the probabilities
            available_probabilities = [
                activities[proc_activity]["weight"]
                for proc_activity in available_activities
            ]

            # scale up the probability to 1.0
            available_probabilities = numpy_array(available_probabilities)
            available_probabilities /= available_probabilities.sum()

            activity = numpy_choice(available_activities, p=available_probabilities)

            # Add the activity to the diary
            output[ref_time_proc.hour] = activity

        else:
            output[ref_time_proc.hour] = numpy_choice(list(activities.keys()))

        ref_time_proc += timedelta(hours=1)

    return output


@ray.remote
def create_diary_remote(
    syspop_data: DataFrame, ncpu: int, print_log: bool, activities: dict or None = None
) -> DataFrame:
    """Create diaries in parallel processing

    Args:
        workdir (str): Working directory
        syspop_data (DataFrame): Synthetic population
        ncpu (int): Number of CPUs in total
            (this is just for displaying the progress)
    """
    return create_diary(syspop_data, ncpu, print_log, activities_cfg=activities)


def create_diary(
    syspop_data: DataFrame,
    ncpu: int,
    print_log: bool,
    activities_cfg: dict or None = None,
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

        proc_activities = activities_cfg.get(
            "worker"
            if isinstance(proc_people["company"], str)
            else "student" if isinstance(proc_people["school"], str) else "default"
        )

        output = create_diary_single_person(activities=proc_activities)

        all_diaries["id"].append(proc_people.id)

        for j in output:
            all_diaries[j].append(output[j])

    all_diaries = DataFrame.from_dict(all_diaries)

    return all_diaries
