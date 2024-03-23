from argparse import ArgumentParser
from os import makedirs
from os.path import exists, join
from pickle import load as pickle_load

from funcs import DAY_TYPE_WEIGHT
from funcs.vis import plot_diary_percentage
from pandas import concat


def combine_diary_wrapper(
    workdir: str, people_list: list, age_list: list, day_list: list
):
    """Combine diary together

    Args:
        workdir (str): Working directory holding the diary data
        people_list (list): people type list
        age_list (list): age list
        day_list (list): day type list
    """
    all_data = []
    filename = ""
    for proc_people in people_list:
        for proc_age in age_list:
            for proc_day in day_list:

                # proc_data_path = f"diary_{proc_people}_{proc_year}_{proc_day}.p"
                proc_data_path = join(
                    workdir, f"diary_{proc_people}_{proc_age}_{proc_day}.p"
                )

                if not exists(proc_data_path):
                    continue

                filename += f"{proc_people}_{proc_age}_{proc_day}"
                with open(proc_data_path, "rb") as fid:
                    proc_data = pickle_load(fid)["data"]

                proc_weight = DAY_TYPE_WEIGHT[proc_day]["weight"]

                for _ in range(proc_weight):

                    all_data.append(proc_data)

    all_data = concat(all_data)

    vis_dir = join(workdir, "vis")
    if not exists(vis_dir):
        makedirs(vis_dir)

    plot_diary_percentage(
        all_data,
        join(vis_dir, f"combined_{filename}.png"),
        title_str="Combined schedule",
    )


if __name__ == "__main__":
    parser = ArgumentParser(description="Combining diary")

    parser.add_argument(
        "--workdir",
        type=str,
        required=False,
        default="/tmp/syspop_llm",
        help="Working directory",
    )

    parser.add_argument(
        "--people_list",
        nargs="+",
        help="List of people types, e.g., student, worker1 etc.",
    )

    parser.add_argument(
        "--age_list",
        nargs="+",
        help="List of age, e.g., 0-18, 5-13",
    )

    parser.add_argument(
        "--day_list",
        nargs="+",
        help="List of day types, e.g., weekend, weekday",
    )

    args = parser.parse_args(
        [
            "--people_list",
            "toddler",
            "student",
            "--age_list",
            "2-5",
            "--day_list",
            "weekday",
            "weekend",
        ]
    )

    combine_diary_wrapper(args.workdir, args.people_list, args.age_list, args.day_list)
