from argparse import ArgumentParser
from glob import glob
from os import makedirs
from os.path import exists, join
from pickle import dump as pickle_dump
from pickle import load as pickle_load

from funcs import DAY_TYPE_WEIGHT
from funcs.vis import plot_diary_percentage
from pandas import concat


def create_group_data_wrapper(
    workdir: str, group_name: str, people_list: list, age_list: list, day_list: list
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

                filename += f"{proc_people}_{proc_age}_{proc_day}_"
                with open(proc_data_path, "rb") as fid:
                    proc_data = pickle_load(fid)["data"]

                proc_weight = DAY_TYPE_WEIGHT[proc_day]["weight"]

                for _ in range(proc_weight):

                    all_data.append(
                        proc_data[["Hour", "Activity", "Location", "People_id"]]
                    )

    filename = filename[:-1]
    all_data = concat(all_data)

    output_dir = {}
    for proc_dir_name in ["combined", "vis"]:
        output_dir[proc_dir_name] = join(workdir, proc_dir_name)
        if not exists(output_dir[proc_dir_name]):
            makedirs(output_dir[proc_dir_name])

    pickle_dump(
        {"type": group_name, "data": all_data},
        open(join(output_dir["combined"], f"{filename}.p"), "wb"),
    )

    plot_diary_percentage(
        all_data,
        join(output_dir["vis"], f"combined_{filename}.png"),
        title_str="Combined schedule",
    )


def create_all_data(workdir):
    """Combine all pickle files together

    Args:
        workdir (_type_): _description_
    """
    all_data = {}
    for proc_file in glob(join(workdir, "combined", "*.p")):
        with open(proc_file, "rb") as fid:
            proc_data = pickle_load(fid)

        all_data[proc_data["type"]] = proc_data["data"]

    pickle_dump(
        all_data,
        open(join(workdir, "combined", "combined_all.p"), "wb"),
    )


if __name__ == "__main__":
    parser = ArgumentParser(description="Combining diary")

    parser.add_argument(
        "--workdir",
        type=str,
        required=False,
        default="/tmp/syspop_llm/combined",
        help="Working directory",
    )

    parser.add_argument(
        "--create_group_data",
        help="Creating group combined data (requiring people_list, age_list and day_list)",
        action="store_true",
    )

    parser.add_argument(
        "--group_name",
        type=str,
        help="Group name, e.g., student",
        required=False,
        default=None,
    )

    parser.add_argument(
        "--people_list",
        nargs="+",
        help="List of people types, e.g., student, worker1 etc.",
        required=False,
        default=None,
    )

    parser.add_argument(
        "--age_list",
        nargs="+",
        help="List of age, e.g., 0-18, 5-13",
        required=False,
        default=None,
    )

    parser.add_argument(
        "--day_list",
        nargs="+",
        help="List of day types, e.g., weekend, weekday",
        required=False,
        default=None,
    )

    parser.add_argument(
        "--create_all_data",
        help="Combining all (group combined) data available in the combined directory",
        action="store_true",
    )

    args = parser.parse_args(
        [
            "--workdir",
            "/tmp/syspop_llm/run_20240323T21/",
            "--group_name",
            "student",
            "--people_list",
            # "toddler",
            "student",
            "--age_list",
            "6-18",
            "--day_list",
            "weekday",
            "weekend",
            "--create_group_data",
            "--create_all_data",
        ]
    )

    if args.create_group_data:
        if (
            args.group_name is None
            or args.people_list is None
            or args.age_list is None
            or args.day_list is None
        ):
            raise Exception(
                "group_name/people_list/age_list/day_list is required if create_group_data is on"
            )
        create_group_data_wrapper(
            args.workdir,
            args.group_name,
            args.people_list,
            args.age_list,
            args.day_list,
        )

    if args.create_all_data:
        create_all_data(args.workdir)
