from argparse import ArgumentParser
from datetime import datetime
from logging import Logger
from os import makedirs
from os.path import exists, join
from pickle import dump as pickle_dump
from pickle import load as pickle_load

from funcs import MAX_ALLOWED_FAILURE, PEOPLE_CFG
from funcs.create import (
    combine_data,
    dict2df,
    prompt_llm,
    update_location_name,
    update_locations_with_weights,
)
from funcs.utils import check_locations, create_logger
from funcs.vis import plot_diary_percentage


def diary_wrapper(
    day_type: str,
    scenarios: int,
    people: str,
    model_path: str,
    workdir: str,
    overwrite_data: bool = False,
    overwrite_vis: bool = False,
) -> str:
    """Create diary wrapper

    Args:
        day_type (str): weekday or weekend
        scenarios (int): how many scenarios you want to create
        people (str): the people type, e.g., student etc.
        model_path (str): LLAMA model path
        logger (LoggerorNone, optional): logger fid. Defaults to None.

    Returns:
        str: output data path
    """
    start_time = datetime.utcnow()

    if not exists(workdir):
        makedirs(workdir)

    logger = create_logger()  # nvidia-smi

    total_data_list = []
    failure_index = 0
    success_index = 0
    total_index = 0

    agent_features = PEOPLE_CFG[people]["default"]
    agent_features["time"] = day_type
    output_path = join(workdir, f"diary_{people}_{agent_features['age']}_{day_type}.p")

    create_data_flag = False
    if not exists(output_path) or overwrite_data:
        create_data_flag = True

    if create_data_flag:
        while success_index < scenarios:
            if logger is not None:
                logger.info(f"Processing {total_index} scenario ...")

            total_index += 1

            try:
                agent_features_update = PEOPLE_CFG[people][day_type]
                for updated_key in agent_features_update:
                    agent_features[updated_key] = agent_features_update[updated_key]
            except KeyError:
                pass

            if not check_locations(agent_features["locations"]):
                raise Exception("There are unknown locations")

            try:
                proc_data = prompt_llm(
                    agent_features=agent_features, model_path=model_path, print_log=True
                )
                proc_df = dict2df(proc_data)
                success_index += 1
                logger.info(
                    f"Successefully finished {success_index} (Target: {scenarios}; Failure: {failure_index}) ..."
                )
            except (ValueError, KeyError):
                logger.info("Not able to decode data ...")
                failure_index += 1

                if MAX_ALLOWED_FAILURE is not None:
                    if failure_index > MAX_ALLOWED_FAILURE:
                        raise Exception(f"Too many failures ({failure_index})")
                continue
            total_data_list.append(proc_df)

        total_data = combine_data(total_data_list)

        total_data = update_locations_with_weights(total_data, day_type)

        total_data = update_location_name(total_data)

        pickle_dump(
            {"data": total_data, "agent_features": agent_features, "people": people},
            open(output_path, "wb"),
        )
    else:
        with open(output_path, "rb") as fid:
            total_data = pickle_load(fid)["data"]

    vis_dir = join(workdir, "vis")
    vis_path = join(vis_dir, f"diary_{people}_{agent_features['age']}_{day_type}.png")
    create_vis_flag = False
    if not exists(vis_path) or overwrite_vis:
        create_vis_flag = True

    if create_vis_flag:

        if not exists(vis_dir):
            makedirs(vis_dir)

        plot_diary_percentage(
            total_data,
            vis_path,
            title_str=f"{people}, {agent_features['age']}, {day_type}",
        )

    end_time = datetime.utcnow()

    logger.info(f"Used {(end_time - start_time).total_seconds() / 60.0} mins ...")

    logger.info(f"Done ({output_path})...")

    return output_path


if __name__ == "__main__":
    parser = ArgumentParser(description="Creating diary")
    parser.add_argument(
        "--day_type",
        required=True,
        type=str,
        choices=["weekday", "weekend"],
        help="If the diary for weekday or weekend",
    )

    parser.add_argument(
        "--people",
        required=True,
        type=str,
        choices=list(PEOPLE_CFG.keys()),
        help=f"People type from {list(PEOPLE_CFG.keys())}: \n {PEOPLE_CFG}",
    )

    parser.add_argument(
        "--scenarios",
        type=int,
        required=False,
        default=100,
        help="Scenarios number",
    )

    parser.add_argument(
        "--workdir",
        type=str,
        required=False,
        default="/tmp/syspop_llm",
        help="Working directory",
    )

    parser.add_argument(
        "--model_path",
        type=str,
        default="/home/zhangs/Github/llm-abm/llama-2-7b-chat.ggmlv3.q8_0.gguf",  # llama-2-70b-chat.Q4_K_M.gguf / llama-2-7b-chat.ggmlv3.q8_0.gguf
        required=False,
        help="LLM model path",
    )

    args = parser.parse_args(
        # [
        #    "--workdir",
        #    "/tmp/syspop_llm/run_20240324T20/",
        #    "--day_type",
        #    "weekend",
        #    "--scenarios",
        #    "3",
        #    "--people",
        #    "student",
        # ]
    )

    output_path = diary_wrapper(
        args.day_type, args.scenarios, args.people, args.model_path, args.workdir
    )
