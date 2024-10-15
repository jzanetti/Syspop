# export PYTHONPATH=~/Github/Syspop/etc/scripts_nz
from argparse import ArgumentParser, BooleanOptionalAction
from os import makedirs
from os.path import exists

from funcs.proj.population import project_pop_data
from funcs.proj.utils import copy_others as project_copy_others
from funcs.proj.validation import pop_validation
from funcs.proj.work import project_work_data
from funcs.wrapper import (
    create_geography_wrapper,
    create_hospital_wrapper,
    create_household_wrapper,
    create_kindergarten_wrapper,
    create_others_wrapper,
    create_population_wrapper,
    create_school_wrapper,
    create_shared_space_wrapper,
    create_travel_wrapper,
    create_work_wrapper,
)

from funcs.utils import read_cfg

def import_raw_data(workdir: str, input_cfg: str):
    """Imports and processes raw data for various demographic and geographic categories.

    This function checks if the specified working directory 
    exists and creates it if it does not.
    It then calls a series of wrapper functions to 
    create and process data for different categories such as population,
    household, geography, commute, work, school, kindergarten, 
    hospital, and shared spaces.
    Finally, it creates additional attributes.

    Args:
        workdir (str): The working directory where the data will be stored and processed.
    """
    if not exists(workdir):
        makedirs(workdir)

    input_cfg = read_cfg(input_cfg)
    # -----------------------------
    # Create population
    # -----------------------------
    create_population_wrapper(workdir, input_cfg)

    # -----------------------------
    # Create household
    # -----------------------------
    create_household_wrapper(workdir, input_cfg)

    # -----------------------------
    # Create geography
    # -----------------------------
    create_geography_wrapper(workdir, input_cfg)

    # -----------------------------
    # Create commute
    # -----------------------------
    create_travel_wrapper(workdir, input_cfg)

    # -----------------------------
    # Create work
    # -----------------------------
    create_work_wrapper(workdir, input_cfg)

    # -----------------------------
    # Create school and kindergarten
    # -----------------------------
    create_school_wrapper(workdir, input_cfg)
    create_kindergarten_wrapper(workdir, input_cfg)

    # -----------------------------
    # Create hospital
    # -----------------------------
    create_hospital_wrapper(workdir, input_cfg)

    # -----------------------------
    # Create shared space
    # -----------------------------
    create_shared_space_wrapper(
        workdir,
        input_cfg,
        space_names=[
            "supermarket",
            "wholesale",
            "department_store",
            "restaurant",
            "bakery",
            # "pharmacy",
            "cafe",
            "fast_food",
            # "museum",
            "pub",
            "park",
        ],
    )
    # -----------------------------
    # Create attributes
    # -----------------------------
    create_others_wrapper(workdir, input_cfg)

    print("Job done ...")


def produce_proj_data(
    workdir: str, input_cfg: dict, all_years: None or list = [2023, 2028, 2033, 2038, 2043]
):
    """
    Produces projected data by copying files and processing population and work data
    for specified target years.

    Args:
        workdir (str): The working directory containing the data files.
        all_years (None or list, optional): A list of target years for projection.
            Defaults to [2023, 2028, 2033, 2038, 2043].

    Returns:
        None
    """
    input_cfg = read_cfg(input_cfg)
    print("Start projection ...")
    project_pop_data(workdir, input_cfg, all_years=all_years)
    project_work_data(workdir, input_cfg, all_years=all_years)
    pop_validation(workdir)
    project_copy_others(workdir, all_years)
    print("Projection done ...")


if __name__ == "__main__":
    parser = ArgumentParser(description="Creating NZ data")

    parser.add_argument(
        "--workdir",
        type=str,
        required=False,
        default="/tmp/syspop_v5.0",
        help="Working directory",
    )
    parser.add_argument(
        "--input",
        type=str,
        required=False,
        default="etc/scripts_nz/input.yml",
        help="Input data configuration",
    )

    parser.add_argument("--add_proj", action=BooleanOptionalAction)

    args = parser.parse_args(
       # ["--workdir", "etc/data/test_data", "--input", "etc/scripts_nz/input_cfg.yml"]
    )  # ["--workdir", "etc/data/test_data_wellington_latest"]
    import_raw_data(args.workdir, args.input)

    if args.add_proj: # it does not work for input_v2.0 yet
        produce_proj_data(args.workdir, args.input)
