# export PYTHONPATH=~/Github/Syspop/etc/scripts_nz
from argparse import ArgumentParser
from os import makedirs
from os.path import exists

from funcs.wrapper import (
    create_geography_wrapper,
    create_hospital_wrapper,
    create_household_number,
    create_population_wrapper,
    create_school_wrapper,
    create_shared_space_wrapper,
    create_travel_wrapper,
    create_work_wrapper,
)


def import_raw_data(workdir: str):
    if not exists(workdir):
        makedirs(workdir)

    # -----------------------------
    # Create geography
    # -----------------------------
    create_geography_wrapper(workdir)

    # -----------------------------
    # Create household
    # -----------------------------
    create_household_number(workdir)

    # -----------------------------
    # Create population
    # -----------------------------
    create_population_wrapper(workdir)

    # -----------------------------
    # Create commute
    # -----------------------------
    create_travel_wrapper(workdir)

    # -----------------------------
    # Create work
    # -----------------------------
    create_work_wrapper(workdir)

    # -----------------------------
    # Create school
    # -----------------------------
    create_school_wrapper(workdir)

    # -----------------------------
    # Create hospital
    # -----------------------------
    create_hospital_wrapper(workdir)

    # -----------------------------
    # Create shared space
    # -----------------------------
    create_shared_space_wrapper(
        workdir,
        space_names=[
            "supermarket",
            "wholesale",
            "department_store",
            "restaurant",
            "kindergarten",
            # "pharmacy",
            "cafe",
            "fast_food",
            # "museum",
            "childcare",
            "pub",
            "park",
        ],
    )
    print("Job done ...")


if __name__ == "__main__":
    parser = ArgumentParser(description="Creating NZ data")

    parser.add_argument(
        "--workdir",
        type=str,
        required=False,
        default="/tmp/syspop_v5.0",
        help="Working directory",
    )

    args = parser.parse_args(
        # [
        #    "--workdir",
        #    "/tmp/syspop_llm/run_20240323T21/"
        # ]
    )
    import_raw_data(args.workdir)
