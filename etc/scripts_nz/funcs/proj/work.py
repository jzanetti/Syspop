from copy import deepcopy
from os import makedirs
from os.path import exists, join
from pickle import dump as pickle_dump
from pickle import load as pickle_load

from funcs import RAW_DATA
from pandas import read_csv


def project_work_data(
    workdir,
    scenario="median",
    all_years: None or list = [2023, 2028, 2033, 2038, 2043],
):
    """Projects work data based on a given scenario and reference year.

    Parameters:
        workdir (str): The working directory where the raw work data and projections are stored.
        scenario (str): The scenario to use for projections. Default is "median".

    Raises:
        Exception: If the specified scenario is not implemented.

    This function performs the following steps:
    1. Loads raw work data from a pickle file.
    2. Reads projection data from a CSV file and renames columns for clarity.
    3. Filters the projection data to include only total ages and total sex.
    4. Selects the median scenario data if the scenario is "median".
    5. Groups the projection data by year and calculates a scaler based on the reference year.
    6. Iterates over all projection years, scales the work data, and saves the projected work data to new pickle files.

    Example: project_work_data("/path/to/workdir", scenario="median", proj_ref_year=2020)
    """
    raw_work_data = pickle_load(open(join(workdir, "work.pickle"), "rb"))
    proj_data = read_csv(
        join(RAW_DATA["projection"]["business"]["labours"]),
        usecols=[
            "SCENARIO_POPPR_LAB_001: Scenario",
            "YEAR_POPPR_LAB_001: Year at 30 June",
            "AGE_POPPR_LAB_001: Age",
            "SEX_POPPR_LAB_001: Sex",
            "OBS_VALUE",
        ],
    )
    proj_data = proj_data.rename(
        columns={
            "SCENARIO_POPPR_LAB_001: Scenario": "scenario",
            "YEAR_POPPR_LAB_001: Year at 30 June": "year",
            "AGE_POPPR_LAB_001: Age": "age",
            "SEX_POPPR_LAB_001: Sex": "sex",
            "OBS_VALUE": "value",
        }
    )

    proj_data = proj_data[
        (proj_data["age"] == "TOTALALLAGES: Total people, ages")
        & (proj_data["sex"] == "SEX3: Total people, sex")
    ]

    if scenario == "median":
        proj_data = proj_data[proj_data["scenario"] == "P50: 50th percentile (median)"][
            ["year", "value"]
        ]
    else:
        raise Exception("the scenario has not been implemented")

    proj_data = proj_data.groupby("year")["value"].sum().reset_index()

    proj_data["scaler"] = proj_data["value"] / proj_data.iloc[0]["value"]

    if all_years is None:
        all_years = proj_data.year.unique()

    for proc_year in all_years:

        print(f"Processing work projection: Year {proc_year}")

        proj_dir = join(workdir, "proj", str(proc_year))

        proc_scaler = proj_data[proj_data["year"] == proc_year]["scaler"].values[0]

        if not exists(proj_dir):
            makedirs(proj_dir)
        proc_work_data = deepcopy(raw_work_data)

        proc_work_data["employee"]["employee_number"] = (
            proc_work_data["employee"]["employee_number"] * proc_scaler
        ).astype(int)
        proc_work_data["employer"]["employer_number"] = (
            proc_work_data["employer"]["employer_number"] * proc_scaler
        ).astype(int)
        pickle_dump(
            proc_work_data,
            open(join(proj_dir, "work.pickle"), "wb"),
        )
