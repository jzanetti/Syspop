from copy import deepcopy
from os import makedirs
from os.path import exists, join
from pickle import dump as pickle_dump
from pickle import load as pickle_load

from funcs import RAW_DATA
from funcs.proj.utils import (
    get_geo_codes,
    process_ethnicity_data,
    process_gender_age_data,
)
from pandas import DataFrame
from pandas import merge as pandas_merge


def obtain_ref_scaler(
    proj_data: DataFrame, raw_pop_data: DataFrame, reference_year: int = 2018
) -> DataFrame:
    """
    Calculate scaling factors for population projections.

    This function takes in two DataFrames: one with projected population data and one with raw population data.
    It calculates scaling factors for each age group by dividing the raw population values by the projected values.
    The resulting DataFrame contains the scaling factors for each area and age group.

    Parameters:
        proj_data_ref (DataFrame): DataFrame containing projected
            population data with columns ['area', 'year', 'age', 'scenarios', 'value'].
        raw_pop_data (DataFrame): DataFrame containing
            raw population data with columns ['area', 'age', 'value'].

    Returns:
        DataFrame: A DataFrame with columns ['area'] and scaling factors for each age group,
            where the column names are the age groups.
    """
    proj_data_ref = proj_data[proj_data["year"] == reference_year]
    proj_data_ref_age = deepcopy(
        proj_data_ref.groupby(["area", "year", "age", "scenarios"], as_index=False)[
            "value"
        ].sum()
    )
    proj_data_ref_age = proj_data_ref_age[["area", "age", "value"]]
    proj_data_ref_age = proj_data_ref_age.pivot_table(
        index="area", columns="age", values="value"
    ).reset_index()
    raw_pop_data_age = raw_pop_data["age"]

    scaler_ref = pandas_merge(
        raw_pop_data_age, proj_data_ref_age, on="area", suffixes=("_raw", "_proj")
    )

    scaler_columns = []
    for col in range(100):
        try:
            scaler_ref[str(col) + "_scaler"] = (
                scaler_ref[str(col) + "_raw"] / scaler_ref[str(col) + "_proj"]
            )
            scaler_columns.append(str(col) + "_scaler")
        except KeyError:
            print(f"Missing the age {col} ...")
    scaler_ref = scaler_ref[["area"] + scaler_columns]
    scaler_ref.columns = [
        col.replace("_scaler", "") if col != "area" else col
        for col in scaler_ref.columns
    ]

    return scaler_ref


def project_pop_data(
    workdir: str,
    all_years: None or list = [2023, 2028, 2033, 2038, 2043],
):
    """
    Processes and projects population data by age, gender, and ethnicity.

    This function performs the following steps:
    1. Retrieves geographical codes.
    2. Processes population data by ethnicity.
    3. Processes population data by age and gender.
    4. Loads raw population data from a pickle file.
    5. Iterates through each year of projection data to:
        a. Create directories for each year's projection data.
        b. Process and aggregate population data by age.
        c. Calculate gender percentages and adjust population data accordingly.
        d. Calculate ethnicity percentages and adjust population data accordingly.

    Args:
        workdir (str): The working directory where population data is stored and processed.

    Returns:
        None
    """
    geocode = get_geo_codes(RAW_DATA["geography"]["geography_hierarchy"])

    proj_data_ethnicity = process_ethnicity_data(
        RAW_DATA["projection"]["population"]["population_by_ethnicity"]
    )

    proj_data_age_and_gender = process_gender_age_data(
        RAW_DATA["projection"]["population"]["population_by_age_by_gender"]
    )
    proj_data_age_and_gender = proj_data_age_and_gender.rename(columns={"sa2": "area"})

    if all_years is None:
        all_years = list(proj_data_age_and_gender.year.unique())

    for proc_year in all_years:

        print(f"Processing population projection: Year {proc_year}")

        proj_dir = join(workdir, "proj", str(proc_year))

        if not exists(proj_dir):
            makedirs(proj_dir)

        proc_proj_data = proj_data_age_and_gender[
            proj_data_age_and_gender["year"] == proc_year
        ]
        proc_proj_data_ethnicity = proj_data_ethnicity[
            proj_data_ethnicity["year"] == str(proc_year)
        ][["area_code", "ethnicity", "age", "sex", "value"]]

        # ---------------------
        # Processing age
        # ---------------------
        proc_proj_data_age = deepcopy(
            proc_proj_data.groupby(["area", "year", "age"], as_index=False)[
                "value"
            ].sum()
        )

        proc_proj_data_age = proc_proj_data_age[["area", "age", "value"]]
        proc_proj_data_age = proc_proj_data_age.pivot_table(
            index="area", columns="age", values="value"
        ).reset_index()

        # ---------------------
        # Processing gender percentage
        # ---------------------
        proc_proj_data_gender = deepcopy(
            proc_proj_data.groupby(["area", "age", "sex"], as_index=False)[
                "value"
            ].sum()
        )

        proc_proj_data_gender_total = (
            proc_proj_data_gender.groupby(["area", "age"], as_index=False)["value"]
            .sum()
            .rename(columns={"value": "total_value"})
        )

        proc_proj_data_gender = pandas_merge(
            proc_proj_data_gender,
            proc_proj_data_gender_total,
            on=["area", "age"],
        )

        proc_proj_data_gender["percentage"] = (
            proc_proj_data_gender["value"] / proc_proj_data_gender["total_value"]
        ) * 100

        # Drop the 'total_value' column if not needed
        proc_proj_data_gender = proc_proj_data_gender[
            ["area", "age", "sex", "percentage"]
        ]

        proc_proj_data_age_melted = proc_proj_data_age.melt(
            id_vars=["area"], var_name="age", value_name="population"
        )
        proc_proj_data_gender = pandas_merge(
            proc_proj_data_age_melted,
            proc_proj_data_gender,
            on=["area", "age"],
            how="left",
        )
        proc_proj_data_gender["population"] = proc_proj_data_gender["population"] * (
            proc_proj_data_gender["percentage"] / 100
        )

        proc_proj_data_gender = proc_proj_data_gender.drop(columns=["percentage"])

        # Pivot the dataframe back to wide-form
        proc_proj_data_gender = proc_proj_data_gender.pivot_table(
            index=["area", "sex"], columns="age", values="population"
        ).reset_index()

        # ---------------------
        # Processing ethnicity percentage
        # ---------------------
        proc_proj_data_ethnicity = proc_proj_data_ethnicity.groupby(
            ["area_code", "ethnicity", "age"], as_index=False
        )["value"].sum()
        total_population = proc_proj_data_ethnicity.groupby(["area_code", "age"])
        proc_proj_data_ethnicity["total_population"] = total_population[
            "value"
        ].transform("sum")
        proc_proj_data_ethnicity["percentage"] = (
            proc_proj_data_ethnicity["value"]
            / proc_proj_data_ethnicity["total_population"]
        )
        proc_proj_data_ethnicity = proc_proj_data_ethnicity.drop(
            columns=["total_population"]
        )
        proc_proj_data_ethnicity = proc_proj_data_ethnicity.merge(
            geocode, left_on="area_code", right_on="TA2023_code", how="left"
        )[["SA22018_code", "age", "ethnicity", "percentage"]]

        proc_proj_data_ethnicity = proc_proj_data_ethnicity.rename(
            columns={"SA22018_code": "area"}
        )
        proc_proj_data_age_melted = proc_proj_data_age.melt(
            id_vars=["area"], var_name="age", value_name="number_of_people"
        )
        proc_proj_data_age_melted["age"] = proc_proj_data_age_melted["age"].astype(int)
        df_merged = pandas_merge(
            proc_proj_data_age_melted, proc_proj_data_ethnicity, on=["area", "age"]
        )
        df_merged["number_of_people"] = (
            df_merged["number_of_people"] * df_merged["percentage"]
        )
        proc_proj_data_ethnicity = df_merged.pivot_table(
            index=["area", "ethnicity"], columns="age", values="number_of_people"
        ).reset_index()

        proc_proj_data_ethnicity.columns.name = None
        proc_proj_data_ethnicity["ethnicity"].replace(
            "European or Other (including New Zealander)",
            "European and others",
            inplace=True,
        )

        pickle_dump(
            {
                "age": proc_proj_data_age,
                "gender": proc_proj_data_gender,
                "ethnicity": proc_proj_data_ethnicity,
            },
            open(join(proj_dir, "population.pickle"), "wb"),
        )
