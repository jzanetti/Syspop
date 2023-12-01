from copy import copy, deepcopy
from math import ceil as math_ceil
from os.path import join
from re import findall as re_findall
from re import match as re_match

from numpy import inf, nan
from pandas import DataFrame, melt, merge, pivot_table, read_csv, read_excel, to_numeric

from funcs import RAW_DATA
from funcs.utils import read_anzsic_code, read_leed


def create_population_travel_to_work_by_method():
    """Write Transport Model file

    Args:
        workdir (str): Working directory
        transport_mode_cfg (dict): Transport model configuration
    """

    data = read_csv(RAW_DATA["transport"]["population_travel_to_work_by_method"])

    data = data[
        [
            "SA2_code_usual_residence_address",
            # "SA2_code_workplace_address",
            "Drive_a_private_car_truck_or_van",
            "Drive_a_company_car_truck_or_van",
            "Passenger_in_a_car_truck_van_or_company_bus",
            "Public_bus",
            "Train",
            "Bicycle",
            "Ferry",
            "Other",
            "Total",
        ]
    ]

    data = data.replace(-999.0, 0)

    data = data.groupby("SA2_code_usual_residence_address").sum().reset_index()

    data = data.rename(columns={"SA2_code_usual_residence_address": "geography"})

    for data_key in data.columns:
        if data_key != "geography":
            data = data.rename(
                columns={
                    data_key: f"Method of Travel to Work: {data_key}; measures: Value"
                }
            )

    data["date"] = 2018
    data["geography code"] = data["geography"]
    data["Rural Urban"] = "Total"

    # Remove rows that there is no travel methods
    travel_methods = [
        x
        for x in list(data.columns)
        if x
        not in [
            "geography",
            "date",
            "geography code",
            "Rural Urban",
            "Method of Travel to Work: Total; measures: Value",
        ]
    ]
    data = data.loc[~((data[travel_methods] == 0).all(axis=1))]

    # Convert all columns from string to integer, except for the Rural Urban
    for column in data.columns:
        if column != "Rural Urban":
            data[column] = data[column].astype(int)

    # data = data.drop(columns=["date", "geography code", "Rural Urban"])

    return data


def write_workplace_and_home_locations(
    geography_hierarchy_definition: DataFrame,
):
    """Write workplace and home commute file

    Args:
        workdir (str): Working directory
        workplace_and_home_cfg (dict): Workplace and home commute configuration
        geography_hierarchy_definition (DataFrame): Geography hierarchy data
        use_sa3_as_super_area (bool): If use SA3 as super area, otherwise use regions
    """

    def _get_required_gender_data() -> DataFrame:
        """Get requred gender data

        Args:
            gender_data_path (str): Gender data path

        Returns:
            DataFrame: Dataframe to be exported
        """
        gender_profile = read_csv(
            RAW_DATA["transport"]["workplace_and_home_locations"][
                "population_by_gender"
            ]
        )

        gender_profile = gender_profile[
            ~gender_profile["Area"].str.endswith(("region", "regions", "region/SA2"))
        ]
        gender_profile["Area"] = gender_profile["Area"].apply(
            lambda x: re_findall("\d+", str(x))[0]
            if re_findall("\d+", str(x))
            else None
        )

        gender_profile["Percentage_Male"] = gender_profile["Male"] / (
            gender_profile["Male"] + gender_profile["Female"]
        )

        gender_profile["Percentage_Female"] = gender_profile["Female"] / (
            gender_profile["Male"] + gender_profile["Female"]
        )

        gender_profile = gender_profile[
            ["Area", "Percentage_Male", "Percentage_Female"]
        ]

        return gender_profile

    commute_data = read_csv(
        RAW_DATA["transport"]["workplace_and_home_locations"]["travel-to-work-info"]
    )[["SA2_code_usual_residence_address", "SA2_code_workplace_address", "Total"]]

    gender_profile = _get_required_gender_data()
    commute_data = commute_data.rename(
        columns={"SA2_code_usual_residence_address": "Area"}
    )

    gender_profile["Area"] = gender_profile["Area"].astype(str)
    commute_data["Area"] = commute_data["Area"].astype(str)

    merged_df = merge(gender_profile, commute_data, on="Area")

    # Perform the calculations and create new columns
    merged_df["Male"] = merged_df["Percentage_Male"] * merged_df["Total"]
    merged_df["Female"] = merged_df["Percentage_Female"] * merged_df["Total"]

    merged_df["Male"] = merged_df["Male"].astype(int)
    merged_df["Female"] = merged_df["Female"].astype(int)

    # "Area of residence","Area of workplace","All categories: Sex","Male","Female"
    merged_df = merged_df[
        ["Area", "SA2_code_workplace_address", "Total", "Male", "Female"]
    ]

    merged_df = merged_df.rename(
        {
            "Area": "Area of residence",
            "SA2_code_workplace_address": "Area of workplace",
            "Total": "All categories: Sex",
            "Male": "Male",
            "Female": "Female",
        }
    ).astype(int)

    mapping_dict = dict(
        zip(
            geography_hierarchy_definition["area"],
            geography_hierarchy_definition["super_area"],
        )
    )

    merged_df["Area"] = merged_df["Area"].map(mapping_dict)
    merged_df["SA2_code_workplace_address"] = merged_df[
        "SA2_code_workplace_address"
    ].map(mapping_dict)
    merged_df = (
        merged_df.groupby(["Area", "SA2_code_workplace_address"]).sum().reset_index()
    )

    merged_df = merged_df.rename(
        columns={
            "Area": "Area of residence",
            "SA2_code_workplace_address": "Area of workplace",
            "Total": "All categories: Sex",
            "Male": "Male",
            "Female": "Female",
        }
    )

    merged_df["Area of residence"] = merged_df["Area of residence"].astype(int)
    merged_df["Area of workplace"] = merged_df["Area of workplace"].astype(int)

    return merged_df
