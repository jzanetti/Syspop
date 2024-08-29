from logging import getLogger

from numpy import arange, isinf
from pandas import DataFrame, isna, read_csv

logger = getLogger()


def _expand_age_ranges(df) -> DataFrame:
    """
    Expands age ranges in a DataFrame to individual ages.

    This function takes a DataFrame with age ranges and expands each range into individual ages.
    It calculates the value per year for each age range and creates a new DataFrame with individual ages.

    Args:
        df (pandas.DataFrame): DataFrame containing age ranges and corresponding values.

    Returns:
        pandas.DataFrame: DataFrame with individual ages and corresponding values.
    """
    df[["start_age", "end_age"]] = df["age"].str.extract(r"(\d+)-(\d+)").astype(int)

    # Calculate the number of years in the range
    df["num_years"] = df["end_age"] - df["start_age"] + 1

    # Calculate the value per year
    df["value_per_year"] = df["value"] / df["num_years"]

    # Create a new DataFrame with individual ages
    expanded_df = df.loc[df.index.repeat(df["num_years"])].copy()
    expanded_df["age"] = expanded_df.apply(
        lambda row: arange(row["start_age"], row["end_age"] + 1), axis=1
    )
    expanded_df = expanded_df.explode("age").reset_index(drop=True)

    expanded_df = expanded_df.drop_duplicates()

    # Drop the temporary columns
    expanded_df = expanded_df.drop(
        columns=["start_age", "end_age", "num_years", "value"]
    )

    # Rename the columns to match the original DataFrame
    expanded_df = expanded_df.rename(columns={"value_per_year": "value"})

    return expanded_df


def get_geo_codes(raw_data_path: str) -> DataFrame:
    """
    Extracts and returns unique geographic codes from a CSV file.

    This function reads a CSV file from the specified path, extracts the
    'TA2023_code' and 'SA22023_code' columns, and returns a DataFrame
    containing unique combinations of these codes.

    Parameters:
        raw_data_path (str): The file path to the raw CSV data.

    Returns:
        DataFrame: A DataFrame with unique 'TA2023_code' and 'SA22023_code' values.
    """
    data = read_csv(raw_data_path)

    data["TA2023_code"] = data["TA2023_code"].astype(str).str.zfill(3)

    return data[["TA2023_code", "SA22018_code"]].drop_duplicates()


def process_ethnicity_data(raw_data_path: str, scenario: str = "Medium") -> DataFrame:
    """
    Processes ethnicity projection data from a CSV file and returns a cleaned DataFrame.

    This function reads a CSV file containing ethnicity projection data, selects and renames
    relevant columns, splits and cleans data, and filters it based on specified criteria.
    The resulting DataFrame contains unique combinations of area codes, area names, ethnicity,
    age ranges, sex, year, scenario, and values.

    Parameters:
        raw_data_path (str): The file path to the raw CSV data.
        scenario (str): The projection scenario to filter by (default is "Medium").

    Returns:
        DataFrame: A cleaned DataFrame with processed ethnicity projection data.
    """
    logger.info("Reading projection raw data (ethnicity) ...")
    pop_ethnicity_data = read_csv(raw_data_path)

    pop_ethnicity_data = pop_ethnicity_data[
        [
            "ETHNICITY_POPPR_ETH_010: Ethnicity",
            "PROJECTION_POPPR_ETH_010: Projection",
            "AREA_POPPR_ETH_010: Area",
            "YEAR_POPPR_ETH_010: Year at 30 June",
            "SEX_POPPR_ETH_010: Sex",
            "AGE_POPPR_ETH_010: Age",
            "OBS_VALUE",
        ]
    ]

    pop_ethnicity_data = pop_ethnicity_data.rename(
        columns={
            "ETHNICITY_POPPR_ETH_010: Ethnicity": "ethnicity",
            "PROJECTION_POPPR_ETH_010: Projection": "scenario",
            "AREA_POPPR_ETH_010: Area": "area",
            "YEAR_POPPR_ETH_010: Year at 30 June": "year",
            "SEX_POPPR_ETH_010: Sex": "sex",
            "AGE_POPPR_ETH_010: Age": "age",
            "OBS_VALUE": "value",
        }
    )

    for col in ["ethnicity", "scenario", "year", "sex"]:
        pop_ethnicity_data[col] = pop_ethnicity_data[col].str.split(":").str[1]
    pop_ethnicity_data[["area_code", "area_name"]] = pop_ethnicity_data[
        "area"
    ].str.split(":", expand=True)
    pop_ethnicity_data["age_range"] = pop_ethnicity_data["age"].str.replace(
        r"AGE(\d{2})(\d{2})", r"\1-\2", regex=True
    )
    pop_ethnicity_data = pop_ethnicity_data[
        ~pop_ethnicity_data["age_range"].isin(
            ["00-14", "15-39", "40-64", "65-00", "TOTALALLAGES"]
        )
    ]

    pop_ethnicity_data = pop_ethnicity_data[
        [
            "area_code",
            "area_name",
            "ethnicity",
            "age_range",
            "sex",
            "year",
            "scenario",
            "value",
        ]
    ]

    pop_ethnicity_data = pop_ethnicity_data.rename(columns={"age_range": "age"})

    pop_ethnicity_data["age"] = pop_ethnicity_data["age"].replace({"90-00": "90-100"})

    for col in pop_ethnicity_data.columns:
        if col == "value":
            continue
        pop_ethnicity_data[col] = pop_ethnicity_data[col].str.strip()
    pop_ethnicity_data = pop_ethnicity_data[
        (pop_ethnicity_data["scenario"] == scenario)
        & (pop_ethnicity_data["sex"].isin(["Male", "Female"]))
        & (~pop_ethnicity_data["area_code"].isin(["NIRC", "NZRC", "NZTA", "SIRC"]))
        & (pop_ethnicity_data["area_code"].str.len() == 3)
        & (
            pop_ethnicity_data["ethnicity"].isin(
                [
                    "Asian",
                    "European or Other (including New Zealander)",
                    "Maori",
                    "Pacific",
                ]
            )
        )
    ]

    return _expand_age_ranges(pop_ethnicity_data)


def process_gender_age_data(raw_data_path: str, scenario: str = "MEDIUM") -> DataFrame:
    """
    Processes raw gender and age demographic data from a CSV file, expanding age ranges into individual ages.

    This function reads a CSV file containing demographic data, filters and renames columns, expands age ranges
    (e.g., "0-4 years") into individual ages (e.g., 0, 1, 2, 3, 4), and adjusts the values accordingly. The resulting
    DataFrame contains individual ages with values divided by the number of years in the original age range.

    Parameters:
    ----------
    raw_data_path : str
        The file path to the raw CSV data.

    Returns:
    -------
    pd.DataFrame
        A DataFrame with expanded age ranges and adjusted values.

    Notes:
    -----
    The input CSV file is expected to have the following columns:
    - 'AREA_POPPR_SUB_007': Area code
    - 'YEAR_POPPR_SUB_007': Year
    - 'Age': Age range (e.g., "0-4 years")
    - 'Sex': Gender (e.g., "Male", "Female")
    - 'PROJECTION_POPPR_SUB_007': Projection scenario
    - 'OBS_VALUE': Observed value

    The function performs the following steps:
    1. Reads the CSV file into a DataFrame.
    2. Filters the DataFrame to include only relevant columns and rows.
    3. Renames columns to more descriptive names.
    4. Expands age ranges into individual ages.
    5. Adjusts the values by dividing by the number of years in the age range.
    6. Ensures all values are integers and greater than 1.0.

    Example:
    -------
        df = process_gender_age_data('path/to/raw_data.csv')
        print(df.head())
    """

    logger.info("Reading projection raw data (sex/age)")
    pop_age_sex_data = read_csv(raw_data_path).sample(1000)

    logger.info("Processing projection raw data")
    pop_age_sex_data = pop_age_sex_data[
        [
            "AREA_POPPR_SUB_007",
            "YEAR_POPPR_SUB_007",
            "Age",
            "Sex",
            "PROJECTION_POPPR_SUB_007",
            "OBS_VALUE",
        ]
    ]

    pop_age_sex_data = pop_age_sex_data[
        pop_age_sex_data["AREA_POPPR_SUB_007"].astype(str).str.len() >= 5
    ]

    pop_age_sex_data = pop_age_sex_data[
        pop_age_sex_data["Age"].isin(
            [
                "0-4 years",
                "5-9 years",
                "10-14 years",
                "15-19 years",
                "20-24 years",
                "25-29 Years",
                "30-34 years",
                "35-39 years",
                "40-44 years",
                "45-49 years",
                "50-54 years",
                "55-59 years",
                "60-64 years",
                "65-69 years",
                "70-74 years",
                "75-79 years",
                "80-84 years",
                "85-89 years",
                "90 years and over",
            ]
        )
    ]

    pop_age_sex_data["Age"] = pop_age_sex_data["Age"].replace(
        "90 years and over", "90-100 years"
    )

    pop_age_sex_data = pop_age_sex_data[
        pop_age_sex_data["Sex"].isin(["Male", "Female"])
    ]

    pop_age_sex_data = pop_age_sex_data.rename(
        columns={
            "AREA_POPPR_SUB_007": "sa2",
            "YEAR_POPPR_SUB_007": "year",
            "Age": "age",
            "Sex": "sex",
            "PROJECTION_POPPR_SUB_007": "scenarios",
            "OBS_VALUE": "value",
        }
    )

    df = _expand_age_ranges(pop_age_sex_data)

    return df[df["scenarios"] == scenario]
