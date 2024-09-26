
from pandas import DataFrame, concat, read_csv, read_excel, to_numeric
from math import ceil as math_ceil

from geopandas import read_file as gpd_read_file
from geopandas import sjoin as gpd_sjoin
from re import match as re_match
from funcs import REGION_CODES, REGION_NAMES_CONVERSIONS, RAW_DATA_INFO
from funcs.utils import get_central_point
from copy import deepcopy
from numpy import nan as numpy_nan

def _read_raw_household(raw_household_path, include_public_dwelling: bool = False) -> DataFrame:
    """
    Reads and processes raw household data from a CSV file.

    Parameters:
        raw_household_path (str): The file path to the raw household CSV data.
        include_public_dwelling (bool): If True, includes public dwellings in the data. Defaults to False.

    Returns:
        DataFrame: A DataFrame containing the processed household data with columns for area, number of adults, number of children, and count of households.
    
    The output looks sth like:
             area  adults  children  num
    0      100100       0         1    3
    1      100100       0         2    4
    2      100100       1         0  142
    3      100100       1         1   14
    4      100100       1         2   15
    ...       ...     ...       ...  ...
    53773  363300       4         2    3

    The function performs the following steps:
    1. Reads the CSV file from the given path.
    2. Renames columns for better readability.
    3. Filters out public dwellings if include_public_dwelling is False.
    4. Converts the 'adults' and 'people' columns to integers.
    5. Calculates the number of children by subtracting adults from people.
    6. Groups the data by area, number of adults, and number of children, summing the household counts.
    7. Returns the processed DataFrame with selected columns.
    """
    data = read_csv(raw_household_path)

    data = data.rename(
        columns={
            "SA2 Code": "area",
            "Number of people": "people",
            "Number of adults": "adults",
            "Dwelling type": "dwelling_type",
            "Count": "num",
        })


    if not include_public_dwelling:
        data = data[data["dwelling_type"] < 2000]

    data["adults"] = data["adults"].astype(int)
    data["people"] = data["people"].astype(int)
    data["children"] = data["people"].astype(int) - data["adults"].astype(int)

    data = data.groupby(["area", "adults", "children"], as_index=False)['num'].sum()

    return data[["area", "adults", "children", "num"]]
        

def _read_raw_population(raw_population_path) -> DataFrame:
    """
    Reads and processes population data from an Excel file.

    This function performs the following steps:
    1. Reads the population data from an Excel file, starting from the 7th row.
    2. Renames the columns 'Area' to 'area' and 'Unnamed: 2' to 'population'.
    3. Drops the column 'Unnamed: 1'.
    4. Drops the last row of the DataFrame.
    5. Converts the data types of all columns to integers.
    6. Filters the DataFrame to include only rows where the 'area' is greater than 10,000.

    The output looks like:
                area  population
        2     100100        1680
        3     100200        2280
        4     100300          84
        5     100400        1236
        6     100500        1107
              ...         ...

    Returns:
        pandas.DataFrame: The processed population data.
    """

    data = read_excel(raw_population_path, header=6)

    data = data.rename(columns={"Area": "area", "Unnamed: 2": "population"})

    data = data.drop("Unnamed: 1", axis=1)

    # Drop the last row
    data = data.drop(data.index[-1])

    data = data.astype(int)

    data = data[data["area"] > 10000]

    return data


def _read_raw_age(raw_age_path: str) -> DataFrame:
    """
    Processes population data by age and region, normalizes it, and merges it with total population data.

    Args:
        total_population_data (DataFrame): A DataFrame containing total population data by region.

    Returns:
        DataFrame: A DataFrame with normalized population data by age and region.

    The function performs the following steps:
    1. Reads population data by age from an Excel file.
    2. Cleans and preprocesses the data by stripping whitespace, filtering out unwanted regions, and setting the index.
    3. Renames columns to standardize age ranges.
    4. Creates a new DataFrame with columns for each age from 0 to 100.
    5. Distributes population counts evenly across age ranges.
    6. Normalizes the data by applying a ceiling function.
    7. Calculates the total population for each region and merges it with the provided total population data.
    8. Adjusts the population counts based on the ratio of the provided total population to the calculated total.
    9. Cleans up the DataFrame by removing infinite values and rounding the data.
    10. Returns the final DataFrame with normalized population data by age and region.

    The output looks like:

                area   0   1   2   3   4    ...    94  95  96  97  98  99  100
        0     100100  16  16  16  16  16    ...     1   1   1   1   1   1    1
        1     100200  40  40  40  40  40    ...    11  11  11   8   8   8    8 
        ...
    """

    def _find_range(number, ranges):
        for age_range in ranges:
            start, end = map(int, age_range.split("-"))
            if start <= number <= end:
                return age_range
        return None

    df = read_excel(raw_age_path, header=2)

    df.columns = df.columns.str.strip()

    df = df[
        [
            "Region and Age",
            "0-4 Years",
            "5-9 Years",
            "10-14 Years",
            "15-19 Years",
            "20-24 Years",
            "25-29 Years",
            "30-34 Years",
            "35-39 Years",
            "40-44 Years",
            "45-49 Years",
            "50-54 Years",
            "55-59 Years",
            "60-64 Years",
            "65-69 Years",
            "70-74 Years",
            "75-79 Years",
            "80-84 Years",
            "85-89 Years",
            "90 Years and over",
        ]
    ]

    df = df.drop(df.index[-1])

    df["Region and Age"] = df["Region and Age"].str.strip()

    df = df[~df["Region and Age"].isin(["NZRC", "NIRC", "SIRC"])]

    df["Region and Age"] = df["Region and Age"].astype(int)

    df = df[df["Region and Age"] > 10000]

    df = df.set_index("Region and Age")

    df.columns = [str(name).replace(" Years", "") for name in df]
    df = df.rename(columns={"90 and over": "90-100"})

    new_df = DataFrame(columns=["Region"] + list(range(0, 101)))

    for cur_age in list(new_df.columns):
        if cur_age == "Region":
            new_df["Region"] = df.index
        else:
            age_range = _find_range(cur_age, list(df.columns))
            age_split = age_range.split("-")
            start_age = int(age_split[0])
            end_age = int(age_split[1])
            age_length = end_age - start_age + 1
            new_df[cur_age] = (df[age_range] / age_length).values

    new_df = new_df.applymap(math_ceil)

    new_df = new_df.rename(columns={"Region": "area"})

    all_ages = range(101)
    for index, row in new_df.iterrows():
        total = sum(row[col] for col in all_ages)
        new_df.at[index, "total"] = total

    return new_df


def _read_raw_gender(raw_gender_path: str) -> DataFrame:
    """
    Reads and processes raw gender-based population data from an Excel file.

    Args:
        raw_gender_path (str): The file path to the raw gender-based population data.

    Returns:
        DataFrame: A DataFrame containing cleaned and processed gender-based population data by region.

    The function performs the following steps:
    1. Reads the Excel file starting from the fourth row (header=3).
    2. Renames columns to standardize gender and age group labels.
    3. Drops the unnamed second column.
    4. Removes the first three rows and the last three rows of the DataFrame.
    5. Converts the data to integers.
    6. Filters out rows where the 'area' value is less than or equal to 10,000.

    the output is sth like:
            area  Male (15)  Female (15)  Male (40)  Female (40)  Male (65)  Female (65)  Male (90)  Female (90)
    4     100100        180          150        200          200        290          310        170          170
    5     100200        310          290        350          310        370          410        200          180
    6     100300          0            0          0            0         10           10         10            0
    7     100400        110          130        120          120        250          250        180          150
    8     100500        140          130        140          170        220          200        100           90
    .....

    Returns:
        DataFrame: The cleaned and processed gender-based population data.

    """
    df = read_excel(raw_gender_path, header=3)

    df = df.rename(
        columns={
            "Male": "Male (15)",
            "Female": "Female (15)",
            "Male.1": "Male (40)",
            "Female.1": "Female (40)",
            "Male.2": "Male (65)",
            "Female.2": "Female (65)",
            "Male.3": "Male (90)",
            "Female.3": "Female (90)",
            "Sex": "area",
        }
    )

    df = df.drop("Unnamed: 1", axis=1)

    df = df.drop([0, 1, 2]).drop(df.tail(3).index).astype(int)

    df = df[df["area"] > 10000]

    return df


def _read_raw_ethnicity(raw_ethnicity_path: str) -> DataFrame:
    """
    Reads and processes raw ethnicity-based population data from multiple Excel files.

    Args:
        raw_ethnicity_path (str): A dictionary where keys are age group identifiers and values are file paths to the raw ethnicity-based population data.

    Returns:
        DataFrame: A combined DataFrame containing cleaned and processed ethnicity-based population data by region and age group.

    The function performs the following steps:
    1. Iterates over the provided file paths, reading each Excel file starting from the fifth row (header=4).
    2. Drops the first two rows and the last three rows of each DataFrame.
    3. Removes the unnamed second column and strips whitespace from column names.
    4. Renames columns to standardize ethnicity group labels.
    5. Converts all data to numeric, dropping non-numeric rows, and converts the remaining data to integers.
    6. Calculates the total population for each region by summing the counts of different ethnic groups.
    7. Stores each processed DataFrame in a dictionary with the corresponding age group identifier.
    8. Combines all processed DataFrames into a single DataFrame, adding an 'age' column to indicate the age group.

    The output looks like:
                area  European  Maori  Pacific  Asian  MELAA  total  age
        0     100100       171    267       30      9      3    480    0
        1     100200       387    453       48     18      0    906    0
        2     100300         3      0        0      0      0      3    0
        3     100400       126    174       24      6      0    330    0
        4     100500       162    186       21      0      3    372    0
        ......

    Returns:
        DataFrame: The final combined DataFrame with ethnicity-based population data by region and age group.
    """
    dfs = {}

    for proc_age_key in raw_ethnicity_path:
        df = read_excel(
            raw_ethnicity_path[proc_age_key],
            header=4,
        )
        df = df.drop([0, 1]).drop(df.tail(3).index)
        df = df.drop("Unnamed: 1", axis=1)
        df.columns = df.columns.str.strip()

        df = df.rename(
            columns={
                "Ethnic group": "area",
                "Pacific Peoples": "Pacific",
                "Middle Eastern/Latin American/African": "MELAA",
            }
        )

        df = (
            df.apply(to_numeric, errors="coerce").dropna().astype(int)
        )  # convert str ot others to NaN, and drop them and convert the rests to int

        df["total"] = (
            df["European"] + df["Maori"] + df["Pacific"] + df["Asian"] + df["MELAA"]
        )

        dfs[proc_age_key] = df

    return concat(
        [df.assign(age=key) for key, df in dfs.items()],
        ignore_index=True
    )


def _read_raw_address(raw_sa2_area_path: str, raw_address_path: str):
    """
    Reads and processes raw spatial data files for SA2 areas and addresses, 
    transforming them into a combined DataFrame with latitude and longitude coordinates.

    Parameters:
        raw_sa2_area_path (str): The file path to the raw SA2 area data.
        raw_address_path (str): The file path to the raw address data.

    The output looks like:
                                geometry  index_right    area   longitude   latitude
    0        POINT (174.75596 -36.86515)          831  136000  174.755962 -36.865148
    1        POINT (174.75620 -36.86539)          831  136000  174.756203 -36.865394
    2        POINT (170.50644 -45.89642)          444  354700  170.506439 -45.896419
    3        POINT (172.68243 -43.56910)         2018  331400  172.682428 -43.569101
    4        POINT (172.68265 -43.56881)         2018  331400  172.682646 -43.568808
    ...                              ...          ...     ...         ...        ...

    Returns:
        pandas.DataFrame: A DataFrame containing the SA2 area, latitude, and longitude 
                        for each address within the SA2 areas.
    """
    sa2_data = gpd_read_file(raw_sa2_area_path)
    address_data = gpd_read_file(raw_address_path)

    gdf_sa2 = sa2_data.to_crs(epsg=4326)
    gdf_address = address_data.to_crs(epsg=4326)
    gdf_sa2 = gdf_sa2[["SA22022_V1", "geometry"]]
    gdf_address = gdf_address[["geometry"]]

    combined_df = gpd_sjoin(gdf_address, gdf_sa2, how="inner", op="within")
    combined_df["lon"] = combined_df.geometry.x
    combined_df["lat"] = combined_df.geometry.y

    combined_df = combined_df.rename(
        columns={"SA22022_V1": "area", "lat": "latitude", "lon": "longitude"}
    )

    combined_df["area"] = combined_df["area"].astype(int)

    return combined_df[["area", "latitude", "longitude"]]


def _read_raw_geography_hierarchy(raw_geography_hierarchy_path: str):
    """Create geography

    The output looks like:
        region super_area    area
    0   aa        50010  100100
    3   bb        50010  100200
    6   cc        50010  100600
    8   dd        50030  100400
    10  ee        50030  101000
    ...           ...     ...

    Args:
        raw_geography_hierarchy_path (str): geograph hierarchy data path
    """

    def _map_codes2(code: str) -> list:
        """Create a mapping function

        Args:
            code (str): Regional code to be mapped

        Returns:
            list: The list contains north and south island
        """
        for key, values in REGION_NAMES_CONVERSIONS.items():
            if code == key:
                return values
        return None

    data = read_csv(raw_geography_hierarchy_path)

    data = data[["REGC2023_code", "SA32023_code", "SA32023_name", "SA22018_code"]]

    data = data[~data["REGC2023_code"].isin(REGION_CODES["Others"])]

    data["REGC2023_name"] = data["REGC2023_code"].map(_map_codes2)

    data = data.rename(
        columns={
            "REGC2023_name": "region",
            "SA32023_code": "super_area",
            "SA22018_code": "area",
            "SA32023_name": "super_area_name",
        }
    ).drop_duplicates()

    data = data[["region", "super_area", "area", "super_area_name"]]

    data = data[~data["area"].duplicated(keep=False)]

    return data[["region", "super_area", "area"]]


def _read_raw_geography_location_area(raw_geography_location_path: str):
    """
    Reads and processes raw geography location data from a CSV file.

    Parameters:
        raw_geography_location_path (str): The file path to the raw geography location CSV data.

    Returns:
        DataFrame: A DataFrame containing the processed geography location data with columns for area, latitude, and longitude.
    
    The output looks like:
            area   latitude   longitude
    0     100100 -34.505453  172.775550
    1     100200 -34.916277  173.137443
    2     100300 -35.218501  174.158249
    3     100400 -34.995278  173.378738
    4     100500 -35.123147  173.218604
    ...

    The function performs the following steps:
    1. Reads the CSV file from the given path.
    2. Selects the relevant columns: 'SA22018_V1_00', 'LATITUDE', and 'LONGITUDE'.
    3. Renames the selected columns for better readability.
    4. Returns the processed DataFrame.
    """
    data = read_csv(raw_geography_location_path)

    data = data[["SA22018_V1_00", "LATITUDE", "LONGITUDE"]]

    data = data.rename(
        columns={
            "SA22018_V1_00": "area",
            "LATITUDE": "latitude",
            "LONGITUDE": "longitude",
        }
    )

    return data


def _read_raw_nzdep(raw_nzdep_path: str) -> DataFrame:
    """Read raw NZ deprivaion data

    The output looks like:
                area  deprivation
        0     100100         10.0
        1     100200          9.0
        2     100300          7.0
        3     100400          8.0
        4     100500          9.0
        ...      ...          ...

    Args:
        raw_nzdep_path (str): _description_

    Returns:
        DataFrame: NZ dep ddata
    """
    data = read_csv(raw_nzdep_path)[
        ["SA22018_code", "SA2_average_NZDep2018"]
    ]

    data = data.rename(
        columns={
            "SA22018_code": "area",
            "SA2_average_NZDep2018": "deprivation",
        }
    )

    return data.dropna()


def _read_raw_travel_to_work(raw_travel_to_work_path: str) -> DataFrame:
    """Write Transport Model file

    The output looks like:
        area_home  area_work  Work_at_home  ....  Train  Bicycle  Walk_or_jog  Ferry  Other
    0         100100     100100          ...       0        0           21      0      6
    1         100200     100200          ...       0        0           12      0      0
    2         100400     100400          ...       0        0            0      0      5
    ...      

    Args:
        workdir (str): Working directory
        transport_mode_cfg (dict): Transport model configuration
    """

    data = read_csv(raw_travel_to_work_path)

    data = data[
        [
            "SA2_code_usual_residence_address",
            "SA2_code_workplace_address",
            "Work_at_home",
            "Drive_a_private_car_truck_or_van",
            "Drive_a_company_car_truck_or_van",
            "Passenger_in_a_car_truck_van_or_company_bus",
            "Public_bus",
            "Train",
            "Bicycle",
            "Walk_or_jog",
            "Ferry",
            "Other",
        ]
    ]

    data = data.replace(-999.0, 0)
    data.rename(
        columns={
            "SA2_code_usual_residence_address": "area_home",
            "SA2_code_workplace_address": "area_work",
        },
        inplace=True,
    )

    return data


def _read_raw_employer_employee_data(employer_employee_num_path: str) -> DataFrame:
    """Write the number of employees by gender for different area

    The output looks like:
            area business_code  employee_number  employer_number
    0       100100             A              190               93
    1       100200             A              190              138
    2       100300             A               25                6
    3       100400             A               50               57
    4       100500             A               95               57
    ...        ...           ...              ...              ...

    Args:
        workdir (str): Working directory
        employees_cfg (dict): Configuration
        use_sa3_as_super_area (bool): If apply SA3 as super area, otherwise using regions
    """

    data = read_csv(
        employer_employee_num_path)[["anzsic06", "Area", "ec_count", "geo_count"]]

    data = data[data["Area"].str.startswith("A")]
    data = data[data["anzsic06"].apply(lambda x: len(x) == 1)]

    data = data.rename(columns={"Area": "area"})

    data["area"] = data["area"].str[1:].astype(int)

    data = data.rename(columns={
        "anzsic06": "business_code", 
        "ec_count": "employee_number", 
        "geo_count": "employer_number"})

    return data[[
        "area",
        "business_code", 
        "employer_number", 
        "employee_number"]
    ]


def _read_raw_schools(school_data_path: str) -> DataFrame:

    """
    Reads and processes raw New Zealand school data from a CSV file.

    This function reads a CSV file containing school data, filters the data to include only schools,
    excludes certain types of schools, maps the 'use_type' to a more detailed classification,
    and extracts relevant information such as sector, age range, and geographical coordinates.

    Args:
        school_data_path (str): The file path to the CSV file containing the raw school data.

    The output is sth like:
            estimated_occupancy age_min age_max   latitude   longitude             sector
        0                     0.0      14      19 -36.851138  174.760643          secondary
        1                   906.0       5      19 -36.841742  175.696738  primary_secondary
        3                    24.0       5      13 -46.207408  168.541883            primary
        8                   247.0       5      19 -34.994245  173.463766  primary_secondary
        9                  1440.0      14      19 -35.713358  174.318881          secondary
        ...                   ...     ...     ...        ...         ...                ...

    Returns:
        DataFrame: A pandas DataFrame containing the processed school data with the following columns:
            - estimated_occupancy: The estimated occupancy of the school.
            - age_min: The minimum age of students at the school.
            - age_max: The maximum age of students at the school.
            - latitude: The latitude of the school's central point.
            - longitude: The longitude of the school's central point.
            - sector: The sector of the school (e.g., primary, secondary).

    """

    data = read_csv(school_data_path)

    data = data[data["use"] == "School"]

    data = data[
        ~data["use_type"].isin(
            [
                "Teen Parent Unit",
                "Correspondence School",
            ]
        )
    ]

    data["use_type"] = data["use_type"].map(
        RAW_DATA_INFO["base"]["venue"]["school"]["school_age_table"]
    )

    data[["sector", "age_range"]] = data["use_type"].str.split(" ", n=1, expand=True)
    data["age_range"] = data["age_range"].str.strip("()")
    data[["age_min", "age_max"]] = data["age_range"].str.split("-", expand=True)

    # data[["sector", "age_min", "age_max"]] = data["use_type"].str.extract(
    #    r"([A-Za-z\s]+)\s\((\d+)-(\d+)\)"
    # )

    data["Central Point"] = data["WKT"].apply(get_central_point)

    data["latitude"] = data["Central Point"].apply(lambda point: point.y)
    data["longitude"] = data["Central Point"].apply(lambda point: point.x)

    return data[["estimated_occupancy", "age_min", "age_max", "latitude", "longitude", "sector"]]


def _read_raw_kindergarten(raw_kindergarten_path: str) -> DataFrame:
    """
    Reads and processes raw New Zealand kindergarten data from a CSV file.

    This function reads a CSV file containing kindergarten data, filters the data to include only
    kindergartens with more than 15 licensed positions, and extracts relevant information such as
    area code, maximum licensed positions, and geographical coordinates. It also adds additional
    columns for sector and age range.

    The output looks like:
                area  max_students   latitude   longitude        sector  age_min  age_max
        0     100800            30 -35.118228  173.258565  kindergarten        0        5
        1     101100            30 -34.994478  173.464730  kindergarten        0        5
        2     100700            30 -35.116080  173.270685  kindergarten        0        5
        3     103500            30 -35.405553  173.796409  kindergarten        0        5
        4     103900            30 -35.278121  174.081808  kindergarten        0        5
        .....

    Args:
        raw_kindergarten_path (str): The file path to the CSV file containing the raw kindergarten data.

    Returns:
        DataFrame: A pandas DataFrame containing the processed kindergarten data with the following columns:
            - area: The statistical area code.
            - max_students: The maximum number of licensed positions.
            - latitude: The latitude of the kindergarten.
            - longitude: The longitude of the kindergarten.
            - sector: The sector of the institution (set to 'kindergarten').
            - age_min: The minimum age of students (set to 0).
            - age_max: The maximum age of students (set to 5).
    """
    df = read_csv(raw_kindergarten_path)

    df = df[df["Max. Licenced Positions"] > 15.0]

    df = df[
        [
            "Statistical Area 2 Code",
            "Max. Licenced Positions",
            "Latitude",
            "Longitude",
        ]
    ]

    df = df.rename(
        columns={
            "Statistical Area 2 Code": "area",
            "Max. Licenced Positions": "max_students",
            "Latitude": "latitude",
            "Longitude": "longitude",
        }
    )
    df = df.dropna()

    df["area"] = df["area"].astype(int)
    df["max_students"] = df["max_students"].astype(int)

    df["sector"] = "kindergarten"
    df["age_min"] = 0
    df["age_max"] = 5

    return df


def _read_raw_hospital(raw_hospital_data_path: str) -> DataFrame:
    """Write hospital locations

    The output looks like:
           latitude   longitude  estimated_occupancy source_facility_id
    2    -35.119186  173.260926                 32.0           F04054-H
    4    -45.858787  170.473064                 90.0           F04067-F
    5    -40.337130  175.616683                 11.0           F0B082-C
    6    -40.211906  176.098154                 11.0           F0C087-G
    7    -36.779884  174.756511                 35.0           F3K618-K
    ...         ...         ...                  ...                ...
    Args:
        workdir (str): Working directory
        hospital_locations_cfg (dict): Hospital location configuration
    """
    data = read_csv(raw_hospital_data_path)

    data = data[data["use"] == "Hospital"]

    data["Central Point"] = data["WKT"].apply(get_central_point)

    data["latitude"] = data["Central Point"].apply(lambda point: point.y)
    data["longitude"] = data["Central Point"].apply(lambda point: point.x)

    return data[["latitude", "longitude", "estimated_occupancy", "source_facility_id"]]

def _read_original_csv(osm_data_path: str):
    return read_csv(osm_data_path).drop_duplicates()