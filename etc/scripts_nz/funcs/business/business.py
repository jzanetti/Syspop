

from copy import copy, deepcopy
from math import ceil as math_ceil
from os.path import join
from re import match as re_match

from numpy import inf, nan
from pandas import DataFrame, concat, merge, pivot_table, read_csv, read_excel, to_numeric

from funcs import RAW_DATA, RAW_DATA_INFO
from funcs.utils import read_anzsic_code, read_leed



def _read_employers_by_employees_data() -> DataFrame:
    """Read the number of employers by employee number

    Returns:
        DataFrame: Employers number by employees number
    """

    data = read_csv(RAW_DATA["business"]["employers_by_employees_number"])[
        ["Area", "Measure", "Enterprise employee count size group", "Value"]
    ]
    data["Area"] = data["Area"].str.replace(" Region", "")

    data = data.drop(
        data[data["Enterprise employee count size group"] == "Total"].index
    )
    data = data[data["Measure"] == "Geographic Units"]

    data = data[["Area", "Enterprise employee count size group", "Value"]]

    data["Area"] = data["Area"].replace("Manawatu-Wanganui", "Manawatu-Whanganui")

    data["Enterprise employee count size group"] = (
        data["Enterprise employee count size group"]
        .replace("1 to 19", "1-19")
        .replace("20 to 49", "20-49")
        .replace("50+", "50-xxx")
    )

    return data.rename(columns={"Area": "region", "Value": "employer_num"})

def read_leed(
    leed_path: str, anzsic_code: DataFrame, if_rate: bool = False
) -> DataFrame:
    """Read NZ stats LEED data

    Args:
        leed_path (str): leed path to be processed
        anzsic_code (Dataframe): ANZSIC codes
        if_rate (bool): if return male and female rate

    Returns:
        DataFrame: Leed dataset
    """
    df = read_excel(leed_path)
    industrial_row = df.iloc[0].fillna(method="ffill")

    if anzsic_code is not None:
        for i, row in enumerate(industrial_row):
            row = row.strip()

            if row in ["Industry", "Total people"]:
                continue

            code = anzsic_code[anzsic_code["Description"] == row]["Anzsic06"].values[0]
            industrial_row[i] = code

    # x = anzsic_code.set_index("Description")
    sec_row = df.iloc[1].fillna(method="ffill")
    titles = industrial_row + "," + sec_row
    titles[
        "Number of Employees by Industry, Age Group, Sex, and Region (derived from 2018 Census)"
    ] = "Area"
    titles["Unnamed: 1"] = "Age"
    titles["Unnamed: 2"] = "tmp"

    df = df.iloc[3:]
    df = df.drop(df.index[-1:])
    df = df.rename(columns=titles)
    df = df.drop("tmp", axis=1)
    df["Area"] = df["Area"].fillna(method="ffill")
    # return df.rename(columns=lambda x: x.strip())

    df["Area"] = df["Area"].replace(
        "Manawatu-Wanganui Region", "Manawatu-Whanganui Region"
    )

    if anzsic_code is not None:
        character_indices = set(
            [
                col.split(",")[0][0]
                for col in df.columns
                if col
                not in ["Area", "Age", "Total people,Male", "Total people, Female"]
            ]
        )

        # Iterate over the unique character indices to sum the corresponding columns
        for char_index in character_indices:
            subset_cols_male = [
                col
                for col in df.columns
                if col.startswith(char_index)
                and col.endswith("Male")
                and col
                not in ["Area", "Age", "Total people,Male", "Total people,Female"]
            ]
            subset_cols_female = [
                col
                for col in df.columns
                if col.startswith(char_index)
                and col.endswith("Female")
                and col
                not in ["Area", "Age", "Total people,Male", "Total people,Female"]
            ]
            summed_col_male = f"{char_index},Male"
            summed_col_female = f"{char_index},Female"
            df[summed_col_male] = df[subset_cols_male].sum(axis=1)
            df[summed_col_female] = df[subset_cols_female].sum(axis=1)
            df = df.drop(subset_cols_male + subset_cols_female, axis=1)

    df["Area"] = df["Area"].str.replace(" Region", "")

    if not if_rate:
        return df

    industrial_columns = [
        x
        for x in list(df.columns)
        if x not in ["Area", "Age", "Total people,Male", "Total people,Female"]
    ]

    df = df.groupby("Area")[industrial_columns].sum()

    df_rate = deepcopy(df)

    # Calculate percentages
    for column in df.columns:
        group = column.split(",")[0]
        total = df[[f"{group},Male", f"{group},Female"]].sum(
            axis=1
        )  # Calculate the total for the group

        total.replace(0, nan, inplace=True)
        df_rate[column] = df[column] / total

    return df_rate


def create_employee_by_gender_by_sector(
    pop: DataFrame,
    geography_hierarchy_data: DataFrame,
):
    """Write the number of employees by gender for different area

    Args:
        workdir (str): Working directory
        employees_cfg (dict): Configuration
        use_sa3_as_super_area (bool): If apply SA3 as super area, otherwise using regions
    """

    def _rename_column(column_name):
        # Define a regular expression pattern to match the column names
        pattern = r"([a-zA-Z]+) ([a-zA-Z]+)"
        matches = re_match(pattern, column_name)
        if matches:
            gender = matches.group(1)[0].lower()
            category = matches.group(2)
            return f"{gender} {category}"
        return column_name

    # Read Leed rate for region
    data_leed_rate = read_leed(
        RAW_DATA["business"]["employee_by_gender_by_sector"]["leed"],
        read_anzsic_code(
            RAW_DATA["business"]["employee_by_gender_by_sector"]["anzsic_code"]
        ),
        if_rate=True,
    )

    # Read employees for different area
    data = read_csv(
        RAW_DATA["business"]["employee_by_gender_by_sector"]["employee_by_area"]
    )[["anzsic06", "Area", "ec_count", "geo_count"]]

    data = data[
        data["anzsic06"].isin(
            list(set([col.split(",")[0] for col in data_leed_rate.columns]))
        )
    ]

    data = data[data["Area"].str.startswith("A")]

    data = data.rename(columns={"Area": "area"})

    data["area"] = data["area"].str[1:].astype(int)

    data = data.merge(
        geography_hierarchy_data[["area", "super_area", "region"]], on="area", how="left"
    )
    data = data.dropna()

    data_leed_rate = data_leed_rate.reset_index()

    data_leed_rate = data_leed_rate.rename(columns={"Area": "region"})
    data = data.merge(data_leed_rate, on="region", how="left")

    industrial_codes = []
    industrial_codes_with_genders = []
    for proc_item in list(data.columns):
        if proc_item.endswith("Male"):
            proc_code = proc_item.split(",")[0]
            industrial_codes.append(proc_code)
            industrial_codes_with_genders.extend(
                [f"{proc_code},Male", f"{proc_code},Female"]
            )

    # Create new columns 'male' and 'female' based on 'anzsic06' prefix
    for category in industrial_codes:
        male_col = f"{category},Male"
        female_col = f"{category},Female"
        data.loc[data["anzsic06"] == category, "Male"] = (
            data[male_col] * data["ec_count"]
        )
        data.loc[data["anzsic06"] == category, "Female"] = (
            data[female_col] * data["ec_count"]
        )

    anzsic_unique_values = data['anzsic06'].unique()
    anzsic_mapping = {anzsic: [f"{anzsic},Male", f"{anzsic},Female"] for anzsic in anzsic_unique_values}
    
    all_data = []
    for i in range(len(data)):
        proc_data = data.iloc[[i]]
        proc_anzsic = proc_data["anzsic06"].values[0]
        proc_data = proc_data[
            ["area", "anzsic06", "geo_count", "ec_count"] + anzsic_mapping[proc_anzsic]]
        proc_data.columns = [col.split(',')[1] if ',' in col else col for col in proc_data.columns]
        all_data.append(proc_data)
    
    all_data = concat(all_data, ignore_index=True)

    # all_data["super_area"] = all_data["super_area"].astype(int)

    all_data = all_data.rename(columns={
        "geo_count": "employer_number", 
        "ec_count": "employee_number",
        "anzsic06": "business_code",
        "Male": "employee_male_ratio", 
        "Female": "employee_female_ratio"})

    return all_data

    df = data.drop(columns=industrial_codes_with_genders)

    total_people_target = int(
        pop["population"].sum()
        * RAW_DATA_INFO["base"]["business"]["employee_by_gender_by_sector"][
            "employment_rate"
        ]
    )
    total_people_current = df["ec_count"].sum()
    people_factor = total_people_target / total_people_current

    df["Male"] = df["Male"] * people_factor
    df["Female"] = df["Female"] * people_factor
    df["ec_count"] = df["ec_count"] * people_factor

    df["Male"] = df["Male"].astype("int")
    df["Female"] = df["Female"].astype("int")
    df["ec_count"] = df["ec_count"].astype("int")

    df_pivot = pivot_table(
        df, index="area", columns="anzsic06", values=["Male", "Female"]
    )

    df_pivot.columns = [f"{col[0]} {col[1]}" for col in df_pivot.columns]

    df_pivot = df_pivot.fillna(0.0)
    df_pivot = df_pivot.astype(int)

    df_pivot = (
        df_pivot.rename(columns=_rename_column)
        .reset_index()
        .rename(columns={"area": "oareas"})
    )

    return df_pivot


def create_employers_by_employees_number(
    pop: DataFrame, geography_hierarchy_definition: DataFrame
):
    """Write number of employers by firm size

    Args:
        workdir (str): _description_
        employees_by_super_area_cfg (dict): _description_
        pop (DataFrame): Population object
        use_sa3_as_super_area (bool): Use SA3 as super area, otherwise using Regions
    """

    pop = _add_region_to_pop(
        pop, geography_hierarchy_definition
    )

    employer_data = _read_employers_by_employees_data()

    df = employer_data.merge(
        pop[["region", "super_area", "total_pop_ratio"]],
        on="region",
        how="left",
    ).dropna()
    df["employer_num_adjusted"] = (df["employer_num"] * df["total_pop_ratio"]).round().astype(int)

    df = df[
        [
            "super_area", 
            "Enterprise employee count size group", 
            "employer_num_adjusted"
        ]
    ]

    data = df.rename(columns={"employer_num_adjusted": "employer_num"})
    data["super_area"] = data["super_area"].astype(int)

    df_pivot = pivot_table(
        data,
        values="employer_num",
        index="super_area",
        columns="Enterprise employee count size group",
    ).reset_index()

    df_pivot.columns.name = None

    for df_key in ["super_area", "1-19", "20-49", "50-xxx"]:
        df_pivot[df_key] = df_pivot[df_key].astype(int)

    return df_pivot


def write_employers_by_sector(
    pop: DataFrame,
    geography_hierarchy_definition: DataFrame,
    employers_by_firm_size_data_input: DataFrame,
):
    """Write number of employers by sectors for super area

    Args:
        workdir (str): _description_
        sectors_by_super_area_cfg (dict): Configuration
        pop (DataFrame): Population object
        use_sa3_as_super_area (bool): Use SA3 as super area, otherwise using Regions
        employers_by_firm_size_data (DataFrame or None): Number of employers by firm size data
    """

    def _scale_employers_by_sector_with_employers_by_firm_size_data(
        employers_by_firm_size_data: DataFrame, scale_employers_by_sector: DataFrame
    ) -> DataFrame:
        """Scaling number of employers_by_sector by the number of employers by firm size

        * Usually "number of employers by firm size" < "number of employers_by_sector", since
            "number of employers by firm size" does not include the employers with 0 employees

        Args:
            employers_by_firm_size_data (DataFrame): number of employers by firm size
            scale_employers_by_sector (DataFrame): number of employers_by_sector

        Returns:
            DataFrame: Updated number of employers_by_sector
        """
        employers_by_firm_size_data["total"] = employers_by_firm_size_data[
            [item for item in employers_by_firm_size_data if item != "MSOA"]
        ].sum(axis=1)
        scale_employers_by_sector["total"] = scale_employers_by_sector[
            [item for item in scale_employers_by_sector if item != "MSOA"]
        ].sum(axis=1)
        merged_df = merge(
            employers_by_firm_size_data, scale_employers_by_sector, on="MSOA"
        )
        merged_df["factor"] = merged_df["total_x"] / merged_df["total_y"]

        # Create a new dataframe with columns "X" and "division"
        merged_df = merged_df[["MSOA", "factor"]]
        scale_employers_by_sector = scale_employers_by_sector[
            [item for item in scale_employers_by_sector if item != "total"]
        ]

        scale_employers_by_sector = merge(
            scale_employers_by_sector, merged_df, on="MSOA"
        )

        columns_to_multiply = [
            item for item in scale_employers_by_sector if item not in ["MSOA", "factor"]
        ]
        scale_employers_by_sector.loc[
            :, columns_to_multiply
        ] = scale_employers_by_sector.loc[:, columns_to_multiply].multiply(
            scale_employers_by_sector.loc[:, "factor"], axis="index"
        )
        scale_employers_by_sector = scale_employers_by_sector.fillna(0.0)
        scale_employers_by_sector[columns_to_multiply] = scale_employers_by_sector[
            columns_to_multiply
        ].applymap(lambda x: math_ceil(x))
        scale_employers_by_sector = scale_employers_by_sector.drop("factor", axis=1)

        return scale_employers_by_sector

    employers_by_firm_size_data = copy(employers_by_firm_size_data_input)

    pop_ratio_sa3 = _get_population_ratio_between_region_and_sa3(
        pop, geography_hierarchy_definition
    )

    data = read_csv(RAW_DATA["business"]["employers_by_sector"])[
        ["Area", "ANZSIC06", "Value"]
    ]

    data["Area"] = data["Area"].str.replace(" Region", "")
    data["Area"] = data["Area"].replace("Manawatu-Wanganui", "Manawatu-Whanganui")
    data["ANZSIC06"] = data["ANZSIC06"].str[0]

    df = data.rename(columns={"Area": "region"}).merge(
        pop_ratio_sa3[["region", "super_area", "population_ratio"]],
        on="region",
        how="left",
    )
    df["Value2"] = df["Value"] * df["population_ratio"]
    df = df.dropna()
    df["Value2"] = df["Value2"].round().astype(int)
    data = df[["super_area", "ANZSIC06", "Value2"]]
    data = data.rename(columns={"super_area": "Area", "Value2": "Value"})
    data["Area"] = data["Area"].astype(int)
    data = data[["Area", "ANZSIC06", "Value"]]

    df_pivot = (
        pivot_table(data, values="Value", index="Area", columns="ANZSIC06")
        .dropna()
        .astype(int)
    ).reset_index()

    df_pivot = df_pivot.rename(columns={"Area": "MSOA"})

    if employers_by_firm_size_data is not None:
        df_pivot = _scale_employers_by_sector_with_employers_by_firm_size_data(
            employers_by_firm_size_data, df_pivot
        )

    return df_pivot


def _add_region_to_pop(
    pop: DataFrame, geography_hierarchy_definition: DataFrame
) -> DataFrame:
    """Get population for each super area and region

    Args:
        pop (DataFrame): Population object
        geography_hierarchy_definition (DataFrame): Geography hirarchy defination data

    Returns:
        DataFrame: Population for each super area and region
    """

    pop["total_pop"] = pop.drop(columns="area").sum(axis=1)

    df = pop.merge(
        geography_hierarchy_definition[["area", "super_area", "region"]],
        on="area",
        how="left",
    ).dropna()
    df["super_area"] = df["super_area"].astype(int)
    df = df[["region", "super_area", "total_pop"]]
    df["total_pop"] = df.groupby("super_area")["total_pop"].transform("sum")

    df.drop_duplicates(inplace=True)

    df["total_pop_ratio"] = df.groupby(["region"])["total_pop"].transform(
            lambda x: (x / x.sum())
    )

    return df

    df["population_ratio"] = df.groupby("super_area")["population"].transform("sum")

    df["population_ratio"] = df.groupby(["region"])["population"].transform(
        lambda x: (x / x.sum())
    )
    df = df[["region", "super_area", "population_ratio"]]

    return df
