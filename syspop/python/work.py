
from logging import getLogger

from pandas import Series, DataFrame
from uuid import uuid4
logger = getLogger()

def create_income(income_dataset: DataFrame) -> DataFrame:
    """Get income data

    Args:
        income_dataset (DataFrame): income dataset

    Returns:
        DataFrame: _description_
    """
    return income_dataset


def create_employee(employee_data: DataFrame, all_areas: list) -> DataFrame:
    """
    Filters employee data for specified areas and returns relevant columns.

    Args:
        employee_data (DataFrame): DataFrame containing employee information.
        all_areas (list): List of areas to filter the employee data by.

    Returns:
        DataFrame: A DataFrame containing filtered employee data with the following columns:
            - area_work (str): Area name (renamed from 'area').
            - business_code (str): Business code identifier.
            - percentage (float): Employee probability or percentage.

    Notes:
        - The input data is filtered to include only rows where the area is in `all_areas`.
        - The 'area' column is renamed to 'area_work' in the returned DataFrame.
    """
    employee_data = employee_data[employee_data["area"].isin(all_areas)]
    employee_data = employee_data.rename(columns={"area": "area_work"})
    return employee_data[["area_work", "business_code", "employee"]]


def create_employer(employer_dataset: DataFrame, address_data: DataFrame, all_areas: list) -> DataFrame:
    """
    Expands employer data into individual records and filters by specified areas.

    Args:
        employer_dataset (DataFrame): DataFrame containing employer 
            information with 'area', 'business_code', and 'employer' columns.
        address_data (DataFrame): Address datasets
        all_areas (list): List of areas to include in the expanded DataFrame.

    Returns:
        DataFrame: Expanded employer data with individual 
            records for each employer, filtered by specified areas.

    Notes:
        - Each row in the original DataFrame is expanded into multiple rows based on the 'employer' count.
        - A unique 6-digit ID is generated for each individual record.
        - The resulting DataFrame contains 'area', 'business_code', and 'id' columns.
    """
    employer_dataset = employer_dataset[employer_dataset["area"].isin(all_areas)]

    employer_datasets = []
    # Loop through each row in the original DataFrame
    for _, row in employer_dataset.iterrows():
        area = row["area"]
        business_code = row["business_code"]
        count = row["employer"]
        proc_address_data_area = address_data[
            address_data["area"] == area]

        # Create individual records for each household
        for _ in range(count):
            proc_address_data = proc_address_data_area.sample(n=1)
            employer_datasets.append({
                "area_work": int(area),
                "business_code": str(business_code),
                "latitude": float(proc_address_data.latitude),
                "longitude": float(proc_address_data.longitude),
                "employer": str(uuid4())[:6]  # Create a 6-digit unique ID
            })
    
    return DataFrame(employer_datasets)


def place_agent_to_employee(employee_data: DataFrame, agent: Series) -> Series:
    """
    Assigns an business_code to an agent based on age, location, and employment rate.

    Args:
        employee_data (DataFrame): DataFrame containing employee information with 'area' and 'employee' columns.
        agent (Series): Series containing agent information with 'age' and 'area' values.

    Returns:
        Series: The updated agent Series with an added 'employee_status' value.

    Notes:
        - Agents under 18 are automatically assigned None (not employed).
        - Agents 18 and older are assigned an employee status based on thrre 
            employment rate and a randomly selected business code from the egfmployee_data DataFrame.
        - The 'employee_status' value is either a business code (str) or None.

    Raises:
        ValueError: If employment_rate is not between 0 and 1.
    """
    if agent.area_work is None:
        selected_code = None
    else:
        proc_employee_data = employee_data[
            employee_data["area_work"] == agent.area_work]
        total_employee = proc_employee_data["employee"].sum()
        if total_employee == 0:
            selected_code = "Unknown"
        else:
            proc_employee_weight = proc_employee_data["employee"] / total_employee
            selected_code = proc_employee_data.sample(
                    n=1, 
                    weights=proc_employee_weight)["business_code"].values[0]

    agent["business_code"] = selected_code

    return agent


def place_agent_to_income(income_data: DataFrame, agent: Series) -> Series:
    """
    Assigns an income value to an agent based on specific criteria from a DataFrame of income data.

    This function filters the income_data DataFrame based on the agent's characteristics (gender, business code,
    ethnicity, and age) and assigns the corresponding income value to the agent. If no matching income record is found,
    the income is set to "Unknown".

    Parameters:
    ----------
    income_data : DataFrame
        A DataFrame containing income data with columns for gender, business_code, age, ethnicity, and value.
    
    agent : Series
        A Series representing an agent with attributes including area_work, gender, business_code, ethnicity, and age.

    Returns:
    -------
    Series
        The modified agent Series, now including an 'income' attribute with the assigned income value 
        or "Unknown" if no match is found.
    """
    if agent.area_work is None:
        selected_income = None
    else:
        income_data[["business_code1", "business_code2"]] = income_data["business_code"].str.split(",", expand=True)
        income_data[["age1", "age2"]] = income_data["age"].str.split("-", expand=True)

        for item in ["business_code1", "business_code2"]:
            income_data[item] = income_data[item].str.strip()
        for item in ["age1", "age2"]:
            income_data[item] = income_data[item].astype(int)

        proc_income_data = income_data[
            (income_data["gender"] == agent.gender) & 
            ((income_data["business_code1"] == agent.business_code) | (income_data["business_code2"] == agent.business_code)) & 
            (income_data["ethnicity"] == agent.ethnicity) &
            ((agent.age >= income_data["age1"]) & (agent.age <= income_data["age2"]) )]

        if len(proc_income_data) > 1:
            raise Exception("Income data decoding error ...")
    
        if len(proc_income_data) == 0:
            selected_income = "Unknown"
        else:
            selected_income = proc_income_data["value"].values[0]

        agent["income"] = str(selected_income) # we can't have nan, unknown and a numerical value together in a parquet
    
    return agent