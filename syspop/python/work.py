from copy import deepcopy
from logging import getLogger

from numpy import nan as numpy_nan
from numpy.random import choice as numpy_choice
from numpy.random import uniform as numpy_uniform
from pandas import DataFrame, concat, merge
from syspop.python.address import add_random_address
from syspop.python.commute import travel_between_home_and_work, shared_transport

logger = getLogger()


def assign_employees_employers_to_base_pop(
    base_pop: DataFrame, all_employers: dict, employee_data: DataFrame
) -> DataFrame:
    """Assign employer/company to base population

    Args:
        base_pop (DataFrame): Base population to be added
        all_employers (dict): employers list, e.g.,
            {110400: [110400_N_4, 110400_N_5, 110400_S_4, ...], ...}

    Returns:
        DataFrame: Updated population
    """

    def process_row(proc_row, employee_data, all_employers):
        if proc_row["area_work"] == -9999:
            return numpy_nan  # or any other placeholder for invalid data

        proc_area_work = proc_row["area_work"]
        proc_employee_data = employee_data[employee_data["area"] == proc_area_work]

        tries = 0
        while True:
            
            if tries > 5:
                break
            try:
                possible_work_sector = proc_employee_data.sample(
                    weights=proc_employee_data["employee_percentage"])["business_code"].values[0]
                possible_employers = all_employers[proc_area_work]
                output_employer = numpy_choice(
                    [item for item in possible_employers if 
                     item.startswith(possible_work_sector)])
            except ValueError:
                continue

            return output_employer
        return "Unknown"

    total_employees_per_area = employee_data.groupby("area")["employee"].transform("sum")
    employee_data["employee_percentage"] = employee_data[
        "employee"] / total_employees_per_area

    base_pop["company"] = numpy_nan
    base_pop["company"] = base_pop.apply(
        lambda row: process_row(row, employee_data, all_employers), axis=1)

    return base_pop


def align_commute_data_to_employee_data(
    employee_input: DataFrame,
    commute_input: DataFrame,
    process_remained_people: bool = False,
) -> DataFrame:
    """Align commute dataset (the number of people travel to work) to employee data

    Args:
        employee_input (DataFrame): employee data to be used
        commute_input (DataFrame): commute data to be used
        method (str): can be multiply or adding
            - mutiple is simpler and faster, but it may not match well (e.g., mutiplication applies to 0.0 will still be 0.0)
            - adding: slower but more accurate, it adds the mismatch randomly to the target dataframe

    Returns:
        DataFrame: updated commute data
    """

    total_employee_from_commute_data = (
        commute_input.drop(columns=["area_home", "area_work"]).sum().sum()
    )
    total_employee_from_employee_data = employee_input["employee_number"].sum()

    if total_employee_from_commute_data == 0:
        commute_input.loc[commute_input.index, "Other"] = (
            total_employee_from_employee_data
        )
        return commute_input

    # Step 1: scaling up/down the commute employee number
    scaling_factor = (
        total_employee_from_employee_data / total_employee_from_commute_data
    )
    # apply scaling factor to align commute data to employee data
    commute_input = commute_input.apply(
        lambda x: x * scaling_factor if x.name not in ["area_home", "area_work"] else x,
        axis=0,
    )

    commute_input = commute_input.apply(
        lambda x: x.astype(int) if x.name not in ["area_home", "area_work"] else x
    )
    # commute_input = commute_input.fillna(0.0)

    if process_remained_people:
        # Step 2: add remained employee numbers randomly
        total_employee_from_commute_data = (
            commute_input.drop(columns=["area_home", "area_work"]).sum().sum()
        )

        total_value_to_add = (
            total_employee_from_employee_data - total_employee_from_commute_data
        )

        columns_to_adjust = [
            col
            for col in commute_input.columns
            if col not in ["area_home", "area_work"]
        ]

        while total_value_to_add != 0.0:
            # Step 2.1: randomly select a column and row
            column_name = numpy_choice(columns_to_adjust)
            row_index = numpy_choice(commute_input.index)

            # Step 2.2: create a random value to add
            random_value_range = total_value_to_add / 3.0

            if total_value_to_add > 0:
                random_value = int(
                    min(numpy_uniform(0, random_value_range), random_value_range)
                )
                random_value = random_value if random_value != 0 else 1
            else:
                random_value = int(
                    max(numpy_uniform(random_value_range, 0), random_value_range)
                )
                random_value = random_value if random_value != 0 else -1

            # Step 2.3: update the value
            updated_value = commute_input.at[row_index, column_name] + random_value

            if (
                updated_value < 0
            ):  # make sure that the number of employees are larger than zero
                continue

            commute_input.at[row_index, column_name] = updated_value

            # Step 2.4: Update the remaining total
            total_value_to_add -= random_value

    # recalculate the total commute (home and work) using updated data:
    commute_input["Total"] = commute_input.loc[
        :, ~commute_input.columns.isin(["area_home", "area_work"])
    ].sum(axis=1)

    return commute_input


def create_employers(
    employer_input: DataFrame, employer_num_factor: float = 1.0
) -> list:
    """Create available employers

    Args:
        employer_input (DataFrame): employers dataset
        employer_num_factor (int): Should we reduce the number of employers
            (so more people can get together ?)

    Returns:
        list: the possible employers
    """
    employers = []
    for index, proc_row in employer_input.iterrows():
        proc_employer_num = int(proc_row["employer"] / employer_num_factor)
        proc_employer_code = proc_row["business_code"]
        proc_employer_area = proc_row["area"]

        for employer_id in range(proc_employer_num):
            employers.append(f"{proc_employer_code}_{employer_id}_{proc_employer_area}")

    return employers


def work_and_commute_wrapper(
    employer_data: DataFrame,
    employee_data: DataFrame,
    pop_data: DataFrame,
    base_address: DataFrame,
    commute_data: DataFrame,
    geo_hirarchy_data: DataFrame,
    geo_address_data: DataFrame or None = None
) -> DataFrame:
    """Create business and commute data

    Args:
        business_data (dict): Business data, e.g., employer, employee, school etc.
        pop_data (DataFrame): Population dataset
        commute_data (DataFrame): Commute dataset, e.g., home_to_work etc.


    Raises:
        Exception: Not implemented yet ...

    Returns:
        DataFrame: Updated population data
    """

    base_pop = create_work_and_commute(
        employer_data,
        employee_data,
        pop_data,
        commute_data
    )

    if geo_address_data is not None:
        proc_address_data = add_random_address(
            deepcopy(base_pop), geo_address_data, "company"
        )
        base_address = concat([base_address, proc_address_data])

    base_pop = shared_transport(base_pop, geo_hirarchy_data)

    return base_pop, base_address


def create_work_and_commute(
    employer_data: DataFrame,
    employee_data: DataFrame,
    pop_data: DataFrame,
    commute_data: DataFrame
):
    """Assign individuals to different companies:

    Args:
        employer_data (DataFrame): _description_
        employee_data (DataFrame): _description_
        pop_data (DataFrame): _description_
        commute_data (DataFrame): _description_
    """
    all_areas = list(pop_data["area"].unique())

    all_commute_data = []
    all_employers = {}
    all_employees = {}
    for proc_area in all_areas:
        proc_commute_data = commute_data[commute_data["area_work"] == proc_area]
        proc_employee_data = employee_data[employee_data["area"] == proc_area]
        proc_employer_data = employer_data[employer_data["area"] == proc_area]

        all_commute_data.append(proc_commute_data)

        all_employers[proc_area] = create_employers(proc_employer_data)
        all_employees[proc_area] = proc_employee_data["employee"].sum()

    all_commute_data = concat(all_commute_data, ignore_index=True)

    logger.info("Assign home and work locations ...")
    base_pop = travel_between_home_and_work(
        all_commute_data, pop_data, all_employees
    )

    logger.info("Assign employers ...")
    base_pop = assign_employees_employers_to_base_pop(base_pop, all_employers, employee_data)

    return base_pop



# --------------------------
# New
# --------------------------
from pandas import Series
from uuid import uuid4

def create_employee_probability(employee_data: DataFrame, all_areas: list) -> DataFrame:
    """
    Calculates employee probability for each business code within specified areas.

    Args:
        employee_data (DataFrame): DataFrame containing employee information.
        all_areas (list): List of areas to filter employee data by.

    Returns:
        DataFrame: A DataFrame containing area, business code, and corresponding employee probability.

    Notes:
        - Employee probability is calculated as the proportion of employees for each business code within an area.
        - The resulting DataFrame is filtered to only include areas specified in `all_areas`.

    Columns:
        - area_work (str): Area name
        - business_code (str): Business code identifier
        - percentage (float): Employee probability (proportion of employees)
    """
    employee_data = employee_data[employee_data["area"].isin(all_areas)]

    total_employees_per_area = employee_data.groupby("area")["employee"].transform("sum")
    employee_data["percentage"] = employee_data[
        "employee"] / total_employees_per_area
    
    employee_data = employee_data.rename(columns={"area": "area_work"})
    return employee_data[["area_work", "business_code", "percentage"]]


def create_employer(employer_dataset: DataFrame, all_areas: list) -> DataFrame:
    """
    Expands employer data into individual records and filters by specified areas.

    Args:
        employer_dataset (DataFrame): DataFrame containing employer 
            information with 'area', 'business_code', and 'employer' columns.
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
        
        # Create individual records for each household
        for _ in range(count):
            employer_datasets.append({
                "area_work": int(area),
                "business_code": str(business_code),
                "id": str(uuid4())[:6]  # Create a 6-digit unique ID
            })
    
    return DataFrame(employer_datasets)


def place_agent_to_employer(employer_data: DataFrame, agent: Series) -> Series:
    """
    Assigns an employer to an agent based on area and business code.

    Args:
        employer_data (DataFrame): DataFrame containing employer information with 'area', 'business_code', and 'id' columns.
        agent (Series): Series containing agent information with 'area', 'business_code', and optionally 'employer' values.

    Returns:
        Series: The updated agent Series with an assigned 'employer' value.

    Notes:
        - If the agent's business code is None, the employer is set to None.
        - If no matching employer is found, the employer is set to 'Unknown'.
        - Otherwise, a random matching employer is selected.
    """
    if agent.area_work is None:
        selected_employer = None
    else:
        selected_employers = employer_data[
            (employer_data["area_work"] == agent.area_work) & 
            (employer_data["business_code"] == agent.business_code)]
        
        if len(selected_employers) == 0:
            selected_employer = "Unknown"
        else:
            selected_employer = selected_employers.sample(n=1).id.values[0]
        
    agent["employer"] = selected_employer

    return agent

    


def assign_agent_to_employee(employee_data: DataFrame, agent: Series, employment_rate: float = 0.9) -> Series:
    """
    Assigns an employee status to an agent based on age, location, and employment rate.

    Args:
        employee_data (DataFrame): DataFrame containing employee information with 'area' and 'percentage' columns.
        agent (Series): Series containing agent information with 'age' and 'area' values.
        employment_rate (float, optional): Probability of an adult agent being employed. Defaults to 0.9.

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
        selected_code = employee_data[
            employee_data["area_work"] == agent.area_work].sample(
                n=1, weights="percentage")["business_code"].values[0]

    agent["business_code"] = selected_code

    return agent