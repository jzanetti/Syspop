from copy import deepcopy
from logging import getLogger

from numpy import NaN
from numpy import vectorize as numpy_vectorize
from numpy.random import choice as numpy_choice
from numpy.random import uniform as numpy_uniform
from pandas import DataFrame, concat

from syspop.process.address import add_random_address
from syspop.process.commute import home_and_work, shared_transport

logger = getLogger()


def assign_employers_to_base_pop(
    base_pop: DataFrame, all_employers: dict, use_for_loop: bool = False
) -> DataFrame:
    """Assign employer/company to base population

    Args:
        base_pop (DataFrame): Base population to be added
        all_employers (dict): employers list, e.g.,
            {110400: [110400_N_4, 110400_N_5, 110400_S_4, ...], ...}

    Returns:
        DataFrame: Updated population
    """
    base_pop["company"] = NaN
    total_people = len(base_pop)

    if use_for_loop:
        i = 0
        for index, proc_row in base_pop.iterrows():
            if i % 100000 == 0.0:
                logger.info(f"Business processing: {i}/{total_people}")

            if proc_row["area_work"] == -9999:
                i += 1
                continue

            proc_area_work = proc_row["area_work"]

            possible_employers = all_employers[proc_area_work]

            base_pop.at[index, "company"] = numpy_choice(possible_employers)

            i += 1
    else:
        # Create a mask for rows where area_work is not -9999
        mask = base_pop["area_work"] != -9999

        # Apply the mask to filter relevant rows
        valid_rows = base_pop[mask]

        # Use numpy_choice to generate random choices for each row in valid_rows
        choices = numpy_vectorize(lambda x: numpy_choice(all_employers[x]))(
            valid_rows["area_work"]
        )

        # Assign the choices back to the "company" column in the original dataframe
        base_pop.loc[mask, "company"] = choices

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
        proc_employer_num = int(proc_row["employer_number"] / employer_num_factor)
        proc_employer_code = proc_row["business_code"]
        proc_employer_area = proc_row["area"]

        for employer_id in range(proc_employer_num):
            employers.append(f"{proc_employer_code}_{employer_id}_{proc_employer_area}")

    return employers


def work_and_commute_wrapper(
    business_data: dict,
    pop_data: DataFrame,
    base_address: DataFrame,
    commute_data: DataFrame,
    geo_hirarchy_data: DataFrame,
    geo_address_data: DataFrame or None = None,
    use_parallel: bool = False,
    n_cpu: int = 4,
) -> DataFrame:
    """Create business and commute data

    Args:
        business_data (dict): Business data, e.g., employer, employee, school etc.
        pop_data (DataFrame): Population dataset
        commute_data (DataFrame): Commute dataset, e.g., home_to_work etc.
        use_parallel (bool, optional): If run jobs in parallel. Defaults to False.
        n_cpu (int, optional): Number of CPUs to use. Defaults to 4.

    Raises:
        Exception: Not implemented yet ...

    Returns:
        DataFrame: Updated population data
    """

    base_pop = work_wrapper(
        business_data["employer"],
        business_data["employee"],
        pop_data,
        commute_data,
        use_parallel=use_parallel,
        n_cpu=n_cpu,
    )

    if geo_address_data is not None:
        proc_address_data = add_random_address(
            deepcopy(base_pop), geo_address_data, "company", use_parallel=use_parallel
        )
        base_address = concat([base_address, proc_address_data])

    base_pop = shared_transport(base_pop, geo_hirarchy_data)

    return base_pop, base_address


def work_wrapper(
    employer_data: DataFrame,
    employee_data: DataFrame,
    pop_data: DataFrame,
    commute_data: DataFrame,
    use_parallel: bool = False,
    n_cpu: int = 4,
):
    """Assign individuals to different companies:

    Args:
        employer_data (DataFrame): _description_
        employee_data (DataFrame): _description_
        pop_data (DataFrame): _description_
        commute_data (DataFrame): _description_
    """
    all_work_areas = list(pop_data["area"].unique())

    all_commute_data = []
    all_employers = {}
    for proc_area in all_work_areas:
        proc_commute_data = commute_data[commute_data["area_work"] == proc_area]
        proc_employee_data = employee_data[employee_data["area"] == proc_area]
        proc_employer_data = employer_data[employer_data["area"] == proc_area]

        proc_commute_data = align_commute_data_to_employee_data(
            proc_employee_data, proc_commute_data
        )

        all_commute_data.append(proc_commute_data)

        all_employers[proc_area] = create_employers(proc_employer_data)

    all_commute_data = concat(all_commute_data, ignore_index=True)

    logger.info("Assign home and work locations ...")

    base_pop = home_and_work(
        all_commute_data, pop_data, use_parallel=use_parallel, n_cpu=n_cpu
    )

    logger.info("Assign employers ...")
    base_pop = assign_employers_to_base_pop(base_pop, all_employers)

    return base_pop
