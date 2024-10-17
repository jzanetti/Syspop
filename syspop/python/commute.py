from logging import getLogger
from numpy.random import choice as numpy_choice
from pandas import DataFrame
from pandas import Series

logger = getLogger()


def create_commute_probability(
        commute_dataset: DataFrame, 
        areas: list, 
        commute_type: str = "work") -> DataFrame:
    """
    Calculates commute probabilities for specified areas and commute type.

    Args:
        commute_dataset (DataFrame): DataFrame containing commute data with 'area_home' and 'area_work' columns.
        areas (list): List of areas to filter commute data by.
        commute_type (str, optional): Type of commute (e.g., 'work', 'school'). Defaults to "work".

    Returns:
        DataFrame: Commute probabilities for each travel method, area home, and area work.

    Notes:
        - The function filters commute data by specified areas.
        - It calculates the total number of people commuting from each area home.
        - Probabilities are calculated by dividing the number of people using each travel method
          by the total number of people commuting from each area home.
    """
    commute_dataset = commute_dataset[commute_dataset["area_home"].isin(areas)]
    travel_methods = [col for col in commute_dataset.columns if col not in [
            "area_home", f"area_{commute_type}"]]

    total_people = commute_dataset.groupby("area_home")[travel_methods].sum().sum(axis=1)
    area_sums = commute_dataset.groupby(["area_home", f"area_{commute_type}"])[travel_methods].sum()
    return area_sums.div(total_people, axis=0).reset_index()


def assign_agent_to_commute(
        commute_dataset: DataFrame, 
        agent: Series,
        commute_type: str = "work",
        include_filters: dict = {}) -> Series:
    """
    Assign a commuting area and travel method to an agent based on commute data.

    This function updates an agent's attributes by selecting a commuting area and
    travel method based on the provided commute dataset. It takes into account any 
    exclusion filters that may apply to the agent.

    Parameters:
    ----------
    commute_dataset : DataFrame
        A DataFrame containing commuting data, including areas and corresponding travel methods.
    
    agent : Series
        A Series representing an agent, which includes information such as area and any relevant
        attributes for filtering.

    commute_type : str, optional
        The type of commute (e.g., "work" or "school"). This determines the attributes that will
        be assigned to the agent. Default is "work".

    include_filters : dict, optional
        A dictionary of filters to include certain agents from being assigned a commute area and
        method. Each key corresponds to an attribute of the agent, and the value is a list of
        tuples defining the ranges to include.

    Returns:
    -------
    Series
        The updated agent Series with the assigned commuting area and travel method.
        The following attributes will be modified:
        - area_{commute_type}: int, the area assigned for the commute
        - travel_method_{commute_type}: str, the travel method assigned for the commute
    """
    for include_key in include_filters:
        proc_filters = include_filters[include_key]
        for proc_filter in proc_filters:
            if agent[include_key] < proc_filter[0] or agent[include_key] > proc_filter[1]:
                agent[f"area_{commute_type}"] = None
                agent[f"travel_method_{commute_type}"] = None
                return agent
        
    proc_commute_dataset = commute_dataset[commute_dataset.area_home == agent.area]
    proc_commute_dataset["total"] = proc_commute_dataset.drop(
        columns=["area_home", f"area_{commute_type}"]).sum(axis=1)
    selected_row = proc_commute_dataset.loc[
        numpy_choice(proc_commute_dataset.index, p=proc_commute_dataset["total"])]
    selected_area = int(selected_row[f"area_{commute_type}"])
    selected_row = selected_row.drop(["area_home", f"area_{commute_type}", "total"])
    travel_method = numpy_choice(
        selected_row.index, p=selected_row /selected_row.sum())

    agent[f"area_{commute_type}"] = selected_area
    agent[f"travel_method_{commute_type}"] = travel_method

    return agent