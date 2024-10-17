
from logging import getLogger
from pandas import DataFrame, Series

from uuid import uuid4

logger = getLogger()

def create_households(household_data: DataFrame, address_data: DataFrame, areas: list,):
    """
    Create a DataFrame of individual households from aggregated data.

    Parameters:
        household_data (pd.DataFrame): A DataFrame containing aggregated household data
                                    with columns ['area', 'adults', 'children', 'value'].
                                    'value' indicates the number of households for the
                                    given combination of adults and children.
        address_data (pd.DataFrame): address data in latitude and longitude
        areas (list): the areas to be included

    Returns:
    pd.DataFrame: A DataFrame with individual household records, containing the columns:
                  ['area', 'adults', 'children', 'name'], where 'name' is a unique ID
                  generated for each household.
    """
    households = []

    household_data = household_data[household_data["area"].isin(areas)]
    address_data = address_data[address_data["area"].isin(areas)]

    # Loop through each row in the original DataFrame
    for _, row in household_data.iterrows():
        area = row["area"]
        adults = row["adults"]
        children = row["children"]
        count = row["value"]
        proc_address_data_area = address_data[
            address_data["area"] == area]
        
        # Create individual records for each household
        for _ in range(count):
            proc_address_data = proc_address_data_area.sample(n=1)
            households.append({
                "area": int(area),
                "adults": int(adults),
                "children": int(children),
                "latitude": float(proc_address_data.latitude),
                "longitude": float(proc_address_data.longitude),
                "household": str(uuid4())[:6]  # Create a 6-digit unique ID
            })
    
    return DataFrame(households)


def place_agent_to_household(households: DataFrame, agent: Series) -> tuple:
    """
    Assigns an agent to a household based on the agent's age.

    Args:
        households (DataFrame): A DataFrame containing household information.
        agent (Series): A Series containing agent information, including 'age'.

    Returns:
        tuple: A tuple containing the agent (with household name)
            and the updated households DataFrame.

    Notes:
        - Adults (age >= 18) are assigned to households with available adult spaces.
        - Children (age < 18) are assigned to households with available child spaces.
        - If no suitable households are available, the agent is assigned to a random household.
    """
    agent_type = "adults" if agent.age >= 18 else "children"

    selected_households = households[(households[agent_type] >= 1) & (households["area"] == agent.area)]
    if len(selected_households) > 0:
        selected_household = selected_households.sample(n=1)
        households.at[selected_household.index[0], agent_type] -= 1
    else:
        selected_household = households.sample(n=1)

    agent["household"] = selected_household.household.values[0]
    return agent, households


