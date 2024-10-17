from logging import getLogger

from numpy import arange as numpy_arrange
from pandas import DataFrame
from syspop.python import SHARED_SPACE_NEAREST_DISTANCE_KM
from scipy.spatial.distance import cdist
from pandas import Series
from numpy.random import choice as numpy_choice
from numpy import argsort as numpy_argsort
from uuid import uuid4

logger = getLogger()


def create_shared_data(shared_space_data: DataFrame) -> DataFrame:
    """
    Create a DataFrame of shared space data by transforming the input DataFrame.

    This function extracts the 'area', 'latitude', and 'longitude' columns from the 
    provided DataFrame. It then generates a new DataFrame where each row corresponds 
    to an entry in the input data, with an added unique identifier for each entry.

    Parameters:
        shared_space_data (DataFrame): A pandas DataFrame containing at least the 
                                        'area', 'latitude', and 'longitude' columns.

    Returns:
        DataFrame: A new DataFrame containing the transformed shared space data, 
                with 'area' as an integer, a unique 'id' as a string (first 6 
                characters of a UUID), and 'latitude' and 'longitude' as floats.
    """
    shared_space_data = shared_space_data[["area", "latitude", "longitude"]]

    shared_space_datas = []
    for _, row in shared_space_data.iterrows():
        shared_space_datas.append({
            "area": int(row.area),
            "id": str(uuid4())[:6],
            "latitude": float(row.latitude),
            "longitude": float(row.longitude),
        })

    return DataFrame(shared_space_datas)


def place_agent_to_shared_space_based_on_area(
        shared_space_data: DataFrame, 
        agent: Series, 
        shared_space_type: str,
        filter_keys: list = [],
        weight_key: str or None = None,
        shared_space_type_convert: dict or None = None) -> Series:
    """
    Assign an agent to a shared space based on specified area criteria.

    This function selects a shared space from the provided data based on the agent's 
    area and other filtering criteria. If multiple shared spaces meet the criteria, 
    one is randomly selected, optionally weighted by a specified key.

    Parameters:
        shared_space_data (DataFrame): A pandas DataFrame containing shared space 
                                        information, including area and filter criteria.
        agent (Series): A pandas Series representing an agent with area and filter values.
        shared_space_type (str): The type of shared space to which the agent is being assigned.
        filter_keys (list, optional): A list of keys used for additional filtering of 
                                    shared spaces. Defaults to an empty list.
        weight_key (str or None, optional): A key used for weighting the selection of 
                                            shared spaces. If None, selection is uniform. 
                                            Defaults to None.

    Returns:
        Series: The updated agent Series with the selected shared space ID assigned to 
                the corresponding shared_space_type.
    """
    selected_space_id = None

    if agent[f"area_{shared_space_type}"] is not None:

        selected_spaces = shared_space_data[
            shared_space_data[f"area_{shared_space_type}"] == 
            agent[f"area_{shared_space_type}"]
        ]
         
        for proc_filter_key in filter_keys:
            if proc_filter_key in shared_space_data:
                selected_spaces = selected_spaces[
                    agent[proc_filter_key] == selected_spaces[proc_filter_key]
                ]
            else:
                selected_spaces = selected_spaces[
                    (agent[proc_filter_key] >= selected_spaces[f"{proc_filter_key}_min"]) &
                    (agent[proc_filter_key] <= selected_spaces[f"{proc_filter_key}_max"])
                ]
        if len(selected_spaces) == 0:
            selected_space_id = "Unknown"
        else:
            if weight_key is None:
                selected_space_id = selected_spaces.sample(
                    n=1).id.values[0]
            else:
                selected_space_id = selected_spaces.loc[numpy_choice(
                    selected_spaces.index, 
                    p = selected_spaces[weight_key] / selected_spaces[weight_key].sum())].id

    

    if shared_space_type_convert is not None:
        shared_space_type = shared_space_type_convert[shared_space_type]

    agent[shared_space_type] = selected_space_id

    return agent



def find_nearest_shared_space_from_household(
        household_data: DataFrame, 
        shared_space_address: DataFrame,
        geography_location: DataFrame,
        shared_space_type: str,
        n: int =2) -> DataFrame:
    """
    Find the nearest shared spaces for households based on geographic coordinates.

    This function identifies the nearest shared spaces for each household from the specified
    geography location. It computes distances between the household locations and shared space
    addresses and updates the household data with the nearest shared spaces.

    Parameters:
    ----------
    household_data : DataFrame
        A DataFrame containing household information, including an 'area' column.
    
    shared_space_address : DataFrame
        A DataFrame containing shared space addresses, with 'latitude', 'longitude', and 'id' columns.
    
    geography_location : DataFrame
        A DataFrame containing geographic data, including 'area', 'latitude', and 'longitude' columns.
    
    shared_space_type : str
        The name of the column to be added to the updated DataFrame, which will contain the nearest 
        shared space IDs for each household.

    n : int, optional
        The number of nearest shared spaces to retrieve for each household. Default is 2.

    Returns:
    -------
    DataFrame
        The updated geography_location DataFrame with a new column containing the IDs of the 
        nearest shared spaces based on the specified shared_space_type.
    """
    updated_src_data = geography_location[
        geography_location["area"].isin(household_data.area.unique())]
    # Extract latitude and longitude as numpy arrays
    coords1 = updated_src_data[["latitude", "longitude"]].values
    coords2 = shared_space_address[["latitude", "longitude"]].values

    # Compute distances:
    # distances will be a matrix where each element [i, j] represents the Euclidean distance 
    # between the i-th point in coords1 and the j-th point in coords2.
    distances = cdist(
        coords1, 
        coords2, 
        metric="euclidean")

    # Find the nearest n indices for each row in household_location_data: 
    # nearest_indices is an array where each row contains the indices of the n closest points (in coords2) 
    # to the corresponding point in coords1.
    nearest_indices = numpy_argsort(distances, axis=1)[:, :n]

    distance_value = distances[
        numpy_arrange(nearest_indices.shape[0])[:, None], nearest_indices
    ]
    nearest_names = []
    total_missing = 0
    totals_expected = coords1.shape[0] * n # number of household area * number of nearest points
    for i, indices in enumerate(nearest_indices):
        proc_names = []
        for j, index in enumerate(indices):
            proc_dis = distance_value[i, j]
            if proc_dis > SHARED_SPACE_NEAREST_DISTANCE_KM[shared_space_type] / 110.0:
                total_missing += 1
                continue
            proc_names.append(shared_space_address.loc[index]["id"])
        
        if len(proc_names) == 0:
            nearest_names.append("Unknown")
        else:
            nearest_names.append(", ".join(proc_names))

    logger.info(f"Missing {shared_space_type}: {round(total_missing * 100.0/totals_expected, 2)}%")

    updated_src_data[shared_space_type] = nearest_names
    return updated_src_data



def place_agent_to_shared_space_based_on_distance(
        agent: Series, 
        shared_space_loc: DataFrame) -> Series:
    """
    Assigns the location of an agent to a shared space based on the area attribute.

    This function iterates over the shared space locations and updates the agent's
    position according to the specified area. It finds the corresponding shared space 
    location for the agent's area and assigns it to the agent.

    Args:
        agent (Series): A Pandas Series representing the agent, which must contain
                        an 'area' attribute.
        shared_space_loc (DataFrame): A Pandas DataFrame containing shared space 
                                       locations with an 'area' column for filtering.

    Returns:
        Series: The updated agent Series with the assigned shared space locations.
    """
    for proc_shared_space_name in shared_space_loc:
        proc_shared_space_loc = shared_space_loc[proc_shared_space_name]
        agent[proc_shared_space_name] = proc_shared_space_loc[
            proc_shared_space_loc["area"] == agent.area][proc_shared_space_name].values[0]

    return agent