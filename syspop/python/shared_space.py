from logging import getLogger

from numpy import NaN as numpy_nan
from numpy import arange as numpy_arrange
from numpy import where as numpy_where
from pandas import DataFrame, concat
from syspop.python import SHARED_SPACE_NEAREST_DISTANCE_KM
from scipy.spatial.distance import cdist

logger = getLogger()


def _remove_duplicates_shared_space(row):
    """Remove duplicated shared space, e.g.,
    x1,x2,x1 to x1,x2"""
    values = row.split(",")
    unique_values = set(values)
    return ",".join(unique_values)


def shared_space_wrapper(
    shared_space_name: str,
    shared_space_data: DataFrame,
    pop_data: DataFrame,
    address_data: DataFrame,
    household_address: DataFrame,
    geography_location_data: DataFrame,
    num_nearest: int = 3,
    assign_address_flag: bool = False,
    area_name_key: str = "area",
):
    """Create synthetic shared space data

    Args:
        shared_space_data (DataFrame): shared space data such as supermarket
        pop_data (DataFrame): population data to be updated
        geography_location_data (DataFrame): geography data
    """

    shared_space_data = shared_space_data.rename(
        columns={
            "latitude": f"latitude_{shared_space_name}",
            "longitude": f"longitude_{shared_space_name}",
            "name": shared_space_name,
        }
    )

    if area_name_key == "area":
        pop_data = pop_data.merge(
            household_address[["household", "latitude", "longitude"]],
            on="household",
            how="left",
        )

        pop_data = pop_data.rename(
            columns={
                "latitude": "src_latitude",
                "longitude": "src_longitude",
            }
        )
    else:
        geography_location_data_updated = geography_location_data.rename(
            columns={
                "area": area_name_key,
                "latitude": f"{area_name_key}_latitude",
                "longitude": f"{area_name_key}_longitude",
            }
        )
        pop_data = pop_data.merge(
            geography_location_data_updated,
            on="area_work",
            how="left",
        )
        pop_data = pop_data.rename(
            columns={
                "area_work_latitude": "src_latitude",
                "area_work_longitude": "src_longitude",
            }
        )

    for i in range(num_nearest):

        if i == 0:
            distance_matrix = cdist(
                pop_data[[f"src_latitude", f"src_longitude"]],
                shared_space_data[
                    [f"latitude_{shared_space_name}", f"longitude_{shared_space_name}"]
                ],
                metric="euclidean",
            )
        else:
            distance_matrix[range(len(nearest_indices)), nearest_indices] = float("inf")

        nearest_indices = distance_matrix.argmin(axis=1)

        nearest_rows = shared_space_data.iloc[nearest_indices].reset_index(drop=True)

        nearest_rows.rename(columns={shared_space_name: f"tmp_{i}"}, inplace=True)

        nearest_rows = nearest_rows.drop(columns=["area"])

        # Remove shared space too far away
        dis_value = distance_matrix[
            numpy_arrange(nearest_indices.shape[0]), nearest_indices
        ]
        dis_indices = numpy_where(
            dis_value > SHARED_SPACE_NEAREST_DISTANCE_KM[shared_space_name] / 110.0
        )[0]

        logger.info(
            f"{shared_space_name}({i}, {area_name_key}): Removing {round((dis_indices.shape[0] / dis_value.shape[0]) * 100.0, 2)}% due to distance"
        )

        nearest_rows.loc[dis_indices, :] = numpy_nan

        pop_data = concat([pop_data, nearest_rows], axis=1)

        pop_data.loc[pop_data[area_name_key] == -9999, f"tmp_{i}"] = ""

        pop_data = pop_data.drop(
            columns=[f"latitude_{shared_space_name}", f"longitude_{shared_space_name}"]
        )

    # Loop through the columns and combine them
    for i in range(num_nearest):
        pop_data[shared_space_name] = (
            pop_data.get(shared_space_name, "")
            + ","
            + pop_data[f"tmp_{i}"].astype(str)
            + ","
        )
        pop_data = pop_data.drop(columns=[f"tmp_{i}"])

    pop_data[shared_space_name] = (
        pop_data[shared_space_name].str.replace(",,", ",").str.strip(",")
    )
    pop_data = pop_data.drop(columns=["src_latitude", "src_longitude"])

    if assign_address_flag:
        address_data = add_shared_space_address(
            pop_data, shared_space_data, address_data, shared_space_name
        )

    pop_data[shared_space_name] = pop_data[shared_space_name].apply(
        _remove_duplicates_shared_space
    )

    return pop_data, address_data


def add_shared_space_address(
    pop_data: DataFrame,
    shared_space_data: DataFrame,
    address_data: DataFrame,
    shared_space_name: str,
) -> DataFrame:
    """Add shared space address

    Args:
        shared_space_data (DataFrame): _description_
        address_data (DataFrame): _description_

    Returns:
        DataFrame: Updated address dataset
    """
    unique_shared_space = list(
        set(pop_data[shared_space_name].str.split(",").explode())
    )

    # get the lat/lon for unique shared space
    unique_shared_space = (
        shared_space_data[
            shared_space_data[shared_space_name].apply(
                lambda x: any(item in x for item in unique_shared_space)
            )
        ]
    ).drop_duplicates()

    unique_shared_space = unique_shared_space.rename(
        columns={
            shared_space_name: "name",
            f"latitude_{shared_space_name}": "latitude",
            f"longitude_{shared_space_name}": "longitude",
        }
    )

    unique_shared_space["type"] = shared_space_name

    return concat([address_data, unique_shared_space])


# ------------------------------
# NEw
# ------------------------------
from pandas import Series
from numpy.random import choice as numpy_choice
from numpy import argsort as numpy_argsort
from uuid import uuid4


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
                    shared_space_data[proc_filter_key] == agent[proc_filter_key]
                ]
            else:
                selected_spaces = selected_spaces[
                    (agent[proc_filter_key] >= shared_space_data[f"{proc_filter_key}_min"]) &
                    (agent[proc_filter_key] <= shared_space_data[f"{proc_filter_key}_max"])
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

    # Compute distances
    distances = cdist(
        coords1, 
        coords2, 
        metric="euclidean")  # Use 'haversine' for geographic distances

    # Find the nearest n indices for each row in household_location_data
    nearest_indices = numpy_argsort(distances, axis=1)[:, :n]

    # Retrieve the names of the nearest areas
    nearest_names = []
    for indices in nearest_indices:
        nearest_names.append(', '.join(shared_space_address.iloc[indices]['id'].values))

    # Update household_location_data with the nearest names
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