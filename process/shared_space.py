
from pandas import DataFrame, merge, concat
from scipy.spatial.distance import cdist

def shared_space_wrapper(
        shared_space_name: str,
        shared_space_data: DataFrame, 
        pop_data: DataFrame,
        geography_location_data: DataFrame,
        num_nearest: int = 3):
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
            "name": shared_space_name}
    )

    shared_space_data = shared_space_data.drop(columns=["area"])

    pop_data = merge(pop_data, geography_location_data, on="area", how="left")

    for i in range(num_nearest):

        if i == 0:
            distance_matrix = cdist(
                pop_data[["latitude", "longitude"]], 
                shared_space_data[[f"latitude_{shared_space_name}", f"longitude_{shared_space_name}"]], 
                metric="euclidean")
        else:
            distance_matrix[range(len(nearest_indices)), nearest_indices] = float('inf')

        nearest_indices = distance_matrix.argmin(axis=1)
        nearest_rows = shared_space_data.iloc[nearest_indices].reset_index(drop=True)

        nearest_rows.rename(columns={shared_space_name: f"tmp_{i}"}, inplace=True)

        pop_data = concat([pop_data, nearest_rows], axis=1)

        pop_data = pop_data.drop(
            columns=[f"latitude_{shared_space_name}", f"longitude_{shared_space_name}"])
        
    # Loop through the columns and combine them
    for i in range(num_nearest):
        pop_data[shared_space_name] = pop_data.get(shared_space_name, "") + pop_data[f"tmp_{i}"].astype(str) + ","
        pop_data = pop_data.drop(columns=[f"tmp_{i}"])

    pop_data[shared_space_name] = pop_data[shared_space_name].str.rstrip(",")
    pop_data = pop_data.drop(columns=["latitude", "longitude"])
    return pop_data
