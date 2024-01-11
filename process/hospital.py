
from pandas import DataFrame, merge, concat
from logging import getLogger
from scipy.spatial.distance import cdist

logger = getLogger()

def create_hospital_names(hospital_data: DataFrame) -> DataFrame:
    """Create hospital name for hospital school following the pattern:
        {sector}_{area}_{id}

    Args:
        hospital_data (DataFrame): _description_
    """
    hospital_data["hospital_name"] = hospital_data.groupby("area").cumcount().astype(str)
    hospital_data['hospital_name'] = (
        hospital_data["area"].astype(str) + 
        "_hospital_" +  
        hospital_data["beds"].astype(str) + 
        "_" + 
        hospital_data["hospital_name"])

    return hospital_data

def hospital_wrapper(
        hospital_data: DataFrame,
        pop_data: DataFrame,
        address_data: DataFrame,
        geography_location_data: DataFrame,
        assign_address_flag: bool = False):
    """Create the nearest and second nearest hospital for each agent

    Args:
        hospital_data (DataFrame): Hospital data
        pop_data (DataFrame): Base population data
        geography_location_data (DataFrame): Geography data
    """
    hospital_data = create_hospital_names(hospital_data)

    hospital_data = hospital_data.rename(
        columns={
            "latitude": "latitude_hospital", 
            "longitude": "longitude_hospital", 
            "area": "area_hospital"}
    )

    pop_data = merge(pop_data, geography_location_data, on="area", how="left")

    # Step 1: get the nearest hospital for each agents
    distance_matrix = cdist(
        pop_data[["latitude", "longitude"]], 
        hospital_data[["latitude_hospital", "longitude_hospital"]], 
        metric="euclidean")
    nearest_indices = distance_matrix.argmin(axis=1)
    nearest_rows = hospital_data.iloc[nearest_indices].reset_index(drop=True)

    pop_data = concat([pop_data, nearest_rows], axis=1)

    pop_data = pop_data.drop(columns=["latitude_hospital", "longitude_hospital", "beds", "area_hospital"])
    pop_data = pop_data.rename(columns={"hospital_name": "primary_hospital"})

    # Step 2: get the second nearest hospital for each agents: 
    #         Set the distance of the nearest rows to infinity in the distance_matrix
    distance_matrix[range(len(nearest_indices)), nearest_indices] = float('inf')

    # Find the index of the second nearest row in X2 for each row in X1
    second_nearest_indices = distance_matrix.argmin(axis=1)

    # Create a new DataFrame with the second nearest rows from X2
    second_nearest_rows = hospital_data.iloc[second_nearest_indices].reset_index(drop=True)

    # Concatenate X1 and second_nearest_rows
    pop_data = concat([pop_data, second_nearest_rows], axis=1)

    # Save address if needed:
    if assign_address_flag:
        address_data = get_hospital_address(pop_data, address_data)

    pop_data = pop_data.drop(columns=["latitude_hospital", "longitude_hospital", "beds", "area_hospital"])
    pop_data = pop_data.rename(columns={"hospital_name": "secondary_hospital"})

    # Final, remove area lat/lon:
    pop_data = pop_data.drop(columns=["latitude", "longitude"])

    return pop_data, address_data


def get_hospital_address(pop_data: DataFrame, address_data: DataFrame) -> DataFrame:
    """Get hospital data address

    Args:
        pop_data (DataFrame): _description_

    Returns:
        DataFrame: _description_
    """
    unique_hospitals = pop_data[["hospital_name", "latitude_hospital", "longitude_hospital"]].drop_duplicates()
    unique_hospitals = unique_hospitals.rename(columns={
        "hospital_name": "name",
        "latitude_hospital": "latitude",
        "longitude_hospital": "longitude"
    })
    unique_hospitals["type"] = "hospital"

    address_data = concat([address_data, unique_hospitals])

    return address_data
