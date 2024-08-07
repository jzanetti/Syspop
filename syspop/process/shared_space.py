from logging import getLogger

from numpy import NaN as numpy_nan
from numpy import arange as numpy_arrange
from numpy import where as numpy_where
from pandas import DataFrame, concat
from process import SHARED_SPACE_NEAREST_DISTANCE_KM
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
