from copy import deepcopy
from datetime import datetime
from logging import getLogger
from random import choices as random_choices
from random import sample as random_sample

from numpy import array as numpy_array
from numpy import isnan
from numpy import nan as numpy_nan
from numpy.random import choice as numpy_choice
from numpy.random import choice as random_choice
from numpy.random import randint
from numpy.random import randint as numpy_randint
from numpy import where as numpy_where
from pandas import DataFrame, Series, concat, isna
from pandas import merge as pandas_merge
from pandas import to_numeric as pandas_to_numeric
from syspop.python.address import add_random_address

from uuid import uuid4

logger = getLogger()


def assign_any_remained_people(
    proc_base_pop: DataFrame,
    adults: DataFrame,
    children: DataFrame,
    assign_children: bool = True,
    assign_adults: bool = True,
) -> DataFrame:
    """Randomly assign remained people to existing household"""

    # Randomly assign remaining adults and children to existing households
    existing_households = proc_base_pop["household"].unique()
    existing_households = [
        x
        for x in existing_households
        if x != "NaN" and not (isinstance(x, float) and isnan(x))
    ]

    while len(adults) > 0 and assign_adults:
        household_id = numpy_choice(existing_households)
        dwelling_type_id = proc_base_pop[proc_base_pop["household"] == household_id][
            "dwelling_type"
        ].values[0]
        hhd_src_id = proc_base_pop[proc_base_pop["household"] == household_id][
            "hhd_src"
        ].values[0]

        num_adults_to_add = numpy_randint(0, 3)

        if num_adults_to_add > len(adults):
            num_adults_to_add = len(adults)

        adult_ids = adults.sample(num_adults_to_add).index.tolist()
        proc_base_pop.loc[proc_base_pop.index.isin(adult_ids), "household"] = (
            household_id
        )
        proc_base_pop.loc[proc_base_pop.index.isin(adult_ids), "dwelling_type"] = (
            dwelling_type_id
        )
        proc_base_pop.loc[proc_base_pop.index.isin(adult_ids), "hhd_src"] = hhd_src_id
        adults = adults.loc[~adults.index.isin(adult_ids)]

    while len(children) > 0 and assign_children:
        household_id = numpy_choice(existing_households)
        dwelling_type_id = proc_base_pop[proc_base_pop["household"] == household_id][
            "dwelling_type"
        ].values[0]
        hhd_src_id = proc_base_pop[proc_base_pop["household"] == household_id][
            "hhd_src"
        ].values[0]
        num_children_to_add = numpy_randint(0, 3)

        if num_children_to_add > len(children):
            num_children_to_add = len(children)

        children_ids = children.sample(num_children_to_add).index.tolist()
        proc_base_pop.loc[proc_base_pop.index.isin(children_ids), "household"] = (
            household_id
        )
        proc_base_pop.loc[proc_base_pop.index.isin(children_ids), "dwelling_type"] = (
            dwelling_type_id
        )
        proc_base_pop.loc[proc_base_pop.index.isin(children_ids), "hhd_src"] = (
            hhd_src_id
        )
        children = children.loc[~children.index.isin(children_ids)]

    return proc_base_pop


def rename_household_id(df: DataFrame, proc_area: str) -> DataFrame:
    """Rename household id from {id} to {adult_num}_{children_num}_{id}

    Args:
        df (DataFrame): base popualtion data

    Returns:
        DataFrame: updated population data
    """
    # Compute the number of adults and children in each household
    df["is_adult"] = df["age"] >= 18
    df["household"] = df["household"].astype(int)

    df["dwelling_type"] = pandas_to_numeric(
        df["dwelling_type"], errors="coerce"
    ).astype("Int64")

    grouped = (
        df.groupby("household")["is_adult"]
        .agg(num_adults="sum", num_children=lambda x: len(x) - sum(x))
        .reset_index()
    )

    # Merge the counts back into the original DataFrame
    df = pandas_merge(df, grouped, on="household")

    df["dwelling_type"] = df["dwelling_type"].astype("str")
    df["dwelling_type"] = df["dwelling_type"].replace({"<NA>": "unknown"})

    # Create the new household_id column based on the specified format
    df["household"] = (
        f"{proc_area}_"
        + df["num_adults"].astype(str)
        + "_"
        + df["num_children"].astype(str)
        + "_"
        + df["household"].astype(str)
    )

    # Drop the temporary 'is_adult' column and other intermediate columns if needed
    return df.drop(["is_adult", "num_adults", "num_children"], axis=1)


def obtain_adult_index_based_on_ethnicity(
    unassigned_adults: DataFrame,
    proc_household_composition: Series,
    ref_ethnicity_prob: float = 0.7,
) -> tuple:
    """Obtain adult index based on ethnicity

    Args:
        unassigned_adults (DataFrame): _description_
        proc_household_composition (DataFrame): _description_
        ref_ethnicity_weight (float, optional): _description_. Defaults to 0.9.

    Returns:
        list: Adult ids
    """

    ref_adult = unassigned_adults.sample(1)
    remained_adults = unassigned_adults[unassigned_adults.index != ref_adult.index[0]]

    adult_ids = ref_adult["index"].tolist()

    probabilities = numpy_where(
        remained_adults["ethnicity"] == ref_adult.ethnicity.values[0], 
        ref_ethnicity_prob, 1.0 - ref_ethnicity_prob)
    probabilities /= probabilities.sum()

    try:
        selected_adults = remained_adults.sample(
            n=proc_household_composition["adults"].values[0] - 1, 
            weights=probabilities)
        adult_ids = adult_ids + selected_adults["index"].to_list()
    except ValueError: # not enough adults to be assigned
        pass

    return adult_ids, ref_adult.ethnicity.values[0]


def assign_household_and_dwelling_id(
    proc_base_pop: DataFrame,
    household_id: str,
    adult_ids: DataFrame,
    children_ids: DataFrame,
    proc_household_composition: DataFrame,
) -> DataFrame:
    """Assign the household and dwelling ID

    Args:
        proc_base_pop (DataFrame): _description_
        household_id (int): _description_
        adult_ids (DataFrame): _description_
        children_ids (DataFrame): _description_
        proc_household_composition (DataFrame): _description_

    Returns:
        DataFrame: _description_
    """
    proc_base_pop.loc[proc_base_pop["index"].isin(adult_ids), "household"] = (
        f"{household_id}"
    )
    proc_base_pop.loc[proc_base_pop["index"].isin(children_ids), "household"] = (
        f"{household_id}"
    )

    """
    proc_base_pop.loc[proc_base_pop["index"].isin(adult_ids), "dwelling_type"] = int(
        proc_household_composition.dwelling_type
    )

    proc_base_pop.loc[proc_base_pop["index"].isin(children_ids), "dwelling_type"] = int(
        proc_household_composition.dwelling_type
    )

    proc_base_pop.loc[proc_base_pop["index"].isin(adult_ids), "hhd_src"] = (
        proc_household_composition.hhd_src
    )

    proc_base_pop.loc[proc_base_pop["index"].isin(children_ids), "hhd_src"] = (
        proc_household_composition.hhd_src
    )
    """
    return proc_base_pop


def get_current_household_composition(proc_houshold_dataset: DataFrame, exclude_row_indices: list) -> DataFrame:
    """
    Sorts the household dataset by randomly selecting a row based on the 'percentage' column,
    after excluding a specified row.

    Parameters:
        proc_houshold_dataset (DataFrame): The household dataset to process.
        exclude_row_index (int): The index of the row to exclude from the dataset.

    Returns:
        DataFrame: A dataframe with one randomly selected row based on the 'percentage' column.
    """
    proc_houshold_dataset = proc_houshold_dataset.drop(exclude_row_indices)

    if len(proc_houshold_dataset) == 0:
        return None

    return proc_houshold_dataset.sample(weights=proc_houshold_dataset["percentage"])


def create_household_composition(
    proc_houshold_dataset: DataFrame,
    proc_base_pop: DataFrame,
    proc_area: int or str,
    only_households_with_adults: bool = True,
) -> DataFrame:
    """Create household composition (V3)

    Args:
        proc_houshold_dataset (DataFrame): Household dataset
        proc_base_pop (DataFrame): Base population dataset
        proc_area (intorstr): Area to use

    Returns:
        DataFrame: Updated population dataset
    """

    if only_households_with_adults:
        proc_houshold_dataset = proc_houshold_dataset[
            proc_houshold_dataset["adults"] > 0
        ]

    unassigned_adults = proc_base_pop[proc_base_pop["age"] >= 18].copy()
    unassigned_children = proc_base_pop[proc_base_pop["age"] < 18].copy()

    # unique_base_pop_ethnicity = list(proc_base_pop["ethnicity"].unique())

    household_id = 0

    exclude_hhd_composition_indices = []

    while True:
        proc_household_composition = get_current_household_composition(
            proc_houshold_dataset, exclude_hhd_composition_indices)

        household_id = str(uuid4())[:6]

        if proc_household_composition is None:
            break

        if (
            len(unassigned_adults) < proc_household_composition["adults"].values[0]
            or len(unassigned_children) < proc_household_composition["children"].values[0]
        ):
            # print("Not enough adults or children to assign.")
            exclude_hhd_composition_indices.append(proc_household_composition.index.values[0])
            continue

        adult_ids, ref_ethnicity = obtain_adult_index_based_on_ethnicity(
            unassigned_adults,
            proc_household_composition,
            # unique_base_pop_ethnicity,
        )

        try:
            children_ids = (
                unassigned_children[
                    unassigned_children["ethnicity"] == ref_ethnicity
                ]
                .sample(proc_household_composition["children"].values[0])["index"]
                .tolist()
            )
        except (
                ValueError,
                IndexError,
            ):
            # Value Error: not enough children for a particular ethnicity to be sampled from;
            # IndexError: len(adults_id) = 0 so mode() does not work
            children_ids = unassigned_children.sample(
                proc_household_composition["children"].values[0]
            )["index"].tolist()

        proc_base_pop = assign_household_and_dwelling_id(
            proc_base_pop,
            f"household_{proc_area}_{len(adult_ids)}-{len(children_ids)}_{household_id}",
            adult_ids,
            children_ids,
            proc_household_composition,
        )

        unassigned_adults = unassigned_adults.loc[
            ~unassigned_adults["index"].isin(adult_ids)
        ]
        unassigned_children = unassigned_children.loc[
            ~unassigned_children["index"].isin(children_ids)
        ]

    proc_base_pop = assign_any_remained_people(
        proc_base_pop, unassigned_adults, unassigned_children
    )

    return proc_base_pop


def household_wrapper(
    houshold_dataset: DataFrame,
    base_pop: DataFrame,
    base_address: DataFrame,
    geo_address_data: DataFrame or None = None
) -> DataFrame:
    """Assign people to different households

    Args:
        houshold_dataset (DataFrame): _description_
        base_pop (DataFrame): _description_
    """
    start_time = datetime.utcnow()

    base_pop["household"] = numpy_nan
    base_pop["dwelling_type"] = numpy_nan
    base_pop["hhd_src"] = numpy_nan

    houshold_dataset["percentage"] = houshold_dataset.groupby("area")["value"].transform(
        lambda x: x / x.sum())

    all_areas = list(base_pop["area"].unique())
    total_areas = len(all_areas)
    results = []

    for i, proc_area in enumerate(all_areas):
        logger.info(f"{i}/{total_areas}: Processing {proc_area}")

        proc_base_pop = base_pop[base_pop["area"] == proc_area].reset_index()
        proc_houshold_dataset = houshold_dataset[houshold_dataset["area"] == proc_area]

        if len(proc_base_pop) == 0:
            continue

        proc_base_pop = create_household_composition(
            proc_houshold_dataset, proc_base_pop, proc_area
        )

        results.append(proc_base_pop)

    for result in results:
        result_index = result["index"]
        result_content = result.drop("index", axis=1)
        base_pop.iloc[result_index] = result_content

    base_pop[["area", "age"]] = base_pop[["area", "age"]].astype(int)
    end_time = datetime.utcnow()

    total_mins = round((end_time - start_time).total_seconds() / 60.0, 3)
    logger.info(f"Processing time (household): {total_mins}")

    if geo_address_data is not None:
        proc_address_data = add_random_address(
            deepcopy(base_pop),
            geo_address_data,
            "household"
        )
        base_address = concat([base_address, proc_address_data])
        base_address["area"] = base_address["area"].astype("int")

    return base_pop, base_address
