from pandas import DataFrame

from copy import deepcopy
from pulp import LpMaximize, LpProblem, LpVariable, lpSum


def add_constrains_shared_space(
    costfunc,
    constrain_var: str,
    population_data_input: DataFrame,
    constrain_data_input: DataFrame,
    population_constrain_map_keys: dict = {"age": ["adult", "child"]},
):

    if len(population_constrain_map_keys.keys()) > 1:
        raise Exception("Population and constrain mapping only support one key")

    population_constrain_map_key = list(population_constrain_map_keys.keys())[0]
    population_constrain_map_values = list(population_constrain_map_keys.values())[0]

    population_data = deepcopy(
        population_data_input[[population_constrain_map_key, "count"]]
    ).reset_index()

    population_data = (
        population_data.groupby(population_constrain_map_key)
        .agg({"index": lambda x: list(x), "count": "sum"})
        .reset_index()
    )

    constrain_data = deepcopy(
        constrain_data_input[population_constrain_map_values + ["count"]]
    ).reset_index()

    constrain_data = (
        constrain_data.groupby(population_constrain_map_values)
        .agg({"index": lambda x: list(x), "count": "sum"})
        .reset_index()
    )

    for proc_key in population_constrain_map_values:

        proc_constrain_terms = []
        for _, proc_constrain_data in constrain_data[
            [proc_key] + ["index", "count"]
        ].iterrows():
            for proc_index in proc_constrain_data["index"]:
                proc_constrain_terms.append(
                    costfunc["constrain_vars"][constrain_var][proc_index]
                    * proc_constrain_data[proc_key]
                )

        proc_population_terms = [
            costfunc["constrain_vars"]["population_data"][i]
            for i in population_data[
                population_data[population_constrain_map_key] == proc_key
            ]["index"].iloc[0]
        ]

        costfunc["costfunc"] += lpSum(proc_constrain_terms) == lpSum(
            proc_population_terms
        )

    return costfunc
