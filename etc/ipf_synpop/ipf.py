from copy import deepcopy

from ipfn.ipfn import ipfn
from pandas import DataFrame, melt, merge


def pop_pivot(
    base_pop: DataFrame,
    pivot_cfg: dict = {"index": ["sex", "ethnicity"], "columns": ["age"]},
):

    base_pop_pivot = base_pop.pivot_table(
        index=pivot_cfg["index"],
        columns=pivot_cfg["columns"],
        values="count",
        aggfunc="sum",
        fill_value=0,
    )

    col_names = [None] * len(pivot_cfg["columns"])
    ind_names = [None] * len(pivot_cfg["index"])

    if len(col_names) > 1:
        base_pop_pivot.columns.names = col_names
    else:
        base_pop_pivot.columns.name = col_names[0]

    if len(ind_names) > 1:
        base_pop_pivot.index.names = ind_names
    else:
        base_pop_pivot.index.name = ind_names[0]

    return base_pop_pivot.astype(float)


def ipf_adjustment(base_pop_input: DataFrame, constrains: dict):

    base_pop = deepcopy(base_pop_input)

    constrain_indices = []
    constrain_values = []
    for proc_constrain_key in constrains:

        if constrains[proc_constrain_key] is None:
            continue

        if proc_constrain_key == "index":
            proc_index = 0
        elif proc_constrain_key == "col":
            proc_index = 1

        constrain_indices.append([proc_index])
        constrain_values.append(constrains[proc_constrain_key])

    IPF = ipfn(
        base_pop.values,
        constrain_values,
        constrain_indices,
        convergence_rate=1e-6,
    )
    synthetic_population_matrix = IPF.iteration()

    base_pop = base_pop.astype(float)

    base_pop.values[:] = synthetic_population_matrix

    return base_pop


def postproc(
    pop_orig: DataFrame,
    pop_updated: DataFrame,
    colnames: list = ["sex", "ethnicity", "age"],
):

    pop_updated = pop_updated.stack().reset_index()
    pop_updated.columns = colnames + ["count"]
    pop_updated = pop_updated[pop_updated["count"] != 0]

    # Reset the index of the final DataFrame
    pop_updated = pop_updated.reset_index(drop=True)

    return merge(
        pop_orig,
        pop_updated,
        on=colnames,
        suffixes=("", "_adjusted"),
        how="outer",
    )
