from copy import deepcopy
from itertools import product as itertools_product

from pandas import DataFrame as pandas_dataframe
from torch import float64 as torch_float64
from torch import mm as torch_mm
from torch import sum as torch_sum
from torch import tensor as torch_tensor
from torch import zeros as torch_zeros
from torch.nn import Module as torch_module
from torch.nn import Parameter as torch_parameters
from torch.optim import Adam
from torchmetrics.regression import MeanSquaredError


def check_data_structure(data: dict):
    """Check if the data structure matches the requirements

    Args:
        data (dict): Data to be checked

    Returns:
        rasie errors if necessary
    """
    base_data = []
    for data_name in data:
        proc_data = data[data_name]

        if proc_data["base"]:
            base_data.append(data_name)

        if "num" not in proc_data["data"].columns:
            raise Exception(f"Missing the column <num> in the dataset: {data_name}")

    if len(base_data) != 1:
        raise Exception(f"base dataset must exist and unique, found {base_data}")


def find_data_type(data: dict) -> dict:
    """Return a list contains the data_name for base and non-base dataset

    Args:
        data (dict): dataset to be processed

    Returns:
        dict: {"base": "x", "non_base": ["y","z",...]}
    """
    data_types = {"base": None, "non_base": []}
    for data_name in data:
        if data[data_name]["base"]:
            data_types["base"] = data_name
        else:
            data_types["non_base"].append(data_name)
    return data_types


def prep_data(input_data: dict, output_cfg: dict) -> dict:
    """Only keep the columns that is required by the output for the input_data.
        We check both the individual data, and household level data, e.g.,
        the data in the hhd_to_individual_conversion

    Args:
        input_data (dict): Input data
        output_data (dict): Output data (e.g., synthentic population)

    Returns:
        dict: updated input data
    """

    output_data_vars = deepcopy(output_cfg["variables"])
    output_data_vars.append("num")

    for data_name in input_data:
        proc_data_value = input_data[data_name]["data"]
        proc_data_column = [
            col for col in output_data_vars if col in proc_data_value.columns
        ]
        proc_data_control_ctl = deepcopy(proc_data_column)
        proc_data_control_ctl.remove("num")
        updated_data = (
            proc_data_value[proc_data_column]
            .groupby(proc_data_control_ctl)
            .sum()
            .reset_index()
        )
        input_data[data_name]["data"] = updated_data

    return input_data


def initial_analysis(
    input_data: dict, base_type_name: str, output_cfg: dict, use_constant: bool = False
) -> pandas_dataframe:
    """Only keep the columns that is required by the output for the input_data.
        We check both the individual data, and household level data, e.g.,
        the data in the hhd_to_individual_conversion

    Args:
        input_data (dict): Input data
        output_data (dict): Output data (e.g., synthentic population)

    Returns:
        dict: updated input data
    """
    output_keys = {}
    for proc_input_data_name in input_data:
        proc_input_data = input_data[proc_input_data_name]
        for proc_key in output_cfg["variables"]:
            if proc_key not in output_keys:
                output_keys[proc_key] = []

            if proc_key not in proc_input_data["data"]:
                continue
            output_keys[proc_key].extend(
                list(proc_input_data["data"][proc_key].unique())
            )

    output_keys_combinations = list(itertools_product(*output_keys.values()))
    output = pandas_dataframe(output_keys_combinations, columns=output_keys.keys())
    output = output.drop_duplicates().reset_index(drop=True)
    if use_constant:
        output["num"] = 0.01
    else:
        output["num"] = input_data[base_type_name]["data"].num.sum() / len(output)

    return output


class LearnableParams(torch_module):
    """doesn't use data signals"""

    def __init__(self, analysis: pandas_dataframe):
        super().__init__()
        self.learnable_params = torch_parameters(
            torch_tensor(analysis["num"].astype(float).values)
        )


def minimization(param_model) -> dict:
    """Set up minimization function

    Args:
        param_model (_type_): Parameter model

    Returns:
        dict: _description_
    """

    opt = Adam(
        filter(lambda p: p.requires_grad, param_model.parameters()),
        lr=0.01,
        differentiable=False,
    )  # loss_fn = NegativeCosineSimilarityLoss()

    loss_func = MeanSquaredError()

    return {"opt": opt, "loss_func": loss_func}


def optimization(
    input_data: dict,
    analysis: pandas_dataframe,
    total_epoch: int = 100,
    loss_cutoff: float = 1e-9,
    save_state_cfg: dict = {"start": 0, "end": 500, "interval": 5},
) -> pandas_dataframe:
    """Model optimization

    Args:
        input_data (dict): Input data to be processed
        analysis (pandas_dataframe): analysis data to be updated
        total_epoch (int, optional): total epoch. Defaults to 100.

    Returns:
        pandas_dataframe: Updated analysis
    """

    param_model = LearnableParams(analysis)
    minimization_model = minimization(param_model)

    epoch = 0
    all_epoch = []
    all_analysis = []
    all_states = []

    state_saving_epoches = range(
        save_state_cfg["start"], save_state_cfg["end"], save_state_cfg["interval"]
    )
    while epoch < total_epoch:

        param = next(iter(param_model.parameters()))

        cost_func_list = []
        for proc_data_name in input_data:

            proc_data = input_data[proc_data_name]["data"]
            proc_err = input_data[proc_data_name]["err"]
            proc_percentage = input_data[proc_data_name]["percentage"]

            if proc_percentage is not None:
                proc_data_max = 1.0
            else:
                proc_data_max = proc_data["num"].max()

            for _, row in proc_data.iterrows():
                proc_conditions_analysis_numerator = [
                    analysis[proc_key] == row[proc_key]
                    for proc_key in row.index
                    if proc_key != "num"
                ]

                combined_conditions_analysis_numerator = [
                    all(condition)
                    for condition in zip(*proc_conditions_analysis_numerator)
                ]

                if proc_percentage is not None:
                    proc_conditions_analysis_denominator = [
                        analysis[proc_key] == row[proc_key]
                        for proc_key in row.index
                        if proc_key not in ["num", proc_percentage]
                    ]

                    combined_conditions_analysis_denominator = [
                        all(condition)
                        for condition in zip(*proc_conditions_analysis_denominator)
                    ]

                    analysis_tensor_numerator = param[
                        analysis[combined_conditions_analysis_numerator].index
                    ]

                    analysis_tensor_denominator = param[
                        analysis[combined_conditions_analysis_denominator].index
                    ]

                    analysis_tensor = torch_sum(analysis_tensor_numerator) / torch_sum(
                        analysis_tensor_denominator
                    )
                else:
                    analysis_tensor = torch_sum(
                        param[analysis[combined_conditions_analysis_numerator].index]
                    )

                target_tensor = torch_tensor(
                    row.num,
                    dtype=torch_float64,
                )

                x = analysis_tensor - target_tensor

                x = x / proc_data_max
                x = x.view(-1, 1)
                xt = x.T
                cost_func_list.append(proc_err * torch_mm(x, xt))

        for i, proc_func in enumerate(cost_func_list):
            if i == 0:
                cost_func = proc_func
            else:
                cost_func += proc_func

        loss = minimization_model["loss_func"](cost_func, torch_zeros(cost_func.shape))

        loss.backward()

        minimization_model["opt"].step()

        minimization_model["opt"].zero_grad(set_to_none=True)

        epoch_loss = loss.detach().item()
        print(epoch, epoch_loss)

        param_values = param.detach().numpy()

        if epoch in state_saving_epoches:
            proc_analysis = deepcopy(analysis)
            proc_analysis["num"] = param_values
            all_analysis.append(proc_analysis)
            all_states.append(epoch)
            all_epoch.append(epoch_loss)

        if len(all_epoch) > 1:
            if abs(all_epoch[-1] - all_epoch[-2]) < loss_cutoff:
                param_values = param.detach().numpy()
                break

        epoch += 1

    all_analysis.append(proc_analysis)
    all_states.append(epoch)
    all_epoch.append(epoch_loss)

    return {"data": all_analysis, "state": all_states, "epoch_loss": all_epoch}


def var(
    input_data: dict, output_cfg: dict, total_epoch: int = 1000
) -> pandas_dataframe:
    """Variational optimization

    Args:
        input_data (dict): Data list to be optimized, e.g.,
        {
            "census": {"data": <dataframe>, "controls": ["ethnicity", "age"], "err": 0.1, ...},
            "imms": {"data": <dataframe>, "controls": ["ethnicity", "age", "imms_status"], "err": 1.0, ...},
            "household_composition": {"data": <dataframe>, "controls": ["age", "household_composition"], "err": 1.0, ...}
        }
        output_cfg (dict): output data configuration

    Returns:
        DataFrame: A combined data sheet representing synthentic population
    """
    check_data_structure(input_data)
    data_types = find_data_type(input_data)

    input_data = prep_data(input_data, output_cfg)
    # input_data = num2percentage(input_data, data_types)
    output_data = initial_analysis(
        input_data, data_types["base"], output_cfg, use_constant=True
    )
    return optimization(input_data, output_data, total_epoch=total_epoch)
