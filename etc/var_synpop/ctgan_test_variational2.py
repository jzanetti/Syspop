# https://colab.research.google.com/drive/1L6i-JhJK9ROG-KFcyzT9G-8FC3L8y8Lc?usp=sharing#scrollTo=swfU5fHCNQ8I
import matplotlib.pyplot as plt
import torch
from numpy import arange as numpy_arange
from pandas import DataFrame as pandas_dataframe
from pandas import merge as pandas_merge
from sklearn.preprocessing import StandardScaler
from torch.optim import SGD, Adam
from torchmetrics.regression import MeanSquaredError

base_census = {
    "age": [3, 1, 1, 2, 3, 2, 2, 4, 3, 3],
    "ethnicity": [5, 5, 4, 4, 4, 5, 4, 4, 6, 5],
    "num": [3, 5, 2, 4, 5, 6, 2, 3, 5, 10],
}
imms_survey = {
    "age": [3, 1, 2, 2],
    "ethnicity": [5, 5, 4, 6],
    "percentage": [0.6, 0.5, 0.75, 0.3],
}

base_census = pandas_dataframe(base_census)
imms_survey = pandas_dataframe(imms_survey)

num_max = base_census.num.max()
percentage_max = imms_survey.percentage.max()

base_census_err = 1.0
imms_survey_err = 1.0

# scaler_num = StandardScaler()
# scaler_percentage = StandardScaler()

# base_census["num"] = scaler_num.fit_transform(base_census[["num"]])
# imms_survey["percentage"] = scaler_percentage.fit_transform(imms_survey[["percentage"]])

base_census = base_census.groupby(["age", "ethnicity"]).sum().reset_index()

# ----------------------------------
# Step the unique combination of key features
# ----------------------------------
var_keys = {
    "age": list(
        set(list(base_census["age"].unique()) + list(imms_survey["age"].unique()))
    ),
    "ethnicity": list(
        set(
            list(base_census["ethnicity"].unique())
            + list(imms_survey["ethnicity"].unique())
        )
    ),
    "imms": [0, 1],
}
from itertools import product as itertools_product

combinations = list(itertools_product(*var_keys.values()))
analysis = pandas_dataframe(combinations, columns=var_keys.keys())
analysis["num"] = base_census.num.sum() / len(analysis)

# ----------------------------------
# Creating a mmr column in base_census
# ----------------------------------
# target_var = "mmr"
# base_census[target_var] = 1.0


# -----------------------------------
# Set up paramater model and loss function
# -----------------------------------
class LearnableParams(torch.nn.Module):
    """doesn't use data signals"""

    def __init__(self):
        super().__init__()
        self.learnable_params = torch.nn.Parameter(
            torch.tensor(analysis["num"].astype(float).values)
        )
        self.min_values = torch.tensor(0)
        self.max_values = torch.tensor(10)
        self.scaling_func = torch.nn.Sigmoid()

    def forward(self, data):
        return self.min_values + (
            self.max_values - self.min_values
        ) * self.scaling_func(data)


param_model = LearnableParams()

opt = SGD(
    filter(lambda p: p.requires_grad, param_model.parameters()),
    lr=0.1,
    differentiable=False,
)


opt = Adam(
    filter(lambda p: p.requires_grad, param_model.parameters()),
    lr=0.01,
    differentiable=False,
)  # loss_fn = NegativeCosineSimilarityLoss()

loss_func = MeanSquaredError()
total_epoch = 5000

data_to_process = {
    "base_census": {"data": base_census, "method": {"name": "num"}},
    "imms_survey": {"data": imms_survey, "method": {"name": "percentage", "keys": [1]}},
}


# -----------------------------------
# Set up variational cost function
# -----------------------------------
epoch = 0
epoch_loss_list = []
param_list = []

while epoch < total_epoch:

    param = next(iter(param_model.parameters()))
    # param = param_model.forward(param)
    cost_func_list = []
    for proc_data_name in data_to_process:

        proc_data = data_to_process[proc_data_name]["data"]
        proc_method = data_to_process[proc_data_name]["method"]

        for index, row in proc_data.iterrows():
            proc_conditions_target = []
            proc_conditions_analysis = []

            for i, proc_key in enumerate(row.index):
                if proc_key in ["num", "percentage"]:
                    continue
                proc_conditions_target.append(proc_data[proc_key] == row[proc_key])
                proc_conditions_analysis.append(analysis[proc_key] == row[proc_key])

            combined_conditions_target = [
                all(condition) for condition in zip(*proc_conditions_target)
            ]

            target_tensor = torch.tensor(
                proc_data[proc_method["name"]][combined_conditions_target].values,
                dtype=torch.float64,
            )

            if len(target_tensor) == 0:
                continue

            if proc_method["name"] == "num":
                combined_conditions_analysis = [
                    all(condition) for condition in zip(*proc_conditions_analysis)
                ]

                analysis_tensor = param[analysis[combined_conditions_analysis].index]
                x = torch.sum(analysis_tensor) - torch.sum(target_tensor)
                x = x / num_max
                x = x.view(-1, 1)
                xt = x.T
                cost_func_list.append(base_census_err * torch.mm(x, xt))

            elif data_to_process[proc_data_name]["method"]["name"] == "percentage":

                combined_conditions_analysis = [
                    all(condition) for condition in zip(*proc_conditions_analysis)
                ]

                for proc_key in data_to_process[proc_data_name]["method"]["keys"]:

                    combined_conditions_analysis_ref = [
                        all(condition)
                        for condition in zip(
                            *(proc_conditions_analysis + [analysis["imms"] == 1.0])
                        )
                    ]

                    analysis_tensor = param[
                        analysis[combined_conditions_analysis].index
                    ]

                    analysis_tensor_ref = param[
                        analysis[combined_conditions_analysis_ref].index
                    ]

                    x = torch.sum(analysis_tensor_ref) / torch.sum(
                        analysis_tensor
                    ) - torch.sum(target_tensor)
                    x = x / percentage_max
                    x = x.view(-1, 1)
                    xt = x.T
                    cost_func_list.append(imms_survey_err * torch.mm(x, xt))

    for i, proc_func in enumerate(cost_func_list):
        if i == 0:
            cost_func = proc_func
        else:
            cost_func += proc_func

    loss = loss_func(cost_func, torch.zeros(cost_func.shape))

    loss.backward()

    opt.step()

    opt.zero_grad(set_to_none=True)

    epoch_loss = loss.detach().item()

    for param in param_model.parameters():
        print(epoch, param, epoch_loss)

    if len(epoch_loss_list) > 1:
        if abs(epoch_loss - epoch_loss_list[-1]) < 1e-7:
            param_values = param.detach().numpy()
            break

    epoch_loss_list.append(epoch_loss)
    param_values = param.detach().numpy()

    # if epoch % 100 == 0:
    #    plt.bar(numpy_arange(len(param_values)), param_values)
    #    plt.savefig(f"test/param_{i}.png")
    #    plt.close()

    epoch += 1

analysis["num"] = param_values
# print(analysis)
analysis2 = (
    analysis[["age", "ethnicity", "num"]]
    .groupby(["age", "ethnicity"])
    .sum()
    .reset_index()
)
merged_df = pandas_merge(
    analysis2,
    base_census,
    on=["age", "ethnicity"],
    suffixes=("_analysis", "_base_census"),
)

print(merged_df)

grouped = analysis.groupby(["age", "ethnicity", "imms"])["num"].sum().unstack()
grouped["percentage"] = grouped[1] / (grouped[0] + grouped[1])
grouped_reset = grouped.reset_index()[["age", "ethnicity", "percentage"]]

merged_df2 = pandas_merge(
    grouped_reset,
    imms_survey,
    on=["age", "ethnicity"],
    suffixes=("_analysis", "_imms_survey"),
)
print(merged_df2)

plt.plot(epoch_loss_list)
plt.savefig("epoch_loss.png")
plt.close()

print(analysis)
x = 3
