# https://colab.research.google.com/drive/1L6i-JhJK9ROG-KFcyzT9G-8FC3L8y8Lc?usp=sharing#scrollTo=swfU5fHCNQ8I
import matplotlib.pyplot as plt
import torch
from numpy import arange as numpy_arange
from pandas import DataFrame as pandas_dataframe
from torch.optim import SGD
from torchmetrics.regression import MeanSquaredError

base_census = {
    "age": [3, 1, 1, 2, 3, 2, 2, 4, 3, 3],
    "ethnicity": [5, 5, 4, 4, 4, 5, 4, 4, 6, 5],
    "num": [3, 5, 2, 4, 5, 6, 2, 3, 5, 10],
}
mmr_survey = {
    "age": [3, 1, 2, 2],
    "ethnicity": [5, 5, 4, 6],
    "mmr": [0.6, 0.5, 0.75, 0.3],
}

base_census = pandas_dataframe(base_census)
mmr_survey = pandas_dataframe(mmr_survey)

# ----------------------------------
# Creating a mmr column in base_census
# ----------------------------------
target_var = "mmr"
base_census[target_var] = 1.0


# -----------------------------------
# Set up paramater model and loss function
# -----------------------------------
class LearnableParams(torch.nn.Module):
    """doesn't use data signals"""

    def __init__(self):
        super().__init__()
        self.learnable_params = torch.nn.Parameter(
            torch.tensor(base_census["mmr"].astype(float).values)
        )
        self.min_values = torch.tensor(0)
        self.max_values = torch.tensor(1)
        self.scaling_func = torch.nn.Sigmoid()

    def forward(self, scale_data):
        return self.min_values + (
            self.max_values - self.min_values
        ) * self.scaling_func(scale_data)


param_model = LearnableParams()

opt = SGD(
    filter(lambda p: p.requires_grad, param_model.parameters()),
    lr=0.01,
    differentiable=False,
)
loss_func = MeanSquaredError()
total_epoch = 5000

# -----------------------------------
# Set up variational cost function
# -----------------------------------

i = 0
epoch_loss_list = []
param_list = []
while i < total_epoch:

    param = next(iter(param_model.parameters()))
    param_values_all = param
    # param_values_all = param_model.forward(param)

    # ---------------------------
    # Get obs difference
    # ---------------------------
    obs_analysis_tensor = torch.zeros(1)
    obs_target = torch.zeros(1)
    total_target = 0
    for index, row in mmr_survey.iterrows():

        proc_conditions = []
        for i, proc_key in enumerate(row.index):
            if proc_key == target_var:  # e.g., mmr
                continue
            proc_conditions.append(base_census[proc_key] == row[proc_key])
        combined_conditions = [all(condition) for condition in zip(*proc_conditions)]

        selected_mmr_tensor = param_values_all[base_census[combined_conditions].index]
        if len(selected_mmr_tensor) == 0:
            continue

        selected_num_tensor = torch.tensor(
            base_census["num"][base_census[combined_conditions].index].values,
            dtype=torch.float64,
        )
        # find the immunisation rate for people with age == 3
        obs_analysis_tensor += torch.sum(
            selected_mmr_tensor * selected_num_tensor
        ) / torch.sum(selected_num_tensor)
        obs_target += row.mmr
        total_target += 1

    bg = param_values_all - param_values_all
    bg = bg.view(-1, 1)
    bg_transpose = bg.T

    obs = (obs_analysis_tensor - obs_target) / total_target
    obs = obs.view(-1, 1)
    obs_transpose = obs.T

    cost_func = torch.mm(bg_transpose, bg) + torch.mm(obs_transpose, obs)

    loss = loss_func(cost_func, torch.zeros(cost_func.shape))

    loss.backward()

    opt.step()

    opt.zero_grad(set_to_none=True)

    epoch_loss = loss.detach().item()

    for param in param_model.parameters():
        print(param, epoch_loss)

    epoch_loss_list.append(epoch_loss)
    param_values = param.detach().numpy()

    if i % 100 == 0:
        plt.bar(numpy_arange(len(param_values)), param_values)
        plt.savefig(f"test/param_{i}.png")
        plt.close()

    i += 1

plt.plot(epoch_loss_list)
plt.savefig("test/epoch_loss.png")
plt.close()


x = 3
