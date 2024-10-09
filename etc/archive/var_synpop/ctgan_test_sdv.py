# https://colab.research.google.com/drive/1L6i-JhJK9ROG-KFcyzT9G-8FC3L8y8Lc?usp=sharing#scrollTo=swfU5fHCNQ8I
import torch
from pandas import DataFrame as pandas_dataframe
from pandas import merge as pandas_merge

# from sdv.datasets.demo import download_demo
from sdv.metadata import MultiTableMetadata

base_census = {
    "age": [3, 1, 1, 2, 3, 2, 2, 4, 3, 3],
    "ethnicity": [5, 5, 4, 4, 4, 5, 4, 4, 6, 5],
    "mmr": [999, 999, 999, 999, 999, 999, 999, 999, 999, 999],
    "num": [3, 5, 2, 4, 5, 6, 2, 3, 5, 10],
}
mmr_survey = {"age": [3, 2], "mmr_ratio": [0.5, 0.75]}

base_census = pandas_dataframe(base_census)

total_people = base_census["num"].to_numpy().astype(float)

mmr_tensor = torch.tensor(
    base_census["mmr"].to_numpy().astype(float), requires_grad=True
)

analysis_field_tensor = torch.zeros(mmr_tensor.shape)

# ---------------------------
# Minimize mmr
# ---------------------------
# find age == 3
selected_mmr_tensor = mmr_tensor[base_census[base_census["age"] == 3].index]
selected_num = torch.tensor(
    base_census["num"][base_census[base_census["age"] == 3].index].values,
    dtype=torch.float64,
)
# find the immunisation rate for people with age == 3
obs_analysis_tensor = torch.sum(selected_mmr_tensor * selected_num) / torch.sum(
    selected_num
)
obs_target = 0.5

bg = analysis_field_tensor - mmr_tensor
bg = bg.view(-1, 1)
bg_transpose = bg.T

obs = obs_analysis_tensor - obs_target
obs = bg.view(-1, 1)
obs_transpose = obs.T

cost_func = torch.mm(bg_transpose, bg) + torch.mm(obs_transpose, obs)

total_people = {
    "age": [3, 1, 1, 2, 3, 2, 2, 4, 2, 2],
    "ethnicity": [5, 5, 4, 4, 4, 5, 4, 4, 6, 5],
    "address": [3, 3, 3, 3, 4, 4, 4, 4, 4, 3],
}

census_b = {
    "age": [2, 2, 1, 3, 1, 4, 2],
    "ethnicity": [5, 4, 4, 4, 4, 5, 5],
    "gender": [1, 1, 1, 0, 1, 0, 0],
    "immunization_rate": [3, 3, 3, 3, 1, 3, 1],
    "num": [3, 10, 5, 1, 4, 5, 6],
}


census_a = pandas_dataframe(census_a)
# census_a = census_a.reindex(census_a.index.repeat(census_a["num"]))
census_a = census_a.reset_index().reset_index()
census_a = census_a.rename(columns={"level_0": "id"})
census_a = census_a[["id", "age", "ethnicity", "address"]]

census_a["group_id"] = census_a.groupby(["age", "ethnicity", "address"]).ngroup()

census_b = pandas_dataframe(census_b)
census_b = census_b.reindex(census_b.index.repeat(census_b["num"]))
census_b = census_b[["age", "ethnicity", "gender", "immunization_rate"]]

census_b = pandas_merge(
    census_b,
    census_a[["age", "ethnicity", "group_id"]],
    on=["age", "ethnicity"],
    how="left",
)


census_b = census_b.dropna()
census_b["group_id"] = census_b["group_id"].astype(int)

x = 3
"""
census_a[["age", "ethnicity", "address"]] = census_a[
    ["age", "ethnicity", "address"]
].astype("category")

census_a["age_ethnicity_address_id"] = census_a["age_ethnicity_address_id"].astype("id")

census_b[["age", "ethnicity", "gender", "immunization_rate"]] = census_b[
    ["age", "ethnicity", "gender", "immunization_rate"]
].astype("category")
"""

# census_a = census_a.reindex(census_a.index.repeat(census_a["num"]))
# census_a = census_a.reset_index(drop=True, inplace=False)

# census_b = census_b.reindex(census_b.index.repeat(census_b["num"]))
# census_b = census_b.reset_index(drop=True, inplace=False)

# census_a = census_a[["age", "ethnicity", "address"]].reset_index()
# census_b = census_b[["age", "ethnicity", "gender", "immunization_rate"]].reset_index()

metadata = MultiTableMetadata()
data = {"census_a": census_a, "census_b": census_b}
metadata.detect_from_dataframes(data=data)

from sdv.multi_table import HMASynthesizer

synthesizer = HMASynthesizer(metadata)
synthesizer.fit(data)
synthetic_data = synthesizer.sample(scale=2)

ref_data = synthetic_data["census_a"]["a"]
