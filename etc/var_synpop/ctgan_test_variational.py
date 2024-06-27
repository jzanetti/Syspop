# https://colab.research.google.com/drive/1L6i-JhJK9ROG-KFcyzT9G-8FC3L8y8Lc?usp=sharing#scrollTo=swfU5fHCNQ8I
import matplotlib.pyplot as plt
from pandas import DataFrame as pandas_dataframe
from pandas import merge as pandas_merge

from syspop.process.var.validate import validate_num, validate_percentage
from syspop.process.var.var import var

output_cfg = {
    "variables": [
        "age",
        "ethnicity",
        "imms_status",
        "depression",
        "household_composition_code",
    ]
}

input_data = {
    "census": {
        "data": pandas_dataframe(
            {
                "age": [3, 1, 1, 2, 3, 2, 2, 4, 3],
                "ethnicity": [5, 5, 4, 4, 4, 5, 4, 4, 6],
                "num": [3, 5, 2, 4, 5, 6, 2, 3, 5],
            }
        ),
        "base": True,
        "percentage": None,
        "err": 1.0,
    },
    "imms_survey": {
        "data": pandas_dataframe(
            {
                "age": [3, 3, 3, 1, 1, 1],
                "ethnicity": [5, 5, 5, 4, 4, 4],
                "imms_status": [0, 1, 2, 0, 1, 2],
                "num": [0.05, 0.3, 0.65, 0.0, 0.75, 0.25],
            }
        ),
        "base": False,
        "percentage": "imms_status",
        "err": 1.0,
    },
    "household_composition_survey": {
        "data": pandas_dataframe(
            {
                "household_composition_code": [11, 11, 11, 12, 13],
                "age": [1, 2, 2, 1, 1],
                "depression": [3, 3, 4, 2, 2],
                "num": [0.3, 0.1, 0.15, 0.2, 0.1],
            }
        ),
        "base": False,
        "percentage": "household_composition_code",
        "err": 1.0,
    },
}


analysis_data = var(input_data, output_cfg, total_epoch=4500)

analysis = analysis_data["data"][-1]

print(analysis)

census_comparisons = validate_num(
    analysis, input_data["census"]["data"], ref_columns=["age", "ethnicity"]
)


imms_survey_comparisons = validate_percentage(
    analysis,
    input_data["imms_survey"]["data"],
    "imms_status",
    ref_columns=["age", "ethnicity", "imms_status"],
)

household_composition_survey_comparisons = validate_percentage(
    analysis,
    input_data["household_composition_survey"]["data"],
    "household_composition_code",
    ref_columns=["age", "depression", "household_composition_code"],
)

print(census_comparisons)
print(imms_survey_comparisons)
print(household_composition_survey_comparisons)

"""
import matplotlib.pyplot as plt

for i, proc_analysis in enumerate(analysis_data["data"]):

    proc_state = analysis_data["state"][i]
    proc_loss = analysis_data["epoch_loss"][0:i]

    census_comparisons = validate_num(
        proc_analysis, input_data["census"]["data"], ref_columns=["age", "ethnicity"]
    )

    imms_survey_comparisons = validate_percentage(
        proc_analysis,
        input_data["imms_survey"]["data"],
        "imms_status",
        ref_columns=["age", "ethnicity", "imms_status"],
    )

    household_composition_survey_comparisons = validate_percentage(
        proc_analysis,
        input_data["household_composition_survey"]["data"],
        "household_composition_code",
        ref_columns=["age", "depression", "household_composition_code"],
    )

    fig, ax = plt.subplots(figsize=(10, 5))
    plt.plot(census_comparisons["num_analysis"].values, label="Analysis")
    plt.plot(census_comparisons["num_truth"].values, label="Truth")

    plt.xlabel("Age/Ethnicity Label")
    plt.ylabel("Number of people")
    plt.legend()
    plt.ylim(0.0, 10.0)
    plt.title(f"Census, Comparison of analysis and truth\nIterations: {proc_state}")
    plt.savefig(f"test/census_comparisons_{proc_state}.png", bbox_inches="tight")
    plt.close()

    fig, ax = plt.subplots(figsize=(10, 5))
    plt.plot(imms_survey_comparisons["num_analysis"].values, label="Analysis")
    plt.plot(imms_survey_comparisons["num_truth"].values, label="Truth")

    plt.xlabel("Age/Ethnicity/Imms Label")
    plt.ylabel("Percentage (%)")
    plt.legend()
    plt.ylim(0.0, 1.0)
    plt.title(
        f"Immunisation, Comparison of analysis and truth\nIterations: {proc_state}"
    )
    plt.savefig(
        f"test/imms_survey_comparisons_{proc_state}.png",
        bbox_inches="tight",
    )
    plt.close()

    fig, ax = plt.subplots(figsize=(10, 5))
    plt.plot(
        household_composition_survey_comparisons["num_analysis"].values,
        label="Analysis",
    )
    plt.plot(
        household_composition_survey_comparisons["num_truth"].values, label="Truth"
    )

    plt.xlabel("Age/deprivation/household_composition Label")
    plt.ylabel("Percentage (%)")
    plt.legend()
    plt.ylim(0.0, 1.0)
    plt.title(
        f"Household composition, Comparison of analysis and truth\nIterations: {proc_state}"
    )
    plt.savefig(
        f"test/household_composition_survey_comparisons_{proc_state}.png",
        bbox_inches="tight",
    )
    plt.close()

    fig, ax = plt.subplots(figsize=(10, 5))
    plt.plot(proc_loss, label="Loss")

    plt.xlabel("iterations")
    plt.ylabel("Loss")
    plt.legend()
    plt.ylim(0.0, 25.0)
    plt.xlim(0, 30)
    plt.title(f"Comparison of analysis and truth\nIterations: {proc_state}")
    plt.savefig(f"test/loss_{proc_state}.png", bbox_inches="tight")
    plt.close()
"""
