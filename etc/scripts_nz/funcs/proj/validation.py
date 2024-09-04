from os import makedirs
from os.path import exists, join
from pickle import load as pickle_load

import matplotlib.pyplot as plt
from numpy import NaN as numpy_nan
from numpy import array


def pop_validation(workdir: str, year_range: list = [2018, 2070]):
    """
    Validate and visualize population data over a range of years.

    This function loads population data from pickle files for each year in the specified range,
    calculates the total population and the population by gender (male and female), and generates
    bar charts to visualize the population data. The charts are saved in the 'vis' directory within
    the specified working directory.

    Parameters:
        workdir (str): The working directory where the population data and visualization directory are located.
        year_range (list): A list containing the start and end year for the range of years to process. Default is [2018, 2070].

    Returns:
        None
    """
    vis_dir = join(workdir, "proj", "vis")

    if not exists(vis_dir):
        makedirs(vis_dir)
    all_data = {
        "age": {},
        "gender": {"male": {}, "female": {}},
        "ethnicity": {
            "Maori": {},
            "Pacific": {},
            "Asian": {},
            "European and others": {},
        },
        "employee": {},
        "employer": {},
    }
    for proc_year in range(year_range[0], year_range[1]):
        try:
            proc_pop = pickle_load(
                open(join(workdir, "proj", str(proc_year), "population.pickle"), "rb")
            )
            proc_work = pickle_load(
                open(join(workdir, "proj", str(proc_year), "work.pickle"), "rb")
            )
        except FileNotFoundError:
            continue

        all_data["age"][proc_year] = proc_pop["age"].loc[:, 0:100].sum().sum()
        all_data["gender"]["male"][proc_year] = (
            proc_pop["gender"][proc_pop["gender"]["sex"] == "Male"]
            .loc[:, 0:100]
            .sum()
            .sum()
        )
        all_data["gender"]["female"][proc_year] = (
            proc_pop["gender"][proc_pop["gender"]["sex"] == "Female"]
            .loc[:, 0:100]
            .sum()
            .sum()
        )
        for proc_eth in ["Maori", "Pacific", "Asian", "European and others"]:
            try:
                all_data["ethnicity"][proc_eth][proc_year] = (
                    proc_pop["ethnicity"][
                        proc_pop["ethnicity"]["ethnicity"] == proc_eth
                    ]
                    .loc[:, 0:100]
                    .sum()
                    .sum()
                )
            except TypeError:
                all_data["ethnicity"][proc_eth][proc_year] = numpy_nan

        for proc_work_type in ["employee", "employer"]:
            all_data[proc_work_type][proc_year] = proc_work[proc_work_type][
                f"{proc_work_type}_number"
            ].sum()

    all_years = list(all_data["age"].keys())
    plt.figure(figsize=(10, 6))
    plt.bar(all_years, list(all_data["age"].values()), color="skyblue")
    plt.xlabel("Year")
    plt.ylabel("Population")
    plt.title("Population by Year")
    plt.xticks(all_years)  # Ensure all years are shown on the x-axis
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    plt.tight_layout()
    plt.savefig(join(vis_dir, "age.png"))
    plt.close()

    plt.figure(figsize=(10, 6))
    for i, proc_sex in enumerate(["male", "female"]):
        if i == 0:
            plt.bar(
                all_years,
                list(all_data["gender"][proc_sex].values()),
                # color="skyblue",
                label=proc_sex,
            )
        else:
            plt.bar(
                all_years,
                list(all_data["gender"][proc_sex].values()),
                bottom=prev_value,
                label=proc_sex,
            )

        prev_value = list(all_data["gender"][proc_sex].values())

    plt.xlabel("Year")
    plt.ylabel("Population")
    plt.title("Population by Year")
    plt.xticks(all_years)  # Ensure all years are shown on the x-axis
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    plt.tight_layout()
    plt.legend()
    plt.savefig(join(vis_dir, "gender.png"))
    plt.close()

    plt.figure(figsize=(10, 6))
    prev_value = 0
    for i, proc_eth in enumerate(["Maori", "Pacific", "Asian", "European and others"]):

        plt.bar(
            all_years,
            list(all_data["ethnicity"][proc_eth].values()),
            bottom=prev_value,
            label=proc_eth,
        )

        if i == 0:
            prev_value = array(list(all_data["ethnicity"][proc_eth].values()))
        else:
            prev_value += array(list(all_data["ethnicity"][proc_eth].values()))
    plt.xlabel("Year")
    plt.ylabel("Population")
    plt.title("Population by Year")
    plt.xticks(all_years)  # Ensure all years are shown on the x-axis
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    plt.tight_layout()
    plt.legend()
    plt.savefig(join(vis_dir, "ethnicity.png"))
    plt.close()

    for i, proc_work_type in enumerate(["employer", "employee"]):
        plt.figure(figsize=(10, 6))
        plt.bar(
            all_years,
            list(all_data[proc_work_type].values()),
            label=proc_work_type,
        )
        plt.xlabel("Year")
        plt.ylabel(proc_work_type)
        plt.title(f"{proc_work_type} by Year")
        plt.xticks(all_years)  # Ensure all years are shown on the x-axis
        plt.grid(axis="y", linestyle="--", alpha=0.7)
        plt.tight_layout()
        plt.legend()
        plt.savefig(join(vis_dir, f"{proc_work_type}.png"))
        plt.close()
