# export PYTHONPATH=~/Github/Syspop/etc/scripts_nz
from argparse import ArgumentParser
from os import makedirs
from os.path import exists, join
from pickle import dump as pickle_dump

from pandas import DataFrame, read_csv

workdir = "/tmp/syspop/tonga"

AREA_NAME = "Tongatapu"
AREA_ID = {"Tongatapu": 12345}
TOTAL_HOUSEHOLDS = {"Tongatapu": 13368}


if not exists(workdir):
    makedirs(workdir)

# ---------------------------------
# Creating population
# ---------------------------------
age_data = read_csv("etc/data/raw_tonga_latest/population_by_age-2019.csv")
age_data["area"] = AREA_ID[AREA_NAME]
ethnicity_data = read_csv("etc/data/raw_tonga_latest/population_by_ethnicity-2022.csv")
ethnicity_data["area"] = AREA_ID[AREA_NAME]
gender_data = read_csv("etc/data/raw_tonga_latest/population_by_gender-2022.csv")
gender_data["area"] = AREA_ID[AREA_NAME]
with open(join(workdir, "population.pickle"), "wb") as fid:
    pickle_dump(
        {"age": age_data, "gender": gender_data, "ethnicity": ethnicity_data},
        fid,
    )


# ---------------------------------
# Creating household
# ---------------------------------
base_pop = {
    "child_num": age_data.drop("area", axis=1).iloc[:, 0:19].sum().sum(),
    "adult_num": age_data.drop("area", axis=1).iloc[:, 19:].sum().sum(),
}

household_data = read_csv("etc/data/raw_tonga_latest/household_composition.csv")[
    ["child_num", "adult_num", "percentage"]
]
household_data["num"] = household_data["percentage"] * TOTAL_HOUSEHOLDS[AREA_NAME]
household_data["num"] = household_data["num"].astype(float)

for scaler_type in ["child_num", "adult_num"]:
    hhd_num = (household_data["num"] * household_data[scaler_type]).sum()

    hhd_scaler = base_pop[scaler_type] / hhd_num
    household_data[scaler_type] *= hhd_scaler
    household_data[scaler_type] = round(household_data[scaler_type]).astype(int)

household_data = household_data[
    (household_data["child_num"] > 0) | (household_data["adult_num"] > 0)
]

hhd_scaler = TOTAL_HOUSEHOLDS[AREA_NAME] / household_data["num"].sum()
household_data["num"] *= hhd_scaler

household_data["people_num"] = household_data["child_num"] + household_data["adult_num"]
household_data["adults_num"] = household_data["adult_num"]
household_data["dwelling_type"] = 9999
household_data["household_num"] = round(household_data["num"]).astype(int)
household_data["area"] = AREA_ID[AREA_NAME]
household_data = household_data[
    ["area", "people_num", "adults_num", "dwelling_type", "household_num"]
]
with open(join(workdir, "household.pickle"), "wb") as fid:
    pickle_dump(
        {"household": household_data},
        fid,
    )

# ---------------------------------
# Creating geography
# ---------------------------------
geo_hierarchy_data = {
    "region": [AREA_ID[AREA_NAME]],
    "super_area": [AREA_ID[AREA_NAME]],
    "area": [AREA_ID[AREA_NAME]],
    "super_area_name": [AREA_NAME],
}
geo_hierarchy_data = DataFrame(geo_hierarchy_data)

geo_location_data = {
    "area": [AREA_ID[AREA_NAME]],
    "latitude": [-21.1466],
    "longitude": [-175.2515],
}
geo_location_data = DataFrame(geo_location_data)

geo_address_data = read_csv("etc/data/raw_tonga_latest/building_yes.csv")

geo_address_data["area"] = AREA_ID[AREA_NAME]

geo_address_data = geo_address_data[["area", "lat", "lon"]]

geo_address_data = geo_address_data.rename(
    columns={"lat": "latitude", "lon": "longitude"}
)

socialeconomic_data = {"area": [AREA_ID[AREA_NAME]], "socioeconomic_centile": 9999}
socialeconomic_data = DataFrame(socialeconomic_data)

with open(join(workdir, "geography.pickle"), "wb") as fid:
    pickle_dump(
        {
            "hierarchy": geo_hierarchy_data,
            "location": geo_location_data,
            "address": geo_address_data,
            "socialeconomic": socialeconomic_data,
        },
        fid,
    )

print("Job done ...")
