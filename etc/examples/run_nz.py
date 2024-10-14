# export PYTHONPATH=/home/zhangs/Github/Syspop


from syspop.python.input import new_zealand

from syspop.start import create as syspop_create

from warnings import filterwarnings
filterwarnings("ignore")

proj_year = None  # can be None or an actual year, e.g., None or 2028

output_dir = "/tmp/syspop_test17/Wellington_test_v2.0"

nz_data = new_zealand()

syn_areas = list(
    nz_data["geography_hierarchy"][
        nz_data["geography_hierarchy"]["region"].isin(["Wellington"])
    ]["area"]
)

syn_areas = [241300, 241200, 243000, 247700, 242400]
#from random import sample as random_sample
#syn_areas = random_sample(syn_areas, 50)
# syn_areas = list(test_data["geog_data"]["hierarchy"]["area"].unique())
# syn_areas = [236800, 237200]
syn_areas = [241800]

syspop_create(
    syn_areas,
    output_dir,
    population = {
        "structure": nz_data["population_structure"]
    },
    geography = {
        "hierarchy": nz_data["geography_hierarchy"],
        "location": nz_data["geography_location"],
        "address": nz_data["geography_address"]
    },
    household={"composition": nz_data["household_composition"]},
    work={"employee": nz_data["work_employee"], "employer": nz_data["work_employer"]},
    commute={"travel_to_work": nz_data["commute_travel_to_work"]},
    education={
        "school": nz_data["school"],
        "kindergarten": nz_data["kindergarten"]
    },
    healthcare={
        "hospital": nz_data["hospital"]
    },
    shared_space={
        "bakery": nz_data["shared_space_bakery"],
        "cafe": nz_data["shared_space_cafe"],
        "department_store": nz_data["shared_space_department_store"],
        "fast_food": nz_data["shared_space_fast_food"],
        "park": nz_data["shared_space_park"],
        "pub": nz_data["shared_space_pub"],
        "restaurant": nz_data["shared_space_restaurant"],
        "supermarket": nz_data["shared_space_supermarket"],
        "wholesale": nz_data["shared_space_wholesale"],

    }
)