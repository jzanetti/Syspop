# export PYTHONPATH=/home/zhangs/Github/Syspop


from syspop.python.input import new_zealand

from syspop.start import create as syspop_create

from warnings import filterwarnings
filterwarnings("ignore")

output_dir = "/tmp/syspop_test17/Wellington_test_v3.0"

nz_data = new_zealand()

syn_areas = list(
    nz_data["geography_hierarchy"][
        nz_data["geography_hierarchy"]["region"].isin(["Wellington"])
    ]["area"]
)

syn_areas = [241300, 241800]

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
    work={"employee": nz_data["work_employee"], "employer": nz_data["work_employer"], "income": nz_data["work_income"]},
    commute={
        "travel_to_work": nz_data["commute_travel_to_work"], 
        "travel_to_school": nz_data["commute_travel_to_school"]},
    education={
        "school": nz_data["school"],
        "kindergarten": nz_data["kindergarten"]
    },
    shared_space={
        "hospital": nz_data["hospital"],
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
