# export PYTHONPATH=/home/zhangs/Github/Syspop


from syspop.python.input import new_zealand

from syspop.start import create as syspop_create

from warnings import filterwarnings
filterwarnings("ignore")

proj_year = None  # can be None or an actual year, e.g., None or 2028

output_dir = "/tmp/syspop_test17/Wellington_test_v2.0"

test_data = new_zealand()

syn_areas = list(
    test_data["geog_data"]["hierarchy"][
        test_data["geog_data"]["hierarchy"]["region"].isin(["Wellington"])
    ]["area"]
)

# syn_areas = [241300, 241200, 243000, 247700, 242400]
#from random import sample as random_sample
#syn_areas = random_sample(syn_areas, 50)
# syn_areas = list(test_data["geog_data"]["hierarchy"]["area"].unique())
syn_areas = [236800, 237200]

syspop_create(
    syn_areas=syn_areas,
    output_dir=output_dir,
    pop_structure = test_data["pop_data"]["population_structure"],
    #pop_gender=test_data["pop_data"]["gender"],
    #pop_ethnicity=test_data["pop_data"]["ethnicity"],
    geo_hierarchy=test_data["geog_data"]["hierarchy"],
    geo_location=test_data["geog_data"]["location"],
    geo_address=test_data["geog_data"]["address"],
    household=test_data["household_data"]["household"],
    socialeconomic=test_data["pop_data"]["deprivation"],
    work_data=test_data["work_data"],
    home_to_work=test_data["commute_data"]["travel_to_work"],
    school_data=test_data["school_data"]["school"],
    kindergarten_data=test_data["kindergarten_data"]["kindergarten"],
    hospital_data=test_data["hospital_data"]["hospital"],
    supermarket_data=test_data["supermarket_data"]["supermarket"],
    restaurant_data=test_data["restaurant_data"]["restaurant"],
    department_store_data=test_data["department_store_data"]["department_store"],
    wholesale_data=test_data["wholesale_data"]["wholesale"],
    fast_food_data=test_data["fast_food_data"]["fast_food"],
    pub_data=test_data["pub_data"]["pub"],
    park_data=test_data["park_data"]["park"],
    cafe_data=test_data["cafe_data"]["cafe"],
    #mmr_data=test_data["others"]["mmr"],
    birthplace_data=test_data["others"]["birthplace"],
    assign_address_flag=True,
    rewrite_base_pop=True,
    # data_years={"vaccine": 2023},
)