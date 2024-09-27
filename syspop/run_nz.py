# export PYTHONPATH=/home/zhangs/Github/Syspop

from os.path import join

from process.utils import _get_data_for_test

from syspop import create as syspop_create
from syspop import diary as syspop_diary
from syspop import validate as syspop_validate
from syspop import vis as syspop_vis

from warnings import filterwarnings
filterwarnings("ignore")

proj_year = None  # can be None or an actual year, e.g., None or 2028

output_dir = "/tmp/syspop_test17/Wellington_test"
input_dir = "etc/data/test_data_wellington_latest"
if proj_year is None:
    output_dir = join(output_dir, "base")
else:
    input_dir = join(input_dir, "proj", str(proj_year))
    output_dir = join(output_dir, str(proj_year))

test_data = _get_data_for_test(input_dir)

syn_areas = list(
    test_data["geog_data"]["hierarchy"][
        test_data["geog_data"]["hierarchy"]["region"].isin(["Wellington"])
    ]["area"]
)

# syn_areas = [241300, 241200, 243000, 247700, 242400]
#from random import sample as random_sample
#syn_areas = random_sample(syn_areas, 50)
# syn_areas = list(test_data["geog_data"]["hierarchy"]["area"].unique())
# syn_areas = [241200]

if_run_syspop_create = True
if_run_diary = False
if_run_validation = True
if_run_vis = False

if if_run_syspop_create:
    syspop_create(
        syn_areas=syn_areas,
        output_dir=output_dir,
        pop_gender=test_data["pop_data"]["gender"],
        pop_ethnicity=test_data["pop_data"]["ethnicity"],
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

if if_run_diary:
    syspop_diary(
        output_dir=output_dir,
        llm_diary_data=test_data["llm_diary_data"],
        activities_cfg=None,
    )

if if_run_validation:
    syspop_validate(
        output_dir=output_dir,
        pop_gender=test_data["pop_data"]["gender"],
        pop_ethnicity=test_data["pop_data"]["ethnicity"],
        household=test_data["household_data"]["household"],
        work_data=test_data["work_data"],
        home_to_work=test_data["commute_data"]["travel_to_work"],
        # mmr_data=test_data["others"]["mmr"],
        # data_years={"vaccine": 2023},
    )

if if_run_vis:
    syspop_vis(
        output_dir=output_dir,
        plot_distribution=True,
        plot_travel=True,
        plot_location=True,
        plot_diary=True,
    )
