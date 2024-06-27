# export PYTHONPATH=/home/zhangs/Github/Syspop/syspop

from process.utils import _get_data_for_test

from syspop import create as syspop_create
from syspop import diary as syspop_diary
from syspop import validate as syspop_validate
from syspop import vis as syspop_vis

data_year = 2023  # can be None or an actual year
data_percentile = "median"
# output_dir = f"/tmp/syspop_test11/Auckland"
output_dir = "/DSC/digital_twin/abm/PHA_report_202405/syspop/NZ"
if data_year is not None:
    output_dir = f"{output_dir}/{data_year}"

if data_percentile is not None:
    output_dir += f"/{data_percentile}"

test_data = _get_data_for_test("etc/data/test_data_latest")

# syn_areas = list(
#    test_data["geog_data"]["hierarchy"][
#        test_data["geog_data"]["hierarchy"]["region"].isin(["Auckland"])
#    ]["area"]
# )

# syn_areas = [135400, 111400, 110400]
syn_areas = list(test_data["geog_data"]["hierarchy"]["area"].unique())

if_run_syspop_create = False
if_run_diary = False
if_run_validation = True
if_run_vis = True

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
        socialeconomic=test_data["geog_data"]["socialeconomic"],
        work_data=test_data["work_data"],
        home_to_work=test_data["commute_data"]["home_to_work"],
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
        mmr_data=test_data["others"]["mmr"],
        birthplace_data=test_data["others"]["birthplace"],
        assign_address_flag=True,
        rewrite_base_pop=True,
        use_parallel=True,
        ncpu=8,
        data_year=data_year,
        data_percentile=data_percentile,
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
        home_to_work=test_data["commute_data"]["home_to_work"],
        mmr_data=test_data["others"]["mmr"],
        data_year=data_year,
        data_percentile=data_percentile,
    )

if if_run_vis:
    syspop_vis(
        output_dir=output_dir,
        plot_distribution=True,
        plot_travel=True,
        plot_location=True,
        plot_diary=True,
    )
