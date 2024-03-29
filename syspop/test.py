# export PYTHONPATH=/home/zhangs/Github/Syspop/syspop

from pickle import load as pickle_load

from syspop import create as syspop_create
from syspop import diary as syspop_diary
from syspop import validate as syspop_validate
from syspop import vis as syspop_vis

with open("etc/data/test_data_latest/population.pickle", "rb") as fid:
    pop_data = pickle_load(fid)

with open("etc/data/test_data_latest/geography.pickle", "rb") as fid:
    geog_data = pickle_load(fid)

with open("etc/data/test_data_latest/household.pickle", "rb") as fid:
    household_data = pickle_load(fid)

with open("etc/data/test_data_latest/commute.pickle", "rb") as fid:
    commute_data = pickle_load(fid)

with open("etc/data/test_data_latest/work.pickle", "rb") as fid:
    work_data = pickle_load(fid)

with open("etc/data/test_data_latest/school.pickle", "rb") as fid:
    school_data = pickle_load(fid)

with open("etc/data/test_data_latest/hospital.pickle", "rb") as fid:
    hospital_data = pickle_load(fid)

with open("etc/data/test_data_latest/supermarket.pickle", "rb") as fid:
    supermarket_data = pickle_load(fid)

with open("etc/data/test_data_latest/department_store.pickle", "rb") as fid:
    department_store_data = pickle_load(fid)

with open("etc/data/test_data_latest/wholesale.pickle", "rb") as fid:
    wholesale_data = pickle_load(fid)

with open("etc/data/test_data_latest/restaurant.pickle", "rb") as fid:
    restaurant_data = pickle_load(fid)

with open("etc/data/test_data_latest/fast_food.pickle", "rb") as fid:
    fast_food_data = pickle_load(fid)

with open("etc/data/test_data_latest/cafe.pickle", "rb") as fid:
    cafe_data = pickle_load(fid)

with open("etc/data/test_data_latest/pub.pickle", "rb") as fid:
    pub_data = pickle_load(fid)

with open("etc/data/test_data_latest/park.pickle", "rb") as fid:
    park_data = pickle_load(fid)

with open("etc/data/test_data_latest/kindergarten.pickle", "rb") as fid:
    kindergarten_data = pickle_load(fid)

with open("etc/data/test_data_latest/llm_diary.pickle", "rb") as fid:
    llm_diary_data = pickle_load(fid)
# llm_diary_data = None

output_dir = "/tmp/syspop_test/test"
# syn_areas = list(
#    geog_data["hierarchy"][geog_data["hierarchy"]["region"] == "Wellington"]["area"]
# )
syn_areas = [135400, 111400, 110400]


if_run_syspop_create = False
if_run_diary = True
if_run_validation = False
if_run_vis = True


if if_run_syspop_create:
    syspop_create(
        syn_areas=syn_areas,
        output_dir=output_dir,
        pop_gender=pop_data["gender"],
        pop_ethnicity=pop_data["ethnicity"],
        geo_hierarchy=geog_data["hierarchy"],
        geo_location=geog_data["location"],
        geo_address=geog_data["address"],
        household=household_data["household"],
        socialeconomic=geog_data["socialeconomic"],
        work_data=work_data,
        home_to_work=commute_data["home_to_work"],
        school_data=school_data["school"],
        kindergarten_data=kindergarten_data["kindergarten"],
        hospital_data=hospital_data["hospital"],
        supermarket_data=supermarket_data["supermarket"],
        restaurant_data=restaurant_data["restaurant"],
        department_store_data=department_store_data["department_store"],
        wholesale_data=wholesale_data["wholesale"],
        fast_food_data=fast_food_data["fast_food"],
        pub_data=pub_data["pub"],
        park_data=park_data["park"],
        cafe_data=cafe_data["cafe"],
        assign_address_flag=True,
        rewrite_base_pop=True,
        use_parallel=True,
        ncpu=8,
    )

if if_run_diary:
    syspop_diary(
        output_dir=output_dir,
        llm_diary_data=llm_diary_data,
        activities_cfg=None,
        map_loc_flag=True,
    )

if if_run_validation:
    syspop_validate(
        output_dir=output_dir,
        pop_gender=pop_data["gender"],
        pop_ethnicity=pop_data["ethnicity"],
        household=household_data["household"],
        work_data=work_data,
        home_to_work=commute_data["home_to_work"],
    )

if if_run_vis:
    syspop_vis(
        output_dir=output_dir,
        plot_distribution=True,
        plot_travel=True,
        plot_location=True,
        plot_diary=True,
    )
