

# export PYTHONPATH=~/Github/Syspop/
from process.base_pop import base_pop_wrapper
from process.utils import setup_logging
from process.household import household_wrapper
from process.social_economic import social_economic_wrapper
from process.work import work_and_commute_wrapper
from process.school import school_wrapper
from process.hospital import hospital_wrapper
from process.shared_space import shared_space_wrapper
from pickle import load as pickle_load
from pickle import dump as pickle_dump
from os.path import exists, join
from os import makedirs

with open("/tmp/syspop/population.pickle", "rb") as fid:
    pop_data = pickle_load(fid)

with open("/tmp/syspop/geography.pickle", "rb") as fid:
    geog_data = pickle_load(fid)

with open("/tmp/syspop/household.pickle", "rb") as fid:
    household_data = pickle_load(fid)

with open("/tmp/syspop/commute.pickle", "rb") as fid:
    commute_data = pickle_load(fid)

with open("/tmp/syspop/work.pickle", "rb") as fid:
    work_data = pickle_load(fid)

with open("/tmp/syspop/school.pickle", "rb") as fid:
    school_data = pickle_load(fid)

with open("/tmp/syspop/hospital.pickle", "rb") as fid:
    hospital_data = pickle_load(fid)

with open("/tmp/syspop/supermarket.pickle", "rb") as fid:
    supermarket_data = pickle_load(fid)

with open("/tmp/syspop/restaurant.pickle", "rb") as fid:
    restaurant_data = pickle_load(fid)

output_data_dir = "/tmp/syspop/output"

if not exists(output_data_dir):
    makedirs(output_data_dir)

output_syn_pop_path = join(output_data_dir, "syspop_base.csv")
output_loc_path = join(output_data_dir, "syspop_location.csv")

logger = setup_logging()

create_base_pop_flag = False
assign_household_flag = True
assign_socialeconomic_flag = False
assign_commute_flag = False
assign_work_flag = False
assign_school_flag = False
assign_hospital_flag = False
assign_supermarket_flag = False
assign_restaurant_flag = False
assign_address_flag = False
create_output = False


if create_base_pop_flag:
    synpop = base_pop_wrapper(
        pop_data["gender"], 
        pop_data["ethnicity"],
        list(geog_data["hierarchy"][geog_data["hierarchy"]["region"] == "Auckland"]["area"]),
        use_parallel=True,
        n_cpu=8)

    with open(join(output_data_dir, "synpop.pickle"), 'wb') as fid:
        pickle_dump({"synpop": synpop}, fid)

if assign_household_flag:
    with open(join(output_data_dir, "synpop.pickle"), "rb") as fid:
        base_pop = pickle_load(fid)

    base_pop = household_wrapper(
        household_data["household"], 
        base_pop["synpop"], 
        geog_data["address"],
        use_parallel=True, 
        n_cpu=16)

    with open(join(output_data_dir, "synpop.pickle"), 'wb') as fid:
        pickle_dump({"synpop": base_pop}, fid)

if assign_socialeconomic_flag:
    with open(join(output_data_dir, "synpop.pickle"), "rb") as fid:
        base_pop = pickle_load(fid)

    base_pop = social_economic_wrapper(
        base_pop["synpop"], 
        geog_data["socialeconomic"])

    with open(join(output_data_dir, "synpop.pickle"), 'wb') as fid:
        pickle_dump({"synpop": base_pop}, fid)

if assign_work_flag:
    with open(join(output_data_dir, "synpop.pickle"), "rb") as fid:
        base_pop = pickle_load(fid)

    base_pop = work_and_commute_wrapper(
        work_data,
        base_pop["synpop"], 
        commute_data["home_to_work"],
        geog_data["hierarchy"],
        use_parallel=True)

    with open(join(output_data_dir, "synpop.pickle"), 'wb') as fid:
        pickle_dump({"synpop": base_pop}, fid)

if assign_school_flag:
    with open(join(output_data_dir, "synpop.pickle"), "rb") as fid:
        base_pop = pickle_load(fid)

    base_pop = school_wrapper(
        school_data["school"],
        base_pop["synpop"], 
        geog_data["hierarchy"])

    with open(join(output_data_dir, "synpop.pickle"), 'wb') as fid:
        pickle_dump({"synpop": base_pop}, fid)

if assign_hospital_flag:
    with open(join(output_data_dir, "synpop.pickle"), "rb") as fid:
        base_pop = pickle_load(fid)

    base_pop = hospital_wrapper(
        hospital_data["hospital"],
        base_pop["synpop"],
        geog_data["location"])

    with open(join(output_data_dir, "synpop.pickle"), 'wb') as fid:
        pickle_dump({"synpop": base_pop}, fid)

if assign_supermarket_flag:
    with open(join(output_data_dir, "synpop.pickle"), "rb") as fid:
        base_pop = pickle_load(fid)

    base_pop = shared_space_wrapper(
        "supermarket",
        supermarket_data["supermarket"], 
        base_pop["synpop"], 
        geog_data["location"],
        num_nearest=2)

    with open(join(output_data_dir, "synpop.pickle"), 'wb') as fid:
        pickle_dump({"synpop": base_pop}, fid)

if assign_restaurant_flag:
    with open(join(output_data_dir, "synpop.pickle"), "rb") as fid:
        base_pop = pickle_load(fid)

    base_pop = shared_space_wrapper(
        "restaurant",
        restaurant_data["restaurant"], 
        base_pop["synpop"], 
        geog_data["location"],
        num_nearest=4)

    with open(join(output_data_dir, "synpop.pickle"), 'wb') as fid:
        pickle_dump({"synpop": base_pop}, fid)


if assign_address_flag:
    with open(join(output_data_dir, "synpop.pickle"), "rb") as fid:
        base_pop = pickle_load(fid)

    base_pop = add_address_wrapper(
        base_pop["synpop"], 
        geog_data["address"],
        use_parallel=True)

    with open(join(output_data_dir, "synpop.pickle"), 'wb') as fid:
        pickle_dump({"synpop": base_pop}, fid)


if create_output:
    with open(join(output_data_dir, "synpop.pickle"), "rb") as fid:
        synpop_data = pickle_load(fid)
    synpop_data["synpop"].to_csv(output_syn_pop_path)