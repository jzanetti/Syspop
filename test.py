

# export PYTHONPATH=~/Github/Syspop/
from process.base_pop import create_base_pop
from process.utils import setup_logging
from process.household import household_wrapper
from process.social_economic import assign_social_economic
from pickle import load as pickle_load
from pickle import dump as pickle_dump

with open("/tmp/syspop/population.pickle", "rb") as fid:
    pop_data = pickle_load(fid)

with open("/tmp/syspop/geography.pickle", "rb") as fid:
    geog_data = pickle_load(fid)

with open("/tmp/syspop/household.pickle", "rb") as fid:
    household_data = pickle_load(fid)

logger = setup_logging()

create_base_pop_flag = False
assign_household_flag = False
assign_socialeconomic_flag = True

if create_base_pop_flag:
    synpop = create_base_pop(
        pop_data["gender"], 
        pop_data["ethnicity"],
        list(geog_data["hierarchy"][geog_data["hierarchy"]["region"] == "Auckland"]["area"]),
        use_parallel=False,
        n_cpu=8)

    with open("/tmp/synpop.pickle", 'wb') as fid:
        pickle_dump({"synpop": synpop}, fid)


if assign_household_flag:
    with open("/tmp/synpop.pickle", "rb") as fid:
        base_pop = pickle_load(fid)

    base_pop = household_wrapper(household_data["household"], base_pop["synpop"], use_parallel=True, n_cpu=16)

    with open("/tmp/synpop.pickle", 'wb') as fid:
        pickle_dump({"synpop": base_pop}, fid)

if assign_socialeconomic_flag:
    with open("/tmp/synpop.pickle", "rb") as fid:
        base_pop = pickle_load(fid)

    base_pop = assign_social_economic(base_pop["synpop"], geog_data["socialeconomic"])

    with open("/tmp/synpop.pickle", 'wb') as fid:
        pickle_dump({"synpop": base_pop}, fid)