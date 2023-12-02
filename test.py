

# export PYTHONPATH=~/Github/Syspop/
from process.base_pop import create_base_pop
from pickle import load as pickle_load
from pickle import dump as pickle_dump

with open("/tmp/syspop/population.pickle", "rb") as fid:
    pop_data = pickle_load(fid)

with open("/tmp/syspop/geography.pickle", "rb") as fid:
    geog_data = pickle_load(fid)

synpop = create_base_pop(
    pop_data["gender"], 
    pop_data["ethnicity"], 
    list(geog_data["hierarchy"][geog_data["hierarchy"]["region"] == "Auckland"]["area"]),
    use_parallel=False)

print(synpop)

with open("/tmp/synpop.pickle", 'wb') as fid:
    pickle_dump({"synpop": synpop}, fid)