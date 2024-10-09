

from syspop.python.input import new_zealand
from syspop.start import validate as syspop_validate
from warnings import filterwarnings
filterwarnings("ignore")


output_dir = "/tmp/syspop_test17/Wellington_test_v2.0"
test_data = new_zealand()

syspop_validate(
    output_dir=output_dir,
    # pop_gender=test_data["pop_data"]["gender"], # only avaliable for v1.0 data
    # pop_ethnicity=test_data["pop_data"]["ethnicity"], # only avaliable for v1.0 data
    household=test_data["household_data"]["household"],
    work_data=test_data["work_data"],
    home_to_work=test_data["commute_data"]["travel_to_work"],
    # mmr_data=test_data["others"]["mmr"],
    # data_years={"vaccine": 2023},
)