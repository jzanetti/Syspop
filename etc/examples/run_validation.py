

from syspop.python.input import new_zealand
from syspop.start import validate as syspop_validate
from warnings import filterwarnings
filterwarnings("ignore")


output_dir = "/tmp/syspop_test17/Wellington_test_v2.0"
# output_dir = "/tmp/syspop"
nz_data = new_zealand()

syspop_validate(
    output_dir=output_dir,
    household={"composition": nz_data["household_composition"]},
    work={"employee": nz_data["work_employee"], "employer": nz_data["work_employer"]},
    commute={
        "travel_to_work": nz_data["commute_travel_to_work"], 
        "travel_to_school": nz_data["commute_travel_to_school"]},
)