

from syspop import diary as syspop_diary
from python.input import new_zealand
from warnings import filterwarnings
filterwarnings("ignore")

output_dir = "/tmp/syspop_test17/Wellington_test_v2.0"

test_data = new_zealand()
syspop_diary(
    output_dir=output_dir,
    llm_diary_data=test_data["llm_diary_data"],
    activities_cfg=None,
)
