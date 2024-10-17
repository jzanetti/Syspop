

from syspop.start import diary as syspop_diary
from syspop.python import input as syspop_input
from warnings import filterwarnings
filterwarnings("ignore")

output_dir = "/tmp/syspop_test17/Wellington_test_v2.0"

llm_diary_data = syspop_input.load_llm_diary()
syspop_diary(
    output_dir=output_dir,
    llm_diary_data=llm_diary_data,
    activities_cfg=None,
)
