import os
from random import randint

from pandas import read_parquet
from slurm.submit import submit

os.chdir("/home/zhangs/Github/EpiModel_ESR/etc/submit/slurm")

WORKDIR = "/home/zhangs/Github/Syspop/etc/route_model/agents_movement_output"
INPUT_DATA_PATH = {
    "sypop_base_path": "/DSC/digital_twin/abm/PHA_report_202405/syspop/NZ/2023/median/syspop_base.parquet",
    "sypop_address_path": "/DSC/digital_twin/abm/PHA_report_202405/syspop/NZ/2023/median/syspop_location.parquet",
    "syspop_diaries_path": "/DSC/digital_twin/abm/PHA_report_202405/syspop/NZ/2023/median/syspop_diaries.parquet",
}
AREA_IDS = [251300, 251400, 251600, 251700, 251800]
SELECTED_PEOPLE_EACH_BATCH = 50
PARALLEL_JOBS = 10

sypop_base = read_parquet(INPUT_DATA_PATH["sypop_base_path"])
sypop_base = sypop_base[(sypop_base["area"].isin(AREA_IDS))]
all_ids = list(sypop_base["id"].unique())

cmd_lists = []
job_index = 0
for i in range(0, len(all_ids), SELECTED_PEOPLE_EACH_BATCH):
    selected_ids = " ".join(map(str, all_ids[i : i + SELECTED_PEOPLE_EACH_BATCH]))
    selected_areas = " ".join(str(item) for item in AREA_IDS)

    cmd = (
        f"python /home/zhangs/Github/Syspop/etc/route_model/create_routes.py "
        + f"--workdir {WORKDIR} "
        + f"--area_ids {selected_areas} "
        + f"--people_ids {selected_ids} "
        + f"--sypop_base_path {INPUT_DATA_PATH['sypop_base_path']} "
        + f"--sypop_address_path {INPUT_DATA_PATH['sypop_address_path']} "
        + f"--syspop_diaries_path {INPUT_DATA_PATH['syspop_diaries_path']}"
    )
    cmd_lists.append(cmd)
    job_index += 1

    if job_index > 30:
        break

submit(
    job_name="syspop_routing",
    job_list=cmd_lists,
    python_path="/home/zhangs/Github/Syspop",
    conda_env="syspop",
    total_jobs=PARALLEL_JOBS,
    memory_per_node=8000,
    job_priority="default",
    partition="prod",
    workdir=f"/home/zhangs/Github/Syspop/etc/route_model/slurm_jobs",
    debug=False,
)
