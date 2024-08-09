import os

from pandas import read_parquet
from slurm.submit import submit

# pip install slurm_esr

os.chdir("/home/zhangs/Github/EpiModel_ESR/etc/submit/slurm")

# -----------------------------------------------------
# Set up the user information
# - workdir: where the output should sit
#       e.g., /home/zhangs/Github/Syspop/etc/route_model/agents_movement_output
# - input_dir: where is the input
# - area_ids: a list contains the SA2 ids
# - selected_people_each_batch: for each slurm job, how many agents we want to process
# - parallel_jobs: how many slurm jobs to run in parallel
# -----------------------------------------------------
WORKDIR = "/home/zhangs/Github/Syspop/etc/route_model/agents_movement_output"
INPUT_DIR = "/DSC/digital_twin/abm/synthetic_population/v3.0/Wellington"
AREA_IDS = [241800]
SELECTED_PEOPLE_EACH_BATCH = 50
PARALLEL_JOBS = 10

# ---------------------------------------------------
# Job starts here
# ---------------------------------------------------
input_data_dict = {
    "sypop_base_path": f"{INPUT_DIR}/syspop_base.parquet",
    "sypop_address_path": f"{INPUT_DIR}/syspop_location.parquet",
    "syspop_diaries_path": f"{INPUT_DIR}/syspop_diaries.parquet",
}

sypop_base = read_parquet(input_data_dict["sypop_base_path"])
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
        + f"--sypop_base_path {input_data_dict['sypop_base_path']} "
        + f"--sypop_address_path {input_data_dict['sypop_address_path']} "
        + f"--syspop_diaries_path {input_data_dict['syspop_diaries_path']} "
        + "--interp"
    )
    cmd_lists.append(cmd)
    job_index += 1

    # if job_index > 30:
    #    break

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
