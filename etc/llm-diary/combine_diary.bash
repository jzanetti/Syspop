#!/bin/bash

# --------------------
# Activate your Conda environment
# --------------------
CONDA_BASE=~/miniconda3
source $CONDA_BASE/bin/activate llm_abm-kscprod-data3


# --------------------
# Getting workdir:
#  e.g., workdir=/tmp/syspop_llm/run_20240325T00/
# --------------------
workdir=$1

if [ -z "$workdir" ]
then
  echo "Please enter the workdir:"
  read workdir
fi

# --------------------
# Run job
# --------------------
python combine_diary.py --workdir $workdir --group_name toddler --people_list toddler --age_list '1-5' --day_list weekday weekend --create_group_data
python combine_diary.py --workdir $workdir --group_name student --people_list student --age_list '6-18' --day_list weekday weekend --create_group_data
python combine_diary.py --workdir $workdir --group_name worker --people_list worker1 worker2 worker3 --age_list '18-65' --day_list weekday weekend --create_group_data
python combine_diary.py --workdir $workdir --group_name retiree --people_list retiree --age_list '65-99' --day_list weekday weekend --create_group_data
python combine_diary.py --workdir $workdir --group_name not_in_employment --people_list not_in_employment --age_list '18-64' --day_list weekday weekend --create_group_data --create_all_data

echo "Done"
