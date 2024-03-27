#!/bin/bash

# Activate your Conda environment
source activate llm_abm-kscprod-data3

# Define array of arguments for the Python script
peoples=("toddler" "student" "worker1" "worker2" "worker3" "retiree" "not_in_employment")
scenarios=300 # Example scenarios, add more as needed

formatted_time=$(date -u +'%Y%m%dT%H%M')

# Counter to keep track of running processes
running=0

workdir=/tmp/syspop_llm/run_$formatted_time

mkdir -p $workdir

# Loop through day types
for people in "${peoples[@]}"; do

    echo "Start" $people
    # Run Python script with arguments
    nohup python create_diary.py --workdir $workdir --day_type weekday --scenarios $scenarios --people $people &> $workdir/log_$people.weeday.log &
    nohup python create_diary.py --workdir $workdir --day_type weekend --scenarios $scenarios --people $people &> $workdir/log_$people.weekend.log &

    # Increment running counter
    ((running++))

    # If number of running processes reaches 3, wait for them to finish
    if [ $running -eq 1 ]; then
        wait
        # Reset running counter
        running=0
    fi

    echo "Finish" $people
done

# Wait for remaining processes to finish
wait

# Deactivate the Conda environment
conda deactivate
