#!/bin/bash

# Activate your Conda environment
source activate llm_abm-kscprod-data3

# Define array of arguments for the Python script
week_types=("weekday" "weekend")
scenarios=300 # Example scenarios, add more as needed

formatted_time=$(date -u +'%Y%m%dT%H%M')

# Counter to keep track of running processes
running=0

workdir=/tmp/syspop_llm/run_$formatted_time

mkdir -p $workdir

# Loop through day types
for week_type in "${week_types[@]}"; do

    echo "Start" $week_type
    # Run Python script with arguments
    nohup python create_diary.py --workdir $workdir --day_type $week_type --scenarios $scenarios --people toddler &> $workdir/log_toddler.$week_type.log &
    nohup python create_diary.py --workdir $workdir --day_type $week_type --scenarios $scenarios --people student &> $workdir/log_student.$week_type.log &
    nohup python create_diary.py --workdir $workdir --day_type $week_type --scenarios $scenarios --people worker1 &> $workdir/log_worker1.$week_type.log &
    nohup python create_diary.py --workdir $workdir --day_type $week_type --scenarios $scenarios --people worker2 &> $workdir/log_worker2.$week_type.log &
    nohup python create_diary.py --workdir $workdir --day_type $week_type --scenarios $scenarios --people worker3 &> $workdir/log_worker3.$week_type.log &
    nohup python create_diary.py --workdir $workdir --day_type $week_type --scenarios $scenarios --people retiree &> $workdir/log_retiree.$week_type.log &
    nohup python create_diary.py --workdir $workdir --day_type $week_type --scenarios $scenarios --people not_in_employment &> $workdir/log_not_in_employment.$week_type.log &
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
