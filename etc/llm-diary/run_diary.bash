#!/bin/bash

# Activate your Conda environment
source activate llm_abm-kscprod-data3

# Define array of arguments for the Python script
peoples=("toddler" "student" "worker1" "worker2" "worker3" "retiree")
scenarios=100 # Example scenarios, add more as needed

# Counter to keep track of running processes
running=0

# Loop through day types
for people in "${peoples[@]}"; do

    echo "Start" $people
    # Run Python script with arguments
    nohup python create_diary.py --day_type weekday --scenarios $scenarios --people $people &> log_$people.weeday.log &
    nohup python create_diary.py --day_type weekend --scenarios $scenarios --people $people &> log_$people.weekend.log &

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
