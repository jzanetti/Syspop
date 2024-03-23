


# How to install:
```
make install_env
make install_llama
```

# How to start the environment:
```
conda activate syspop_llm
```

# Run the job:
### Run a single job
```
python create_diary.py --day_type <DAY_TYPE> --scenarios <NUMBER OF SCENARIOS> --people <PEOPLE_TYPE> [--workdir <WORKDIR> --model_path <LLAMA2 MODEL PATH>]
```

### Run multiple jobs together
```
make run_diary
```

# Appendix:

```
it usually take how many days for a person (between 18 and 65 years old) to visit a pub/cafe/restaurant/supermarket/mall/park/gym in New Zealand or UK (e.g., 1 time every 30 days etc.), can you summarize it in a table ? I need to have some good estimates with credible sources
```