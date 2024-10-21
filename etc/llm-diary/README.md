


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
make create_diary
```

### Combine created diaries for Syspop
```
make combine_diary
```