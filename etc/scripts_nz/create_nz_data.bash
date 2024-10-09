#!/bin/bash

# Example: ./scripts_nz/create_nz_data.bash

# --------------------
# Activate your Conda environment
# --------------------
CONDA_BASE=~/miniconda3
source $CONDA_BASE/bin/activate syspop


formatted_time=$(date -u +'%Y%m%dT%H%M')

raw_data_dir=$1
conf_data_dir=$2
input_cfg=$3
nz_data_dir=$4
llm_diary_path=$5

read -p "If re-run OSM (Enter true or false, if false, raw_data directory must contain pre-downloaded OSM data): " flag

if [ -z "$raw_data_dir" ]
then
  echo "Please enter the raw_data directory (e.g., etc/data/raw_data):"
  read raw_data_dir
fi

if [ -z "$conf_data_dir" ]
then
  echo "Please enter the conf_data directory (e.g., etc/data/confidential_data):"
  read conf_data_dir
fi

if [ -z "$input_cfg" ]
then
  echo "Please enter the raw input configuration file (e.g., etc/scripts_nz/input_v2.0.yml):"
  read input_cfg
fi

if [ -z "$nz_data_dir" ]
then
  echo "Please enter the nz_data directory (e.g., etc/data/test_data_latest):"
  read nz_data_dir
fi

if [ -z "$llm_diary_path" ]
then
  echo "Please enter the LLM diary output (e.g., etc/data/confidential_data/processed/llm/llm_diary.pickle):"
  read llm_diary_path
fi

# --------------------------------
# Step 1: Data backup
# --------------------------------
directory=$(dirname "$raw_data_dir")
filename=$(basename "$raw_data_dir")
export extracted_filename="${filename%_*}" # extracted="${str%_*}"
echo cp -rf ${raw_data_dir} ${directory}/archive/${extracted_filename}_${formatted_time}
cp -rf ${raw_data_dir} ${directory}/archive/${extracted_filename}_${formatted_time}

directory=$(dirname "$nz_data_dir")
filename=$(basename "$nz_data_dir")
export extracted_filename="${filename%_*}" # extracted="${str%_*}"
echo cp -rf ${nz_data_dir} ${directory}/archive/${extracted_filename}_${formatted_time}
cp -rf ${nz_data_dir} ${directory}/archive/${extracted_filename}_${formatted_time}

# --------------------------------
# Step 2: Get OSM data (e.g., add latest OSM data to the raw data directory)
# This is a very slow process ...
# --------------------------------
if [ "${flag,,}" = "true" ]; then
  echo "Running get_osm_data ..."
  python etc/scripts_nz/get_osm_data.py
fi

# --------------------------------
# Step 3: Copy confidential data
# --------------------------------
echo "Copying confidential data ..."
cp -rf ${conf_data_dir}/processed/*.csv ${raw_data_dir}

# --------------------------------
# Step 4: Create NZ data (write NZ data from the raw data directory)
# --------------------------------
echo "Running create_nz_data ..."
python etc/scripts_nz/create_nz_data.py --workdir ${nz_data_dir} --input ${input_cfg} # --add_proj

# --------------------------------
# Step 5: Copy the latest diary data from LLM
# The data is created by create_diary and combine_diary
# --------------------------------
echo "Copying LLM data ..."
cp -rf ${llm_diary_path} ${nz_data_dir}

echo "The Input data for NZ is written to:" ${nz_data_dir}
