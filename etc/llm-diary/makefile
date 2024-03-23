


install_env:
	mamba env create -f env.yml

install_llama:
	source activate syspop_llm & CUDACXX=/usr/local/cuda-12.1/bin/nvcc CMAKE_ARGS="-DLLAMA_CUBLAS=on -DCMAKE_CUDA_ARCHITECTURES=all-major" FORCE_CMAKE=1 pip install llama-cpp-python --no-cache-dir --force-reinstall --upgrade

run_diary:
	nohup ./run_diary.bash >& log &