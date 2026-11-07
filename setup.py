from setuptools import setup, find_packages

setup(
    name="syspop_v2",
    version="0.5.0",
    packages=find_packages(include=["syspop", "syspop.*"]),
    include_package_data=True,
    package_data={
        "syspop": ["config/*.yml", "data/*.yml"],
    },
    install_requires=[
        "pandas",
        "numpy",
        "pyarrow",
        "pyyaml",
        "matplotlib",
        "openpyxl",
        "graphviz",
    ],
    author="Sijin Zhang",
    description="Synthetic population generation via confidential nearest-neighbour aggregation and stochastic imputation",
    python_requires=">=3.8",
)
