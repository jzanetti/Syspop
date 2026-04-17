from setuptools import setup, find_packages

setup(
    name="syspop_v2",
    version="0.4.3",  # Or whatever version is appropriate
    packages=find_packages(),
    install_requires=[
        "pandas",
        "numpy",
        "pyarrow"
        "pyyaml",
        "matplotlib",
        "graphviz"
    ],
    author="Sijin Zhang",
    description="Creating synthetic population",
    python_requires='>=3.8',
)