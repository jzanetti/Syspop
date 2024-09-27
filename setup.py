from setuptools import find_packages, setup

setup(
    name="syspop",
    version="0.2.1",
    setup_requires=["setuptools-git-versioning"],
    packages=find_packages(),
    install_requires=[
        "numpy",
        "pandas",
        "matplotlib",
        "pyarrow",
        "overpy",
        "geopy",
        "scipy",
        "shapely",
        "openpyxl",
        "ray[default]",
        "xlrd",
        "OSMPythonTools",
        "folium",
        "seaborn",
        "geopandas"
    ],
    entry_points={
        "console_scripts": [
            "syspop=syspop.syspop:create",
        ],
    },
    # python_requires="==3.10.*",
)
