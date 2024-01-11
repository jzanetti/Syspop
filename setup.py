from setuptools import find_packages, setup

setup(
    name="syspop",
    version_format="{tag}.dev{commitcount}+{gitsha}",
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
    ],
    entry_points={
        "console_scripts": [
            "syspop=syspop.syspop:create",
        ],
    },
    # python_requires='==3.9.*',
)
