from setuptools import setup, find_packages

setup(
    name='syspop',
    version='0.0.5',
    packages=find_packages(),
    install_requires=[
        'numpy',
        'pandas',
        'matplotlib',
        'pyarrow',
        'overpy',
        'geopy',
        'scipy',
        'shapely',
        'openpyxl',
        'ray[default]',
        'xlrd',
        'OSMPythonTools',
    ],
    entry_points={
        'console_scripts': [
            'syspop=syspop.syspop:create',
        ],
    },
    # python_requires='==3.9.*',
)