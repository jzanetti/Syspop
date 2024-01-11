

# Synthetic & Simulated Population (SysPop)

<p align="center">
    <img src="etc/wiki_img/syspop_wiki.png" alt="Sample Image" width="30%">
</p>

Syspop is developed at [ESR](https://www.esr.cri.nz/home/about-esr/). _Contact: Sijin.Zhang@esr.cri.nz_

### Contents:

* [Installation](https://github.com/jzanetti/Syspop#installation)
* [Usage](https://github.com/jzanetti/Syspop#usage)

**Detailed documentation of SysPop can be found at [SysPop Wiki](https://github.com/jzanetti/Syspop/wiki)**

## Installation

## Usage

A synthetic population can be created using:

```
syspop.create(
    syn_areas = [135400, 111400, 110400],
    output_dir = "/tmp/syspop_test",
    pop_gender = pop_data["gender"],
    pop_ethnicity = pop_data["ethnicity"],
    geo_hierarchy = geog_data["hierarchy"],
    geo_location = geog_data["location"],
    geo_address = geog_data["address"],
    household = household_data["household"],
    socialeconomic = geog_data["socialeconomic"],
    work_data = work_data,
    home_to_work = commute_data["home_to_work"],
    school_data = school_data["school"],
    hospital_data = hospital_data["hospital"],
    supermarket_data = supermarket_data["supermarket"],
    restaurant_data = restaurant_data["restaurant"],
    assign_address_flag = True,
    rewrite_base_pop = False,
    use_parallel = True,
    ncpu = 8
)
```

You can find detailed descriptions of the input data for each argument in [Input data](https://github.com/jzanetti/Syspop/wiki/Input-data).

It's important to note that all arguments in the syspop.create function are optional, and their requirement depends on the specific synthetic information that needs to be generated. To understand the interdependencies between different synthetic information, refer to the documentation available [here](https://github.com/jzanetti/Syspop/wiki/Synthetic-population)

