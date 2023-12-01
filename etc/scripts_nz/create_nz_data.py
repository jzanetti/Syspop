# export PYTHONPATH=~/Github/Syspop/etc/scripts_nz

from os import makedirs
from os.path import exists

from funcs.business.business import (
    create_employee_by_gender_by_sector,
    create_employers_by_employees_number,
    write_employers_by_sector,
)
from funcs.geography.geography import (
    create_geography_hierarchy,
    create_geography_location_area,
    create_geography_location_super_area,
    create_geography_name_super_area,
)
from funcs.household.household import create_household_number
from funcs.population.population import (
    create_age,
    create_ethnicity_and_age,
    create_female_ratio,
    create_population,
    create_socialeconomic,
)
from funcs.postproc import postproc
from funcs.transport.transport import (
    create_population_travel_to_work_by_method,
    write_workplace_and_home_locations,
)
from funcs.venue.venue import create_hospital, create_school, write_leisures


def import_raw_data(workdir: str):

    if not exists(workdir):
        makedirs(workdir)

    # -----------------------------
    # Create geography
    # -----------------------------
    geography_hierarchy_data = create_geography_hierarchy()
    geography_location_super_area_data = create_geography_location_super_area(
        geography_hierarchy_data
    )
    geography_name_super_area_data = create_geography_name_super_area()
    geography_location_area_data = create_geography_location_area()
    socialeconomic_data = create_socialeconomic(geography_hierarchy_data)

    # -----------------------------
    # Create population
    # -----------------------------
    total_population_data = create_population()
    female_ratio_data = create_female_ratio()
    ethnicity_and_age_data = create_ethnicity_and_age(total_population_data)
    age_data = create_age(total_population_data)

    # -----------------------------
    # Create business
    # -----------------------------
    employers_by_employees_number_data = create_employers_by_employees_number(
        total_population_data, geography_hierarchy_data
    )
    employers_by_sector_data = write_employers_by_sector(
        total_population_data,
        geography_hierarchy_data,
        employers_by_employees_number_data,
    )
    employee_by_gender_by_sector_data = create_employee_by_gender_by_sector(
        geography_hierarchy_data, total_population_data
    )

    # -----------------------------
    # Create transport
    # -----------------------------
    population_travel_to_work_by_method_data = (
        create_population_travel_to_work_by_method()
    )

    workplace_and_home_locations_data = write_workplace_and_home_locations(
        geography_hierarchy_data
    )

    # -----------------------------
    # Create household
    # -----------------------------
    household_number_data = create_household_number()

    # -----------------------------
    # Create venues
    # -----------------------------
    create_hospital(workdir, geography_hierarchy_data, geography_location_area_data)
    create_school(workdir, geography_location_area_data)
    write_leisures(workdir)

    # -----------------------------
    # Postprocessing
    # -----------------------------
    postproc(
        workdir,
        {
            "geography_hierarchy_data": geography_hierarchy_data,
            "geography_location_super_area_data": geography_location_super_area_data,
            "geography_location_area_data": geography_location_area_data,
            "geography_name_super_area_data": geography_name_super_area_data,
            "socialeconomic_data": socialeconomic_data,
            "female_ratio_data": female_ratio_data,
            "ethnicity_and_age_data": ethnicity_and_age_data,
            "age_data": age_data,
            "employee_by_gender_by_sector_data": employee_by_gender_by_sector_data,
            "employers_by_employees_number_data": employers_by_employees_number_data,
            "employers_by_sector_data": employers_by_sector_data,
            "population_travel_to_work_by_method_data": population_travel_to_work_by_method_data,
            "household_number_data": household_number_data,
            "workplace_and_home_locations_data": workplace_and_home_locations_data,
        },
        pop=total_population_data,
    )

    print("Job done ...")

if __name__ == "__main__":
    workdir = "/tmp/syspop"
    import_raw_data(workdir)