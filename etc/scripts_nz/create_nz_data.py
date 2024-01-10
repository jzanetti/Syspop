# export PYTHONPATH=~/Github/Syspop/etc/scripts_nz
from funcs.wrapper import (
    create_geography_wrapper, 
    create_household_number, 
    create_population_wrapper, 
    create_travel_wrapper,
    create_school_wrapper,
    create_hospital_wrapper,
    create_shared_space_wrapper,
    create_work_wrapper)
from os.path import exists
from os import makedirs

def import_raw_data(workdir: str):

    if not exists(workdir):
        makedirs(workdir)


    # -----------------------------
    # Create shared space
    # -----------------------------
    create_shared_space_wrapper(workdir)

    # -----------------------------
    # Create geography
    # -----------------------------
    create_geography_wrapper(workdir)

    # -----------------------------
    # Create household
    # -----------------------------
    create_household_number(workdir)

    # -----------------------------
    # Create population
    # -----------------------------
    create_population_wrapper(workdir)

    # -----------------------------
    # Create commute
    # -----------------------------
    create_travel_wrapper(workdir)

    # -----------------------------
    # Create work
    # -----------------------------
    create_work_wrapper(workdir)

    # -----------------------------
    # Create school
    # -----------------------------
    create_school_wrapper(workdir)

    # -----------------------------
    # Create hospital
    # -----------------------------
    create_hospital_wrapper(workdir)

    """
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
    """
    print("Job done ...")

if __name__ == "__main__":
    workdir = "/tmp/syspop"
    import_raw_data(workdir)