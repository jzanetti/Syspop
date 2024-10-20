source("syspop/r/global_vars.R")
source("syspop/r/input.R")
source("syspop/start.R")

library(dplyr)
library(tidyr)
library(lubridate)
library(purrr)
library(arrow)
library(dplyr)
library(lubridate)
library(dplyr)
library(uuid)
library(tibble)
library(fs)
library(data.table)

nz_data <- new_zealand()

output_dir <- "/tmp/syspop"
syn_areas <- c(241400, 241800, 241800)

# Create the synthetic population, assuming syspop_create is a defined function in R or custom code
create(
  syn_areas = syn_areas,
  output_dir = output_dir,
  population = list(structure = nz_data$population_structure),
  geography = list(
    hierarchy = nz_data$geography_hierarchy,
    location = nz_data$geography_location,
    address = nz_data$geography_address
  ),
  household = list(composition = nz_data$household_composition),
  work = list(
    employee = nz_data$work_employee,
    employer = nz_data$work_employer,
    income = nz_data$work_income
  ),
  commute = list(
    travel_to_work = nz_data$commute_travel_to_work,
    travel_to_school = nz_data$commute_travel_to_school
  ),
  education = list(
    school = nz_data$school,
    kindergarten = nz_data$kindergarten
  ),
  shared_space = list(
    hospital = nz_data$hospital,
    bakery = nz_data$shared_space_bakery,
    cafe = nz_data$shared_space_cafe,
    department_store = nz_data$shared_space_department_store,
    fast_food = nz_data$shared_space_fast_food,
    park = nz_data$shared_space_park,
    pub = nz_data$shared_space_pub,
    restaurant = nz_data$shared_space_restaurant,
    supermarket = nz_data$shared_space_supermarket,
    wholesale = nz_data$shared_space_wholesale
  )
)