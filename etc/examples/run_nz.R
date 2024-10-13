source("syspop/r/global_vars.R")
source("syspop/r/input.R")
source("syspop/syspop.R")

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

nz_data <- new_zealand()

output_dir <- "/tmp/syspop"
# syn_areas <- c(236800, 237200)
syn_areas <- c(241800, 242800, 241400, 242000)
# syn_areas <- test_data$`geog_data//hierarchy` %>%
#  filter(region == "Wellington") %>%
#  pull(area)
# syn_areas <- unique(test_data$`geog_data//hierarchy`$area)

create_synthetic_population(
    syn_areas = syn_areas,
    output_dir = output_dir,
    population = list(
      structure = nz_data$population_structure
    ),
    household = list(
      composition = nz_data$household_composition
    ),
    geography = list(
      hierarchy = nz_data$geography_hierarchy,
      location = nz_data$geography_location,
      address = nz_data$geography_address
    ),
    work = list(
      employer = nz_data$work_employer,
      employee = nz_data$work_employee
    ),
    commute = list(
      travel_to_work = nz_data$commute_travel_to_work
    ),
    education = list(
      school = nz_data$school,
      kindergarten = nz_data$kindergarten
    ),
    shared_space = list(
      supermarket = nz_data$shared_space_supermarket,
      restaurant = nz_data$shared_space_restaurant,
      department_store = nz_data$shared_space_department_store,
      wholesale = nz_data$shared_space_wholesale,
      cafe = nz_data$shared_space_cafe,
      fast_food = nz_data$shared_space_fast_food,
      pub = nz_data$shared_space_pub,
      park = nz_data$shared_space_park
    )
)

