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

test_data <- new_zealand_data()

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
    pop_gender = test_data$`pop_data//gender`,
    pop_ethnicity = test_data$`pop_data//ethnicity`,
    population_structure = test_data$`pop_data//population_structure`,
    household = test_data$`household_data//household`,
    geo_hierarchy = test_data$`geog_data//hierarchy`,
    geo_location = test_data$`geog_data//location`,
    geo_address = test_data$`geog_data//address`,
    employer = test_data$`work_data//employer`,
    employee = test_data$`work_data//employee`,
    travel_to_work = test_data$`commute_data//travel_to_work`,
    school = test_data$`school_data//school`,
    supermarket = test_data$`supermarket_data//supermarket`,
    kindergarten = test_data$`kindergarten_data//kindergarten`,
    restaurant = test_data$`restaurant_data//restaurant`,
    cafe = test_data$`cafe_data//cafe`,
    department_store = test_data$`department_store_data//department_store`,
    wholesale =test_data$`wholesale_data//wholesale`,
    fast_food = test_data$`fast_food_data//fast_food`,
    pub = test_data$`pub_data//pub`,
    park = test_data$`park_data//park`,
    birthplace = test_data$`others//birthplace`,
    assign_address_flag = TRUE
)

