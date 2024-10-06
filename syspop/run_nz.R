source("syspop/r/global_vars.R")
source("syspop/r/utils.R")
source("syspop/syspop.R")

library(dplyr)
library(tidyr)
library(lubridate)
library(purrr)
library(arrow)

test_data <- get_data_for_test()

output_dir <- "/tmp"
syn_areas <- c(236800, 237200)
# syn_areas <- test_data$`geog_data//hierarchy` %>%
#  filter(region == "Wellington") %>%
#  pull(area)
# syn_areas <- unique(test_data$`geog_data//hierarchy`$area)

create_synthetic_population(
  syn_areas = syn_areas,
    output_dir = output_dir,
    pop_gender = test_data$`pop_data//gender`,
    pop_ethnicity = test_data$`pop_data//ethnicity`,
    household = test_data$`household_data//household`,
    geo_hierarchy = test_data$`geog_data//hierarchy`,
    geo_location = test_data$`geog_data//location`,
    geo_address = test_data$`geog_data//address`,
    employer = test_data$`work_data//employer`,
    employee = test_data$`work_data//employee`,
    travel_to_work = test_data$`commute_data//travel_to_work`,
    assign_address_flag = TRUE
)

