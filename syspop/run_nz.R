source("syspop/r/global_vars.R")
source("syspop/r/utils.R")
source("syspop/syspop.R")

library(dplyr)
library(tidyr)
library(lubridate)
library(purrr)

test_data <- get_data_for_test()

output_dir <- "/tmp"
#syn_areas <- c(236800, 237200)
syn_areas <- test_data$`geog_data//hierarchy` %>%
  filter(region == "Wellington") %>%
  pull(area)
# syn_areas <- unique(test_data$`geog_data//hierarchy`$area)
create_synthetic_population(
    syn_areas,
    output_dir,
    test_data$`pop_data//gender`,
    test_data$`pop_data//ethnicity`,
    test_data$`geog_data//hierarchy`,
    test_data$`geog_data//location`
)