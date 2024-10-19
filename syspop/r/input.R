source("syspop/r/global_vars.R")
library(arrow)

new_zealand <- function(data_dir = global_vars$NZ_DATA_DEFAULT) {
  # Initialize an empty list to store data
  nz_data <- list()
  
  # List of items to load
  items <- c(
    "population_structure", 
    "geography_hierarchy", 
    "geography_location", 
    "geography_address",
    "household_composition",
    "commute_travel_to_work",
    "commute_travel_to_school",
    "work_employee",
    "work_employer",
    "work_income",
    "school",
    "kindergarten",
    "hospital",
    "shared_space_bakery",
    "shared_space_cafe",
    "shared_space_department_store",
    "shared_space_fast_food",
    "shared_space_park",
    "shared_space_pub",
    "shared_space_restaurant",
    "shared_space_supermarket",
    "shared_space_wholesale"
  )
  
  # Loop through each item and attempt to load corresponding parquet file
  for (item in items) {
    proc_path <- file.path(data_dir, paste0(item, ".parquet"))
    
    # Check if the file exists
    if (file.exists(proc_path)) {
      # Read the parquet file and store in the list
      nz_data[[item]] <- read_parquet(proc_path)
    }
  }
  
  return(nz_data)
}

