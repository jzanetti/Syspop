source("syspop/r/global_vars.R")
library(arrow)

new_zealand <- function(data_dir = global_vars$NZ_DATA_DEFAULT, apply_pseudo_ethnicity=TRUE) {
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

  if (apply_pseudo_ethnicity == TRUE) {
    nz_data[["household_composition"]] = add_pseudo_hhd_ethnicity(
      nz_data[["household_composition"]])
  }
  
  return(nz_data)
}


add_pseudo_hhd_ethnicity <- function(
    household_composition_data,
    ethnicities = c("european", "maori", "asian", "others"),
    weights = c(0.7, 0.15, 0.12, 0.03)
) {
  household_composition_data$ethnicity <- sample(
    ethnicities,
    size = nrow(household_composition_data),
    replace = TRUE,
    prob = weights
  )
  return(household_composition_data)
}
