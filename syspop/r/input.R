source("syspop/r/global_vars.R")
library(arrow)


#' Retrieve Test Data from Catalog
#'
#' Reads the test data catalog, loads Parquet files, and organizes data into a nested list.
#'
#' @return A list containing test data, nested by data type, subtype1 (if available), and subtype2.
#' @details
#'   This function reads the test data catalog CSV file, iterates over each row,
#'   reads the corresponding Parquet file, and stores the data in a nested list.
#'
#' @note
#'   The catalog file should have four columns: data_type, data_subtype1, data_subtype2, and data_path.
#'   If data_subtype1 is missing, data will be nested directly under data_type.
#'  
new_zealand <- function(data_dir = global_vars$nz_data_default) {
  # Get data to create synthetic population
  
  nz_data <- list()
  nz_data$geography <- list()
  
  items <- c(
    "population_structure", 
    "geography_hierarchy", 
    "geography_location", 
    "geography_address",
    "household_composition",
    "commute_travel_to_work",
    "work_employee",
    "work_employer",
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
  
  for (item in items) {
    proc_path <- file.path(data_dir, paste0(item, ".parquet"))
    if (file.exists(proc_path)) {
      nz_data[[item]] <- arrow::read_parquet(proc_path)
    }
  }
  
  return(nz_data)
}

