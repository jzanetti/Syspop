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
new_zealand_data <- function() {
  test_data <- list()
  data_catalog <- read.csv(
    global_vars$test_data_catalog, 
    header = FALSE, 
    col.names = c("data_type", "data_subtype1", "data_subtype2", "data_path"))
  
  for (i in 1:nrow(data_catalog)) {
    row <- data_catalog[i, ]
    file_path <- row$data_path
    data <- read_parquet(file_path)
    if (!is.null(row$data_subtype1)) {
      test_data[[paste0(row$data_type, "/", row$data_subtype1, "/", row$data_subtype2)]] <- data
    } else {
      test_data[[paste0(row$data_type, "/", row$data_subtype2)]] <- data
    }
  }
  
  return(test_data)
}
