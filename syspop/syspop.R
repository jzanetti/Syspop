source("syspop/r/create_pop_wrapper.R")


#' Check Dependencies for Key Items
#'
#' This function checks if all dependencies are met for a given key item.
#'
#' @param key_item Character string representing the key item being checked.
#' @param deps_list Character vector of dependencies required for the key item.
#' @param address_deps Character vector of address-related dependencies required when assign_address_flag is TRUE.
#' @param assign_address_flag Assign address to data
#'
#' @details
#' This function stops execution and throws an error if any dependency is not provided.
#'
#'
#' @examples
#' check_dependencies("base_pop", c("pop_gender", "pop_ethnicity"), c("geo_address", "geo_location"))
#' 
check_dependencies <- function(key_item, deps_list, assign_address_flag, address_deps) {
  for (item_to_check in deps_list) {
    if (is.null(item_to_check)) {
      stop(paste0(key_item, " is presented/required, but its dependency ", item_to_check, " is not provided."))
    }
  }
  
  if (assign_address_flag) {
    for (item_to_check in address_deps) {
      if (is.null(item_to_check)) {
        stop(paste0("Address data is required for ", key_item, ", but its address dependency ", item_to_check, " is not provided."))
      }
    }
  }
}

create_synthetic_population <- function(
    syn_areas = NULL,
    output_dir = "",
    pop_gender = NULL,
    pop_ethnicity = NULL,
    geo_hierarchy = NULL,
    geo_location = NULL,
    geo_address = NULL,
    assign_address_flag = FALSE
) {
  
  # Create temporary directory
  tmp_dir <- file.path(output_dir, "tmp")
  dir.create(tmp_dir, showWarnings = FALSE)
  
  check_dependencies("base_pop", c(pop_gender, pop_ethnicity, syn_areas), assign_address_flag, c())
  create_base_pop(file.path(tmp_dir, "syspop.parquet"), pop_gender, pop_ethnicity, syn_areas, ref_population = "gender")

  

}
