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
      stop(paste0(key_item, " is presented/required, but some of the dependencies are missing"))
    }
  }
  
  if (assign_address_flag) {
    for (item_to_check in address_deps) {
      if (is.null(item_to_check)) {
        stop(paste0("Address data is required, but some of the dependencies are missing"))
      }
    }
  }
}

create_synthetic_population <- function(
    syn_areas = NULL,
    output_dir = "",
    pop_gender = NULL,
    pop_ethnicity = NULL,
    household = NULL,
    geo_hierarchy = NULL,
    geo_location = NULL,
    geo_address = NULL,
    employer = NULL,
    employee = NULL,
    school = NULL,
    kindergarten = NULL,
    travel_to_work = NULL,
    assign_address_flag = FALSE
) {
  
  # Create temporary directory
  tmp_dir <- file.path(output_dir, "tmp")
  dir.create(tmp_dir, showWarnings = FALSE)

  print("Creating base population ...")
  check_dependencies("base_pop", list(pop_gender, pop_ethnicity, syn_areas), assign_address_flag, list(geo_address))
  create_base_pop(
    tmp_dir, 
    pop_gender, 
    pop_ethnicity, 
    syn_areas, 
    ref_population = "gender")
  
  print("Creating household ...")
  check_dependencies("household", list(), assign_address_flag, list(geo_address))
  create_household(
    tmp_dir, 
    household, 
    geo_address)
  
  print("Creating work ...")
  check_dependencies("work", list(travel_to_work, geo_hierarchy), assign_address_flag, list(geo_address))
  create_work(
    tmp_dir, 
    employer,
    employee,
    travel_to_work, 
    geo_hierarchy, 
    geo_address
  )
  
  print("Creating school ...")
  check_dependencies("school", list(school, geo_hierarchy), assign_address_flag, list(geo_address))
  create_school_and_kindergarten(
    "school", 
    tmp_dir, 
    school, 
    geo_hierarchy,
    possible_area_levels = c("area", "super_area", "region")
  )
  
  
  print("Creating kindergarten ...")
  check_dependencies("kindergarten", list(kindergarten, geo_hierarchy), assign_address_flag, list(geo_address))
  create_school_and_kindergarten(
    "kindergarten",
    tmp_dir,
    kindergarten,
    geo_hierarchy,
    possible_area_levels = c("area")
  )
  
}
