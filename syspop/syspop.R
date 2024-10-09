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
    population_structure = NULL,
    household = NULL,
    geo_hierarchy = NULL,
    geo_location = NULL,
    geo_address = NULL,
    employer = NULL,
    employee = NULL,
    school = NULL,
    kindergarten = NULL,
    supermarket = NULL,
    restaurant = NULL,
    cafe = NULL,
    department_store = NULL,
    wholesale = NULL,
    fast_food = NULL,
    pub = NULL,
    park = NULL,
    birthplace = NULL,
    travel_to_work = NULL,
    assign_address_flag = FALSE
) {
  
  # Create temporary directory
  tmp_dir <- file.path(output_dir, "tmp")
  dir.create(tmp_dir, showWarnings = FALSE, recursive=TRUE)

  print("Creating base population ...")
  if(is.null(population_structure)){
    check_dependencies("base_pop", list(pop_gender, pop_ethnicity, syn_areas), assign_address_flag, list(geo_address))
  }
  else {
    check_dependencies("base_pop", list(population_structure, syn_areas), assign_address_flag, list(geo_address))
  }
  create_base_pop(
    tmp_dir, 
    pop_gender, 
    pop_ethnicity,
    population_structure,
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
    tmp_dir, 
    school, 
    "school", 
    geo_hierarchy,
    possible_area_levels = c("area", "super_area", "region")
  )
  
  
  print("Creating kindergarten ...")
  check_dependencies("kindergarten", list(kindergarten, geo_hierarchy), assign_address_flag, list(geo_address))
  create_school_and_kindergarten(
    tmp_dir,
    kindergarten,
    "kindergarten",
    geo_hierarchy,
    possible_area_levels = c("area")
  )
  
  print("Creating supermarket ...")
  check_dependencies("supermarket", list(supermarket, geo_location), assign_address_flag, list(geo_address))
  create_shared_space(
    tmp_dir,
    supermarket,
    "supermarket",
    geo_location,
    area_name_keys_and_selected_nums = list(area = 2)
  )
  
  print("Creating restaurant ...")
  check_dependencies("restaurant", list(restaurant, geo_location), assign_address_flag, list(geo_address))
  create_shared_space(
    tmp_dir,
    restaurant,
    "restaurant",
    geo_location,
    area_name_keys_and_selected_nums = list(area = 2)
  )

  print("Creating cafe ...")
  check_dependencies("cafe", list(cafe, geo_location), assign_address_flag, list(geo_address))
  create_shared_space(
    tmp_dir,
    cafe,
    "cafe",
    geo_location,
    area_name_keys_and_selected_nums = list(area = 2)
  )
  
  print("Creating department_store ...")
  check_dependencies("department_store", list(department_store, geo_location), assign_address_flag, list(geo_address))
  create_shared_space(
    tmp_dir,
    department_store,
    "department_store",
    geo_location,
    area_name_keys_and_selected_nums = list(area = 2)
  )
  
  print("Creating wholesale ...")
  check_dependencies("wholesale", list(wholesale, geo_location), assign_address_flag, list(geo_address))
  create_shared_space(
    tmp_dir,
    wholesale,
    "wholesale",
    geo_location,
    area_name_keys_and_selected_nums = list(area = 2)
  )
  
  print("Creating fast_food ...")
  check_dependencies("fast_food", list(fast_food, geo_location), assign_address_flag, list(geo_address))
  create_shared_space(
    tmp_dir,
    fast_food,
    "fast_food",
    geo_location,
    area_name_keys_and_selected_nums = list(area = 1)
  )
  
  print("Creating pub ...")
  check_dependencies("pub", list(pub, geo_location), assign_address_flag, list(geo_address))
  create_shared_space(
    tmp_dir,
    pub,
    "pub",
    geo_location,
    area_name_keys_and_selected_nums = list(area = 1)
  )
  
  print("Creating park ...")
  check_dependencies("park", list(park, geo_location), assign_address_flag, list(geo_address))
  create_shared_space(
    tmp_dir,
    park,
    "park",
    geo_location,
    area_name_keys_and_selected_nums = list(area = 1)
  )

  print("Creating birthplace ...")
  check_dependencies("birthplace", list(birthplace), FALSE, list())
  create_birthplace(tmp_dir, birthplace)
  
  print("Creating output ...")
  base_pop <- read_parquet(file.path(tmp_dir, "syspop_base.parquet"))
  base_address <- read_parquet(file.path(tmp_dir, "syspop_location.parquet"))

  output_files <- list(
    syspop_base = c("area", "age", "gender", "ethnicity"),
    syspop_household = c("household"),
    syspop_travel = c("travel_mode_work"),
    syspop_work_and_school = c("area_work", "company", "school", "kindergarten"),
    syspop_lifechoice = c("supermarket", "restaurant", "cafe", "department_store", "wholesale", "fast_food", "pub", "park"),
    syspop_immigration = c("birthplace")
  )

  # Add an 'id' column to synpop_data$synpop using row numbers
  base_pop$id <- seq_len(nrow(base_pop))
  for (name in names(output_files)) {
    cols <- output_files[[name]]
    selected_data <- base_pop[, c("id", cols), drop = FALSE]
    write_parquet(selected_data, file.path(output_dir, paste0(name, ".parquet")))
  }
  write_parquet(base_address, file.path(output_dir, "syspop_location.parquet"))
}


validate_synthetic_population <- function(
    output_dir = "",
    population_structure = NULL){
  
  browser()
  if(!is.null(population_structure)) {
    print("Validating population structure")
    syn_population_structure <- read_parquet(file.path(output_dir, "syspop_base.parquet"))
    
  }
}
