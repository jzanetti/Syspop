source("syspop/r/global_vars.R")
library(arrow)

#' Creates a data frame representing a pseudo-animal population in Antarctica.
#'
#' @param TYPES A named list of animal types and their characteristics.
#'   Each animal type should have these elements:
#'     - speed_on_land: numeric or NULL, speed on land in m/s
#'     - speed_in_sea: numeric, speed in sea in m/s
#'     - on_land: logical, whether animal can be on land
#'     - on_sea: logical, whether animal can be in sea
#'     - food: character or NULL, what the animal eats
#'     - enemy: character or NULL, animal's predator
#'   Defaults to predefined penguin, fish, and seal characteristics.
#' @param NUMS A named list of integers specifying the number of each animal type.
#'   Names must match TYPES names. Defaults to list(penguin=1, fish=3, seal=1).
#'
#' @return A list containing a single element "population_structure" with a data frame.
#'   The data frame has columns:
#'     - type: character, animal type name
#'     - speed_on_land: numeric or NA, speed on land in m/s
#'     - speed_in_sea: numeric, speed in sea in m/s
#'     - on_land: logical, whether animal can be on land
#'     - on_sea: logical, whether animal can be in sea
#'     - food: character or NA, what the animal eats
#'     - enemy: character or NA, animal's predator
#'
#' @throws Error if any animal type in TYPES doesn't have all required characteristic names.
#'
#' @examples
#' result <- pseudo_animal_in_antarctic()
#' print(result$population_structure)
#' #   type   speed_on_land speed_in_sea on_land on_sea food    enemy
#' # 1 penguin         1.2         3.0    TRUE   TRUE fish    seal
#' # 2 fish            NA          2.0   FALSE   TRUE <NA>    penguin
#' # 3 fish            NA          2.0   FALSE   TRUE <NA>    penguin
#' # 4 fish            NA          2.0   FALSE   TRUE <NA>    penguin
#' # 5 seal            1.0         3.5    TRUE   TRUE penguin <NA>
#' @export
#' 
pseudo_animal_in_antarctic <- function(
    TYPES = list(
      penguin = list(
        speed_on_land = 1.2,
        speed_in_sea = 3.0,
        on_land = TRUE,
        on_sea = TRUE,
        food = "fish",
        enemy = "seal"
      ),
      fish = list(
        speed_on_land = NULL,
        speed_in_sea = 2.0,
        on_land = FALSE,
        on_sea = TRUE,
        food = NULL,
        enemy = "penguin"
      ),
      seal = list(
        speed_on_land = 1.0,
        speed_in_sea = 3.5,
        on_land = TRUE,
        on_sea = TRUE,
        food = "penguin",
        enemy = NULL
      )
    ),
    NUMS = list(penguin = 1, fish = 3, seal = 1)
) {

  # Initialize empty lists for each column
  animal_data <- list(
    type = character(),
    speed_on_land = numeric(),
    speed_in_sea = numeric(),
    on_land = logical(),
    on_sea = logical(),
    food = character(),
    enemy = character()
  )
  
  # Get reference keys (all except "type")
  ref_keys <- names(animal_data)[names(animal_data) != "type"]
  
  # Check if all TYPES have required keys
  for (proc_type in names(TYPES)) {
    data_keys <- names(TYPES[[proc_type]])
    if (!setequal(ref_keys, data_keys)) {
      stop(sprintf("Data type does not match for %s", proc_type))
    }
  }
  
  # Build the data structure
  for (proc_type in names(TYPES)) {
    n <- NUMS[[proc_type]]
    if (!is.null(n) && n > 0) {
      animal_data$type <- c(animal_data$type, rep(proc_type, n))
      for (proc_key in ref_keys) {
        value <- TYPES[[proc_type]][[proc_key]]
        # Convert NULL to NA for R compatibility
        if (is.null(value)) value <- NA
        animal_data[[proc_key]] <- c(animal_data[[proc_key]], rep(value, n))
      }
    }
  }
  
  # Convert to data frame
  animal_data <- as.data.frame(animal_data)
  
  return(list(population_structure = animal_data))
}


new_zealand <- function(data_dir = global_vars$NZ_DATA_DEFAULT, apply_pseudo_ethnicity=FALSE) {
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
    ethnicities = c("European", "Maori", "Pacific", "Asian", "MELAA"),
    weights = c(0.6, 0.15, 0.1, 0.12, 0.03)
) {
  household_composition_data$ethnicity <- sample(
    ethnicities,
    size = nrow(household_composition_data),
    replace = TRUE,
    prob = weights
  )
  return(household_composition_data)
}
