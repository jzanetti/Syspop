
#' Randomly assign each household to an address
#'
#' This function randomly selects an address from a given set of addresses
#' for each unique household in the population data and returns a list
#' of formatted strings containing the address information.
#'
#' @param address_type A string specifying the type of address (e.g., 
#'        "household" or "company").
#' @param pop_data_input A DataFrame containing population data for the area.
#' @param address_data_input A DataFrame containing address data for the area.
#' @param proc_area An integer representing the area being processed.
#'
#' @return A list of strings, each formatted as 
#'         "address_name, latitude, longitude, area", representing the 
#'         assigned addresses for each household.
#'         
assign_place_to_address <- function(address_type, pop_data_input, address_data_input, proc_area) {
  # Randomly assign each household to an address
  
  all_address <- list()
  all_address_names <- unique(pop_data_input[[address_type]])
  
  for (proc_address_name in all_address_names) {
    if (nrow(address_data_input) > 0) {
      proc_address <- address_data_input[sample(nrow(address_data_input), 1), ]
      all_address <- c(all_address, 
                       sprintf("%s, %.5f, %.5f, %d", 
                               proc_address_name, 
                               round(proc_address$latitude, 5), 
                               round(proc_address$longitude, 5), 
                               proc_area))
    }
  }
  return(all_address)
}


#' Add address (latitude and longitude) to each household
#'
#' This function assigns a random address (latitude and longitude) 
#' to each household in the base population dataset, based on 
#' the specified address type. It processes each unique area and 
#' compiles the results into a structured DataFrame.
#'
#' @param base_pop A DataFrame representing the base population to be processed.
#' @param address_data A DataFrame containing address data, including latitude 
#'        and longitude for each area.
#' @param address_type A string indicating the type of address (e.g., 
#'        "household" or "company").
#'
#' @return A DataFrame containing the assigned addresses for each household, 
#'         including columns for name, latitude, longitude, area, and type.
#'
#' @examples
#' result <- add_random_address(base_population, address_data, "household")
#' 
add_random_address <- function(base_pop, address_data, address_type) {
  # Add address (lat and lon) to each household
  start_time <- Sys.time()
  
  all_areas <- unique(base_pop$area)
  
  results <- list()
  
  for (i in seq_along(all_areas)) {
    proc_area <- all_areas[i]
    message(sprintf("%d/%d: Processing %s", i, length(all_areas), proc_area))
    
    proc_address_data <- address_data[address_data$area == proc_area, ]
    
    area_type <- "area"
    if (address_type == "company") {
      area_type <- "area_work"
    }
    
    proc_pop_data <- base_pop[base_pop[[area_type]] == proc_area, ]
    
    processed_address <- assign_place_to_address(address_type, proc_pop_data, proc_address_data, proc_area)
    
    results[[i]] <- processed_address
  }
  
  # Flatten the results list
  flattened_results <- do.call(c, results)
  
  results_dict <- list(name = character(), latitude = numeric(), longitude = numeric(), area = integer())
  
  for (proc_result in flattened_results) {
    proc_value <- unlist(strsplit(proc_result, ","))
    results_dict$name <- c(results_dict$name, proc_value[1])
    results_dict$latitude <- c(results_dict$latitude, as.numeric(proc_value[2]))
    results_dict$longitude <- c(results_dict$longitude, as.numeric(proc_value[3]))
    results_dict$area <- c(results_dict$area, as.integer(proc_value[4]))
  }
  
  results_df <- data.frame(results_dict)

  results_df$type <- address_type
  
  results_df$area <- as.integer(results_df$area)
  
  end_time <- Sys.time()
  total_mins <- round(difftime(end_time, start_time, units = "mins"), 3)
  message(sprintf("Processing time (address): %f", total_mins))
  
  return(results_df)
}
