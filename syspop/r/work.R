#' Calculate Business Code Probability
#'
#' This function calculates the employee probability for each business code
#' within specified areas. The probability is determined as the proportion of
#' employees for each business code within the given areas.
#'
#' @param employee_data A data frame containing employee information, including
#'                      columns for 'area' and 'employee'.
#' @param all_areas A vector of areas to filter the employee data by.
#'
#' @return A data frame containing the following columns:
#'         - area_work (str): Area name
#'         - business_code (str): Business code identifier
#'         - percentage (float): Employee probability (proportion of employees)
#'
#' @notes The resulting data frame is filtered to only include areas specified
#'        in `all_areas`.
#'
#' @import dplyr
#'
create_business_code_probability <- function(employee_data, all_areas) {
  
  # Filter employee data by specified areas
  employee_data <- employee_data %>%
    filter(area %in% all_areas)
  
  # Calculate total employees per area
  total_employees_per_area <- employee_data %>%
    group_by(area) %>%
    summarise(total_employees = sum(employee), .groups = "drop")
  
  # Join total employees back to the employee_data
  employee_data <- employee_data %>%
    left_join(total_employees_per_area, by = "area") %>%
    mutate(percentage = employee / total_employees) %>%
    rename(area_work = area) %>%
    select(area_work, business_code, percentage)
  
  return(employee_data)
}


#' Expand Employer Data
#'
#' This function expands employer data into individual records, filtering by specified areas.
#'
#' @param employer_dataset A data frame containing employer information with columns
#'                        'area', 'business_code', and 'employer'.
#' @param address_data A data frame containing address datasets with latitude and longitude.
#' @param all_areas A vector of areas to include in the expanded data frame.
#'
#' @return A data frame containing expanded employer data with individual records for
#'         each employer, filtered by specified areas. The resulting data frame contains
#'         the following columns:
#'         - area_work (int): Area name
#'         - business_code (str): Business code identifier
#'         - latitude (numeric): Latitude of the employer's location
#'         - longitude (numeric): Longitude of the employer's location
#'         - id (str): A unique 6-digit ID generated for each individual record
#'
#' @notes Each row in the original data frame is expanded into multiple rows based on the
#'        'employer' count, with a unique ID generated for each record.
#'
#' @import dplyr
#' @import uuid
#'
create_employer <- function(employer_dataset, address_data, all_areas) {
  
  # Filter employer dataset by specified areas
  employer_dataset <- employer_dataset %>%
    filter(area %in% all_areas)
  
  employer_records <- list()  # Initialize an empty list to store individual records
  
  # Loop through each row in the employer dataset
  for (i in 1:nrow(employer_dataset)) {
    row <- employer_dataset[i, ]
    area <- row$area
    business_code <- row$business_code
    count <- row$employer
    proc_address_data_area <- address_data[address_data$area == area, ]
    
    # Create individual records for each employer
    for (j in 1:count) {
      proc_address_data <- proc_address_data_area[sample(nrow(proc_address_data_area), 1), ]
      employer_records[[length(employer_records) + 1]] <- list(
        area_work = as.integer(area),
        business_code = as.character(business_code),
        latitude = as.numeric(proc_address_data$latitude),
        longitude = as.numeric(proc_address_data$longitude),
        id = substr(UUIDgenerate(), 1, 6)  # Generate a unique 6-digit ID
      )
    }
  }
  
  # Convert the list of records to a data frame
  return(bind_rows(employer_records))
}

#' Assign Agent to Business Code
#'
#' This function assigns a business code to an agent based on their age, location, and employment rate.
#'
#' @param employee_data A data frame containing employee information with 'area' and 'percentage' columns.
#' @param agent A named vector containing agent information, including 'age' and 'area' values.
#' @param employment_rate A numeric value representing the probability of an adult agent being employed. 
#'                        Defaults to 0.9.
#'
#' @return A named vector representing the updated agent with an added 'business_code' value.
#'
#' @details
#' - Agents under 18 are automatically assigned NA (not employed).
#' - Agents 18 and older are assigned a business code based on the employment rate and a randomly selected 
#'   business code from the employee_data data frame.
#' - The 'business_code' value is either a character string (the selected business code) or NA.
#'
#' @throws ValueError if employment_rate is not between 0 and 1.
#'
#' @examples
#' # Assuming employee_data is a data frame and agent is a named vector
#' updated_agent <- assign_agent_to_business_code(employee_data, 
#'                                                agent, 
#'                                                employment_rate = 0.9)
#'
#' @export
assign_agent_to_business_code <- function(employee_data, agent, employment_rate = 0.9) {
  
  # Check employment rate
  if (employment_rate < 0 || employment_rate > 1) {
    stop("ValueError: employment_rate must be between 0 and 1.")
  }
  
  # Assign business code based on agent's work area
  if (is.null(agent$area_work)) {
    selected_code <- NULL
  } else {
    employee_data_area_work <- employee_data[
      employee_data$area_work == agent$area_work, ]
  
    selected_code <- employee_data_area_work$business_code[
      sample(nrow(employee_data_area_work), 
             size = 1, 
             prob = employee_data_area_work$percentage)
    ]
  }
  
  # Update the agent's business code
  agent$business_code <- selected_code
  
  return(agent)
}


