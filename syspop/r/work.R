#' Get income data
#'
#' @param income_dataset: income dataset
#'
create_income <- function(income_dataset){
  return (income_dataset)
}


#' Filter employee data by specified areas and return relevant columns.
#'
#' @param employee_data A data frame containing employee information, with at least the columns 'area', 'business_code', and 'employee'.
#' @param all_areas A vector of area names to filter the employee data by.
#'
#' @return A data frame with the following columns:
#' \itemize{
#'   \item area_work: The area name, renamed from 'area'.
#'   \item business_code: The business code identifier.
#'   \item employee: The employee count or data.
#' }
#' 
#' @details
#' This function filters the employee data to only include rows where the 'area' is in the specified `all_areas`.
#' The 'area' column is renamed to 'area_work', and the function returns a data frame containing only the relevant columns.
#'
#' @examples
#' # Example usage:
#' # employee_data <- data.frame(area = c("A", "B"), business_code = c("001", "002"), employee = c(100, 200))
#' # create_employee(employee_data, c("A"))
#' 
create_employee <- function(employee_data, all_areas) {
  # Filter employee data for specified areas
  employee_data <- employee_data[employee_data$area %in% all_areas, ]
  
  # Rename the 'area' column to 'area_work'
  employee_data <- employee_data %>% 
    rename(area_work = area)

  return(employee_data[c("area_work", "business_code", "employee")])
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
        employer = substr(UUIDgenerate(), 1, 6)  # Generate a unique 6-digit ID
      )
    }
  }
  
  # Convert the list of records to a data frame
  return(bind_rows(employer_records))
}

' Place Agent to Employee
#'
#' Assigns a business code to an agent based on age, location, and employment rate.
#'
#' @param employee_data A data frame containing employee information with 'area_work' 
#'                      and 'employee' columns.
#' @param agent A list or data frame containing agent information with 'age' and 'area_work' values.
#'
#' @return A list or data frame: The updated agent with an added 'business_code' value.
#'
#' @details 
#' - Agents under 18 are automatically assigned NA (not employed).
#' - Agents 18 and older are assigned a business code based on the employment rate and a randomly selected 
#'   business code from the `employee_data` data frame.
#' - The 'business_code' value is either a business code (character) or NA.
#'
#' @examples
#' employee_data <- data.frame(area_work = c("A", "B", "A"), 
#'                              employee = c(10, 20, 30), 
#'                              business_code = c("BC1", "BC2", "BC3"))
#' agent <- list(age = 25, area_work = "A")
#' updated_agent <- place_agent_to_employee(employee_data, agent)
#'
place_agent_to_employee <- function(employee_data, agent) {
  # Check if the agent's area is NULL
  if (is.null(agent$area_work)) {
    selected_code <- NA
  } else {
    # Filter the employee data for the agent's area
    proc_employee_data <- employee_data %>%
      filter(area_work == agent$area_work)
    
    # Calculate the weighted probabilities for the employee business codes
    proc_employee_weight <- proc_employee_data$employee / sum(proc_employee_data$employee)
    
    # Sample a business code based on the weights
    selected_code <- sample(
      proc_employee_data$business_code,
      size = 1,
      prob = proc_employee_weight
    )
  }
  
  # Assign the selected business code to the agent
  agent$business_code <- selected_code
  
  return(agent)
}


#' Assigns an income value to an agent based on specific criteria from a DataFrame of income data.
#'
#' This function filters the `income_data` based on the agent's characteristics (gender, business_code, 
#' ethnicity, and age) and assigns the corresponding income value to the agent. If no matching income record is 
#' found, the income is set to "Unknown".
#'
#' @param income_data A data.frame containing income data with columns for gender, business_code, age, ethnicity, and value.
#' @param agent A list representing an agent with attributes: area_work, gender, business_code, ethnicity, and age.
#'
#' @return A modified list representing the agent, now including an 'income' attribute with the assigned income value
#' or "Unknown" if no match is found.
#'
place_agent_to_income <- function(income_data, agent) {

  if (is.null(agent$area_work)) {
    selected_income <- NA
  } else {
    income_data$business_code1 <- sapply(strsplit(income_data$business_code, ","), `[`, 1)
    income_data$business_code2 <- sapply(strsplit(income_data$business_code, ","), `[`, 2)
    income_data$age1 <- as.integer(sapply(strsplit(income_data$age, "-"), `[`, 1))
    income_data$age2 <- as.integer(sapply(strsplit(income_data$age, "-"), `[`, 2))
    
    income_data$business_code1 <- trimws(income_data$business_code1)
    income_data$business_code2 <- trimws(income_data$business_code2)
    
    proc_income_data <- subset(income_data, 
                               gender == agent$gender &
                                 (business_code1 == agent$business_code | business_code2 == agent$business_code) &
                                 ethnicity == agent$ethnicity &
                                 agent$age >= age1 & agent$age <= age2)
    
    if (nrow(proc_income_data) > 1) {
      stop("Income data decoding error ...")
    }
    
    if (nrow(proc_income_data) == 0) {
      selected_income <- "Unknown"
    } else {
      selected_income <- as.character(proc_income_data$value)
    }
    
    agent$income <- selected_income
  }
  
  return(agent)
}

