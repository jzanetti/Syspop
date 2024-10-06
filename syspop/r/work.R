source("syspop/r/commute.R")
source("syspop/r/address.R")

#' Work and Commute Data Processing Wrapper
#'
#' This function serves as a wrapper to handle the integration of employer,
#' employee, population, and commute data. It processes these datasets to
#' create a structured base population and manage geographical data. If
#' geographical address data is provided, it will be processed and appended
#' to the base address data.
#'
#' @param employer_data A DataFrame containing information about employers,
#'        including their codes and other relevant details.
#' @param employee_data A DataFrame containing data related to employees,
#'        including their associated employer codes and areas.
#' @param pop_data A DataFrame that represents the population data, which
#'        may include demographic information such as age and area.
#' @param base_address A DataFrame representing the base addresses to which
#'        additional address data may be appended.
#' @param commute_data A DataFrame containing commuting information, detailing
#'        how individuals travel to work based on their home and work areas.
#' @param geo_hierarchy_data A DataFrame containing geographical hierarchy data,
#'        which may include information on regions, districts, or other levels of
#'        geographical organization.
#' @param geo_address_data A DataFrame containing additional geographical address
#'        information. Defaults to NULL, indicating that this data is optional.
#'
#' @return A list containing two elements:
#'         - `base_pop`: A DataFrame representing the processed base population
#'           with assigned work and commute information.
#'         - `base_address`: A DataFrame representing the updated base addresses,
#'           which includes any additional address data processed if provided.
#'
#' @examples
#' result <- work_and_commute_wrapper(employer_data, employee_data, pop_data, 
#'                                     base_address, commute_data, geo_hierarchy_data, 
#'                                     geo_address_data)
#'
work_and_commute_wrapper <- function(
    employer_data,
    employee_data,
    pop_data, 
    base_address, 
    commute_data, 
    geo_hierarchy_data, 
    geo_address_data = NULL
) {

  # Call create_work_and_commute
  base_pop <- create_work_and_commute(
    employer_data,
    employee_data,
    pop_data,
    base_address,
    commute_data
  )

  # Process geo_address_data if provided
  if (!is.null(geo_address_data)) {
    proc_address_data <- add_random_address(
      base_pop, geo_address_data, "company"
    )
    base_address <- rbind(base_address, proc_address_data)
    base_address$area <- as.integer(base_address$area)
  }
  
  return(list(base_pop = base_pop, base_address = base_address))
}

#' Create Work and Commute Data
#'
#' This function processes employer, employee, population, and commute data 
#' to assign home and work locations to the population, as well as to 
#' assign employers based on the processed data.
#' 
#' @param employer_data A DataFrame containing data on employers, 
#'        which includes the area each employer is located in and the number of employers.
#' @param employee_data A DataFrame containing data on employees, 
#'        including the area where they are located and the number of employees.
#' @param pop_data A DataFrame containing population data, which includes the area of residence.
#' @param base_address A DataFrame containing base address information (not currently used in this function).
#' @param commute_data A DataFrame containing commute data, including work areas and associated travel methods.
#' 
#' @return A DataFrame of the updated base population with assigned work areas and employers.
#' 
#' @details 
#' - The function iterates through each unique area found in the population data. 
#' - For each area, it retrieves corresponding commute, employee, and employer data. 
#' - The total number of employees in each area is summed and stored.
#' - It then calls `travel_between_home_and_work` to assign work locations to the base population. 
#' - Finally, it calls `assign_employees_employers_to_base_pop` to assign employers to the base population.
#' 
#' @examples
#' employer_data <- data.frame(area = c(1, 1, 2), employer_number = c(3, 5, 2))
#' employee_data <- data.frame(area = c(1, 1, 2), employee_number = c(100, 150, 50))
#' pop_data <- data.frame(area = c(1, 1, 2), age = c(25, 34, 45))
#' commute_data <- data.frame(area_work = c(1, 2), travel_method = c("bus", "car"))
#' base_address <- data.frame()  # Example base address, not used here
#' create_work_and_commute(employer_data, employee_data, pop_data, base_address, commute_data)
#'
create_work_and_commute <- function(
    employer_data, 
    employee_data, 
    pop_data,
    base_address,
    commute_data
) {
  all_areas <- unique(pop_data$area)
  
  all_commute_data <- list()
  all_employers <- list()
  all_employees <- list()
  
  for (proc_area in all_areas) {
    proc_commute_data <- commute_data[commute_data$area_work == proc_area, ]
    proc_employee_data <- employee_data[employee_data$area == proc_area, ]
    proc_employer_data <- employer_data[employer_data$area == proc_area, ]
    
    all_commute_data <- append(all_commute_data, list(proc_commute_data))
    all_employers[[proc_area]] <- create_employers(proc_employer_data)
    all_employees[[proc_area]] <- sum(proc_employee_data$employee_number)
  }

  all_commute_data <- do.call(rbind, all_commute_data)
  
  print("Assign home and work locations ...")
  base_pop <- travel_between_home_and_work(
    all_commute_data, pop_data, all_employees
  )
  
  print("Assign employers ...")
  base_pop <- assign_employees_employers_to_base_pop(base_pop, all_employers, employee_data)
  
  return(base_pop)
}


#' Create Available Employers
#'
#' This function generates a list of employer identifiers based on input data.
#' Each employer identifier is constructed from the business code, a unique
#' employer ID, and the area in which the employer is located. The number
#' of employers generated is controlled by the employer number factor.
#' 
#' @param employer_input A DataFrame containing data on employers, which must
#'        include columns for the employer number, business code, and area.
#' @param employer_num_factor A numeric value (default is 1.0) that serves
#'        as a divisor for the employer number, allowing for the reduction
#'        of the number of employers generated. A value greater than 1 will
#'        result in fewer employers being created.
#' 
#' @return A list of strings representing the generated employer identifiers,
#'         formatted as "business_code_employer_id_area".
#' 
#' @examples
#' employer_input <- data.frame(
#'   employer_number = c(5, 10),
#'   business_code = c("A001", "B002"),
#'   area = c("Area1", "Area2")
#' )
#' create_employers(employer_input, employer_num_factor = 2.0)
#'
create_employers <- function(employer_input, employer_num_factor = 1.0) {
  # Create available employers
  employers <- list()
  
  for (i in seq_len(nrow(employer_input))) {
    proc_row <- employer_input[i, ]
    proc_employer_num <- as.integer(proc_row$employer_number / employer_num_factor)
    proc_employer_code <- proc_row$business_code
    proc_employer_area <- proc_row$area
    
    for (employer_id in seq_len(proc_employer_num) - 1) {
      employers <- append(employers, sprintf("%s_%d_%s", proc_employer_code, employer_id, proc_employer_area))
    }
  }
  
  return(employers)
}


#' Assign Employers to Base Population
#'
#' This function assigns a company (employer) to each individual in the base
#' population based on their work area and corresponding employee data.
#' The assignment is made by sampling from available employers that match
#' the business sector of the individual, considering their work area.
#'
#' @param base_pop A DataFrame representing the base population that needs
#'        to be updated with employer information.
#' @param all_employers A list containing employers indexed by work areas,
#'        e.g., `{110400: [110400_N_4, 110400_N_5, 110400_S_4, ...], ...}`.
#' @param employee_data A DataFrame containing information about employees,
#'        including their associated business codes and the percentage of
#'        employees in each sector.
#'
#' @return A DataFrame representing the updated base population, now including
#'         the assigned company for each individual.
#'
#' @examples
#' updated_base_pop <- assign_employees_employers_to_base_pop(base_pop, all_employers, employee_data)
#'
assign_employees_employers_to_base_pop <- function(base_pop, all_employers, employee_data) {
  # Assign employer/company to base population
  
  # Args:
  # base_pop: Base population to be updated
  # all_employers: List of employers, e.g., {110400: [110400_N_4, 110400_N_5, 110400_S_4, ...], ...}
  
  # Returns:
  # DataFrame: Updated population
  
  process_row <- function(proc_row, employee_data, all_employers) {
    if (proc_row$area_work == -9999) {
      return(NA)  # Return NA for invalid data
    }
    
    proc_area_work <- proc_row$area_work
    proc_employee_data <- employee_data[employee_data$area == proc_area_work, ]
    
    tries <- 0
    while (TRUE) {
      if (tries > 5) {
        break
      }
      tryCatch({
        # Sample a business code based on employee_number_percentage
        possible_work_sector <- sample(
          proc_employee_data$business_code, 
          1, 
          prob = proc_employee_data$employee_number_percentage
        )
        
        possible_employers <- all_employers[[proc_area_work]]
        
        output_employer <- sample(
          possible_employers[sapply(possible_employers, function(x) grepl(possible_work_sector, x))], 
          1
        )
        return(as.character(output_employer))
      }, error = function(e) {
        tries <- tries + 1
        next
      })
    }
    return("Unknown")
  }

  base_pop$company <- NA
  for (i in seq_len(nrow(base_pop))) {
    proc_row <- base_pop[i, ]
    proc_row$company <- process_row(proc_row, employee_data, all_employers)
    base_pop[base_pop$index == proc_row$index, ] <- proc_row
  }
  
  return(base_pop)
}


