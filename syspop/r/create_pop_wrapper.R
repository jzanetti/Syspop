
source("syspop/r/base_pop.R")
source("syspop/r/household.R")
source("syspop/r/work.R")
source("syspop/r/school.R")
source("syspop/r/shared_space.R")

#' Create Base Population
#' 
#' This function generates a base population dataset based on specified gender, ethnicity, and synthetic areas.
#' The resulting dataset is saved in Parquet format at the specified temporary data path.
#' 
#' @param tmp_data_path A string representing the file path where the resulting base population data should be saved.
#' @param pop_gender A character vector indicating the genders to be included in the base population.
#' @param pop_ethnicity A character vector specifying the ethnicities to be included in the base population.
#' @param syn_areas A character vector of synthetic area identifiers for the base population.
#' @param ref_population A string that defines the reference population; defaults to "gender". 
#'                       This can be adjusted based on the requirements of the population generation.
#' 
#' @return NA. The function saves the generated base population data to the specified file path.
#' 
create_base_pop <- function(
    tmp_dir,
    pop_structure,
    syn_areas
) {
  # Create base population
  output <- base_pop_wrapper(pop_structure, syn_areas)
  write_parquet(output, file.path(tmp_dir, "syspop_base.parquet"))
  
  # Create initial address
  base_address <- data.frame(type = character(), 
                             name = character(), 
                             latitude = numeric(), 
                             longitude = numeric(), 
                             stringsAsFactors = FALSE)
  write_parquet(base_address, file.path(tmp_dir, "syspop_location.parquet"))
}


#' Create Household Function
#'
#' This function reads base population and location data from specified 
#' Parquet files, processes household data to assign households to 
#' the base population, and updates the base population and address data 
#' with the processed results. The updated data is then written back 
#' to Parquet files.
#'
#' @param tmp_dir A character string specifying the directory path where 
#' the input Parquet files are located and where the output files will be 
#' saved.
#' @param household_data A data frame containing the household data to be 
#' processed, which includes information about households and their 
#' compositions.
#' @param geo_address_data A data frame containing geographical address 
#' data to be optionally used for assigning random addresses to households.
#' 
#' @return This function does not return a value. It writes the updated 
#' base population and address data to Parquet files in the specified 
#' temporary directory.
#'
#' @examples
#' create_household("path/to/tmp_dir", household_data, geo_address_data)
#'
#' @importFrom arrow read_parquet write_parquet
#' @export
create_household <- function(
    tmp_dir,
    household_data,
    geo_address_data) {

  # Read base population
  base_pop <- read_parquet(file.path(tmp_dir, "syspop_base.parquet"))
  
  # Read location data
  base_address <- read_parquet(file.path(tmp_dir, "syspop_location.parquet"))
  
  # add index for household data
  household_data$index <- seq_len(nrow(household_data))
  
  # create household composition
  output <- household_wrapper(
    household_data,
    base_pop,
    base_address,
    geo_address_data=geo_address_data
  )

  write_parquet(output$base_pop, file.path(tmp_dir, "syspop_base.parquet"))
  write_parquet(output$base_address, file.path(tmp_dir, "syspop_location.parquet"))
}


#' Create Work Data for Population
#'
#' This function orchestrates the reading of population and location data,
#' updates them by assigning employers and addresses, and then writes the
#' updated datasets back to parquet files.
#'
#' @param tmp_dir A string representing the temporary directory path where
#'        input and output files are stored.
#' @param employer_data A DataFrame containing data about employers,
#'        including their business codes and area information.
#' @param employee_data A DataFrame containing data about employees,
#'        including business codes and employee numbers per area.
#' @param travel_to_work_data A DataFrame with data about commuting patterns
#'        and travel methods between home and work.
#' @param geo_hierarchy A DataFrame representing the geographical hierarchy
#'        data for the population.
#' @param geo_address A DataFrame containing address data, used for assigning
#'        random addresses to the population if provided.
#'
#' @return None. This function writes the updated base population and address
#'         data to parquet files in the specified temporary directory.
#'
#' @examples
#' create_work(tmp_dir = "path/to/tmp", employer_data, employee_data,
#'              travel_to_work_data, geo_hierarchy, geo_address)
#'
create_work <- function(
    tmp_dir, 
    employer_data,
    employee_data,
    travel_to_work_data, 
    geo_hierarchy, 
    geo_address
) {

  # Read base population
  base_pop <- read_parquet(file.path(tmp_dir, "syspop_base.parquet"))
  
  # Read location data
  base_address <- read_parquet(file.path(tmp_dir, "syspop_location.parquet"))

  # Call work_and_commute_wrapper to update base_pop and base_address
  output <- work_and_commute_wrapper(
    employer_data,
    employee_data,
    base_pop,
    base_address,
    travel_to_work_data,
    geo_hierarchy,
    geo_address_data = geo_address
  )

  write_parquet(output$base_pop, file.path(tmp_dir, "syspop_base.parquet"))
  write_parquet(output$base_address, file.path(tmp_dir, "syspop_location.parquet"))
}


#' Create School and Kindergarten Information
#'
#' This function reads base population and location data, then processes school and kindergarten information 
#' for individuals based on the provided geographic hierarchy. The results are saved back to the respective 
#' parquet files for base population and location data.
#'
#' @param data_type Character. Specifies the type of data to process (e.g., "school" or "kindergarten").
#' @param tmp_dir Character. The directory path where the population and location parquet files are stored.
#' @param school_data DataFrame. Contains school-related data such as school location and capacity.
#' @param geo_hierarchy_data DataFrame. Provides geographic hierarchy information (e.g., region, super_area, area).
#' @param possible_area_levels Character vector. A list of possible area levels to consider when processing 
#'     the geographic hierarchy (default: c("area", "super_area", "region")).
#'
#' @details This function reads parquet files from the specified directory (`tmp_dir`), processes school 
#' and kindergarten information using the \code{school_and_kindergarten_wrapper} function, and then saves 
#' the updated population and address information back to the directory.
#'
#' @return This function does not return a value. It modifies and writes the parquet files for base population
#' and location data in the specified directory.
#'
#' @examples
#' \dontrun{
#' create_school_and_kindergarten(
#'   data_type = "school",
#'   tmp_dir = "data/tmp",
#'   school_data = school_data_frame,
#'   geo_hierarchy_data = geo_hierarchy_frame
#' )
#' }
#' 
create_school_and_kindergarten <- function(
    tmp_dir, 
    school_data, 
    data_type,
    geo_hierarchy_data,
    possible_area_levels = c("area", "super_area", "region")
) {
  
  # Read base population
  base_pop <- read_parquet(file.path(tmp_dir, "syspop_base.parquet"))
  
  # Read location data
  base_address <- read_parquet(file.path(tmp_dir, "syspop_location.parquet"))
  
  # Call wrapper function for school and kindergarten information
  output <- school_and_kindergarten_wrapper(
    data_type,
    school_data,
    base_pop,
    base_address,
    geo_hierarchy_data,
    possible_area_levels = possible_area_levels
  )

  write_parquet(output$pop_data, file.path(tmp_dir, "syspop_base.parquet"))
  write_parquet(output$address_data, file.path(tmp_dir, "syspop_location.parquet"))
}


#' Create Shared Space
#'
#' This function creates shared space data by reading the base population
#' and location data, merging household address data, and processing
#' shared space information based on the specified parameters.
#'
#' @param tmp_dir A character string specifying the temporary directory 
#'        where input and output files are stored.
#' @param shared_space_data A data frame containing the shared space 
#'        information to be processed.
#' @param shared_space_type A character string indicating the type of 
#'        shared space to be created.
#' @param geo_location A geographic location parameter that may be used 
#'        in the processing of shared space.
#' @param assign_address_flag A logical flag indicating whether to 
#'        assign addresses in the shared space.
#' @param area_name_keys_and_selected_nums A named list where each 
#'        element represents a key for an area and the corresponding 
#'        number of nearest locations to be selected. Default is 
#'        list(area = 2).
#'
#' @return This function writes the updated population data and address 
#'         data back to parquet files in the specified temporary 
#'         directory.
#'
#' @examples
#' tmp_dir <- "path/to/tmp_dir"
#' shared_space_data <- data.frame(...)  # Provide shared space data
#' shared_space_type <- "example_type"
#' geo_location <- "example_location"
#' assign_address_flag <- TRUE
#' create_shared_space(tmp_dir, shared_space_data, shared_space_type, 
#'                     geo_location, assign_address_flag)
create_shared_space <- function(
    tmp_dir,
    shared_space_data,
    shared_space_type,
    geo_location,
    area_name_keys_and_selected_nums = list(area = 2)
) {
  
  # Read base population
  base_pop <- read_parquet(file.path(tmp_dir, "syspop_base.parquet"))
  
  # Read location data
  base_address <- read_parquet(file.path(tmp_dir, "syspop_location.parquet"))
  
  # Merge household address data
  household_address <- merge(
    base_pop[, "household", drop = FALSE],
    base_address[, c("name", "latitude", "longitude")],
    by.x = "household",
    by.y = "name",
    all.x = TRUE
  )
  
  household_address <- unique(household_address[, c("household", "latitude", "longitude")])
  
  index = 0
  for (area_name_key in names(area_name_keys_and_selected_nums)) {
    
    if (index == 0) {
      proc_base_pop <- base_pop
      proc_base_address <- base_address
    }
    else {
      proc_base_pop <- output$pop_data
      proc_base_address <- output$address_data
    }
    
    output <- shared_space_wrapper(
      shared_space_type,
      shared_space_data,
      proc_base_pop,
      proc_base_address,
      household_address,
      geo_location,
      num_nearest = area_name_keys_and_selected_nums[[area_name_key]],
      area_name_key = area_name_key
    )
    
    index <- index + 1
  }
  
  write_parquet(output$pop_data, file.path(tmp_dir, "syspop_base.parquet"))
  write_parquet(output$address_data, file.path(tmp_dir, "syspop_location.parquet"))
}



create_birthplace <- function(tmp_dir, birthplace_data) {
  
  # Load the base population data from the file
  base_pop <- read_parquet(file.path(tmp_dir, "syspop_base.parquet"))
  
  # browser()
  # Initialize birthplace column as NA
  base_pop$birthplace <- NA
  
  # Get the unique areas in the population data
  all_areas <- unique(base_pop$area)
  
  data_list <- list()
  
  # Loop through each area
  for (proc_area in all_areas) {
    
    proc_birthplace_data <- birthplace_data %>% filter(area == proc_area)
    proc_base_pop_data <- base_pop %>% filter(area == proc_area)
    
    # If there's no birthplace data for the area, assign default value 9999
    if (nrow(proc_birthplace_data) == 0) {
      proc_base_pop_data$birthplace <- 9999
    } else {
      
      # Calculate number of people from each birthplace
      proc_birthplace_data$num <- round(proc_birthplace_data$percentage * nrow(proc_base_pop_data))
      
      # Replicate the birthplace rows based on the calculated number
      proc_birthplace_data_processed <- proc_birthplace_data %>% 
        slice(rep(1:n(), num)) %>% 
        select(birthplace) %>% 
        sample_frac(1)  # Randomly shuffle rows
      
      # Get the lengths for further checks
      proc_birthplace_data_processed_length <- nrow(proc_birthplace_data_processed)
      proc_base_pop_data_length <- nrow(proc_base_pop_data)
      
      # Assign birthplace data if processed data is enough
      if (proc_birthplace_data_processed_length >= proc_base_pop_data_length) {
        
        if (proc_birthplace_data_processed_length > proc_base_pop_data_length) {
          proc_birthplace_data_processed <- proc_birthplace_data_processed %>% 
            slice_sample(n = proc_base_pop_data_length)
        }
        
        proc_base_pop_data$birthplace <- proc_birthplace_data_processed$birthplace
      } else {
        # Randomly assign birthplace data to part of the base population
        random_indices <- sample(1:nrow(proc_base_pop_data), proc_birthplace_data_processed_length, replace = FALSE)
        proc_base_pop_data$birthplace[random_indices] <- proc_birthplace_data_processed$birthplace
        
        # Assign remaining birthplace values randomly
        proc_base_pop_data$birthplace[is.na(proc_base_pop_data$birthplace)] <- sample(
          proc_birthplace_data_processed$birthplace,
          sum(is.na(proc_base_pop_data$birthplace)),
          replace = TRUE
        )
      }
    }
    
    data_list[[length(data_list) + 1]] <- proc_base_pop_data
  }
  
  # Combine all areas back into a single data frame
  pop_data <- bind_rows(data_list)
  
  # Ensure birthplace is stored as an integer
  pop_data$birthplace <- as.integer(pop_data$birthplace)
  
  # Save the modified data back to the file
  write_parquet(pop_data, file.path(tmp_dir, "syspop_base.parquet"))
}


