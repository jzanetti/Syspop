
source("syspop/r/base_pop.R")
source("syspop/r/household.R")
source("syspop/r/work.R")

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
#' @return NULL. The function saves the generated base population data to the specified file path.
#' 
create_base_pop <- function(
    tmp_dir,
    pop_gender,
    pop_ethnicity,
    syn_areas,
    ref_population = "gender"
) {
  # Create base population
  output <- base_pop_wrapper(pop_gender, pop_ethnicity, syn_areas, ref_population = ref_population)
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
  
  household_data$index <- seq_len(nrow(household_data))
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

