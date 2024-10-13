#' Base Population Wrapper Function
#'
#' This function generates a base population dataset by combining gender and ethnicity data.
#'
#' @param gender_data Data frame containing gender information.
#' @param ethnicity_data Data frame containing ethnicity information.
#' @param output_area_filter Optional vector of output areas to filter data by.
#' @param ref_population Reference population, defaults to "gender".
#'
#' @details
#' The function performs the following steps:
#'  1. Filters data by output area if provided.
#'  2. Melts data into long format.
#'  3. Normalizes data by calculating probabilities.
#'  4. Loops through each output area and age to create the base population.
#'  
#'  The output looks like:
#'       area ethnicity   age gender index
#'     <int> <chr>     <int> <chr>  <int>
#'   1 241400 European      0 female     1
#'   2 241400 European      0 female     2
#'   3 241400 European      0 female     3
#'   4 241400 European      0 female     4
#'   ....
#'
#' @return A data frame containing the generated base population.
#' @export
#' 
base_pop_wrapper <- function(
    structure_data,
    output_area_filter = NULL
) {
  start_time <- Sys.time()
  # Filter data by output area if provided
  if (!is.null(output_area_filter)) {
    structure_data <- structure_data %>% filter(area %in% output_area_filter)
  }
  
  population <- structure_data %>%
    uncount(weights = as.integer(value))  # Repeat rows by "value" column
  population = population[ , !(names(population) %in% c("value"))]
  
  population$age <- as.integer(population$age)
  population$index <- seq_len(nrow(population))

  # Calculate processing time
  end_time <- Sys.time()
  total_mins <- as.numeric(end_time - start_time, units = "mins")
  print(paste0("Processing time (base population): ", round(total_mins, 2)))
  
  return(population)
}