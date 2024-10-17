#' Create Base Population
#'
#' This function generates a synthetic base population by repeating rows in the
#' input data frame based on the values specified in the "value" column.
#' It filters the data frame based on the specified area if a filter is provided.
#' The output includes an index column representing the row number.
#'
#' @param structure_data A data frame containing the population structure data.
#'                       It must include a column named "area" and a column named "value".
#' @param output_area_filter An optional vector of area IDs to filter the rows of
#'                           the structure_data. If NULL, no filtering is applied.
#'
#' @return A data frame containing the generated base population with repeated rows.
#'         The "value" column is removed, and a new column "index" is added that
#'         contains the row numbers.
#'
#' @examples
#' # Example of using the base_pop_wrapper function
#' data <- data.frame(area = c(241300, 241300, 241301),
#'                    value = c(12, 9, 15))
#' population <- base_pop_wrapper(data, output_area_filter = c(241300))
#' print(population)
#'
base_pop_wrapper <- function(
    structure_data, 
    output_area_filter = NULL
) {

  # Start the timer
  start_time <- now(tz = "UTC")
  
  # Filter based on output_area_filter if provided
  if (!is.null(output_area_filter)) {
    structure_data <- structure_data[structure_data$area %in% output_area_filter, ]
  }

  # Repeat rows based on the "value" column and drop the "value" column
  if (!is.null(structure_data)) {
    
    population <- structure_data %>%
      as.data.frame() %>%  # Ensure we are working with a data frame
      .[rep(1:nrow(.), times = as.integer(.$value)), ] %>%
      select(-value)  # Remove the value column
    
    rownames(population) <- NULL

    population <- population %>%
      mutate(index = row_number())
  }
  
  # End the timer
  end_time <- now(tz = "UTC")
  
  # Calculate the processing time in minutes
  total_mins <- round(as.numeric(difftime(end_time, start_time, units = "mins")), 2)
  
  # Log the processing time
  print(sprintf("Processing time (base population): %.2f minutes", total_mins))
  
  return(population)
}
