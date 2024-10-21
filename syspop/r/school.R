#' Create School Data
#'
#' This function creates a data frame of schools filtered by specified areas.
#'
#' @param school_data A data frame containing school information with columns
#'                    'area_school', 'age_min', 'age_max', 'sector', 
#'                    'latitude', 'longitude', and 'max_students'.
#' @param max_student_num An integer specifying the maximum number of students allowed
#'                        per school. Defaults to 30.
#'
#' @return A data frame containing the filtered school data with the following columns:
#'         - area_school (int): The area code of the school
#'         - age_min (int): The minimum age for students
#'         - age_max (int): The maximum age for students
#'         - latitude (numeric): Latitude of the school location
#'         - longitude (numeric): Longitude of the school location
#'         - max_students (int): The maximum number of students
#'         - id (str): A unique identifier for the school
#'
#' @import dplyr
#' @import uuid
#'
create_school <- function(school_data, max_student_num = 30) {
  
  schools <- list()  # Initialize an empty list to store school records
  # Loop through each row in the school data

  for (i in 1:nrow(school_data)) {
    row <- school_data[i, ]
    area <- row$area
    age_min <- row$age_min
    age_max <- row$age_max
    sector <- row$sector
    latitude <- row$latitude
    longitude <- row$longitude
    max_students <- row$max_students
    
    # Adjust max_students if it is less than max_student_num
    if (max_students < max_student_num) {
      max_students <- max_student_num
    }
    
    # Create a unique ID for the school
    name <- paste0(sector, "_", substr(UUIDgenerate(), 1, 6))
    
    # Append the school record to the list
    schools[[length(schools) + 1]] <- list(
      area_school = as.integer(area),
      age_min = as.integer(age_min),
      age_max = as.integer(age_max),
      latitude = as.numeric(latitude),
      longitude = as.numeric(longitude),
      max_students = as.integer(max_students),
      school = as.character(name)
    )
  }
  # Convert the list of school records to a data frame
  return(bind_rows(schools))
}
