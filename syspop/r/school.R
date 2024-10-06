
' School and Kindergarten Assignment Wrapper
#'
#' This function assigns individuals from the population to schools or kindergartens based on geographic 
#' location and age, while respecting school capacity limits. It also handles the geographic hierarchy, 
#' creating school names, and updates population and address data accordingly.
#'
#' @param data_type Character. Specifies whether to process "school" or "kindergarten" data.
#' @param school_data DataFrame. Contains data about schools or kindergartens, including location, capacity, and age ranges.
#' @param pop_data DataFrame. Contains population data, including the ages and geographic locations of individuals.
#' @param address_data DataFrame. Contains address data (including latitude and longitude), where assigned schools or kindergartens will be added.
#' @param geography_hierarchy_data DataFrame. Provides the geographic hierarchy linking areas to super areas and regions.
#' @param possible_area_levels Character vector. A list of possible geographic area levels to consider, with default options: 
#'    \code{c("area", "super_area", "region")}.
#' @param max_students_factor Numeric. A multiplier applied to increase the maximum capacity of schools, with a default value of 1.5.
#'
#' @details This function:
#' \itemize{
#'   \item Assigns individuals from the population to the nearest available school or kindergarten based on the geographic hierarchy.
#'   \item Respects the maximum student capacity of each school.
#'   \item Filters schools based on whether they have room for more students and whether the individual's age fits within the school's age range.
#'   \item Updates and saves population data with assigned schools and address data with the coordinates of schools.
#' }
#'
#' The function also prints out progress at every 1000th individual being processed, including the percentage of missing individuals 
#' who could not be assigned to a school or kindergarten.
#'
#' @return A list with two elements:
#' \item{\code{pop_data}}{The updated population data, where each individual has been assigned a school or kindergarten.}
#' \item{\code{address_data}}{The updated address data, where new schools or kindergartens have been added.}
#'
#' @examples
#' \dontrun{
#' result <- school_and_kindergarten_wrapper(
#'   data_type = "school",
#'   school_data = school_data_frame,
#'   pop_data = population_data_frame,
#'   address_data = address_data_frame,
#'   geography_hierarchy_data = geo_hierarchy_frame
#' )
#' }
school_and_kindergarten_wrapper <- function(
    data_type,  # school or kindergarten
    school_data, 
    pop_data, 
    address_data, 
    geography_hierarchy_data,
    possible_area_levels = c("area", "super_area", "region"),
    max_students_factor = 1.5
) {
  
  start_time <- now(tz = "UTC")
  pop_data[[data_type]] <- NA
  
  # School data update
  school_data <- create_school_names(school_data)
  school_data <- school_data %>%
    mutate(age_min = ifelse(sector == "kindergarten", 2, age_min)) %>%
    filter(max_students > 0) %>%
    mutate(max_students = pmax(max_students * max_students_factor, 50)) %>%
    mutate(max_students = as.integer(max_students))
  
  # Define age range
  age_range <- list(
    min = min(school_data$age_min, na.rm = TRUE),
    max = max(school_data$age_max, na.rm = TRUE)
  )
  
  # Filter population data based on age range
  school_population <- pop_data %>%
    filter(age > age_range$min & age < age_range$max) %>%
    as.data.frame()
  
  # Attach super area to population and school data
  school_population <- merge(
    school_population, geography_hierarchy_data[c("area", "super_area", "region")],
    by = "area", all.x = TRUE
  )
  
  school_data <- merge(
    school_data, geography_hierarchy_data[c("area", "super_area", "region")],
    by = "area", all.x = TRUE
  )
  
  pop_data <- merge(
    pop_data, geography_hierarchy_data[c("area", "super_area", "region")],
    by = "area", all.x = TRUE
  )
  
  total_school_people <- nrow(school_population)
  
  school_assigned_people <- setNames(rep(0, length(unique(school_data$school_name))), unique(school_data$school_name))
  processed_people <- list()
  full_school <- c()
  
  if (!is.null(address_data)) {
    school_address <- list(name = c(), latitude = c(), longitude = c(), area = c())
  }
  
  batch_missing_people <- 0
  total_missing_people <- 0
  
  for (i in seq_len(total_school_people)) {
    proc_people <- school_population[sample(nrow(school_population), 1), ]
    
    proc_people_location <- list(
      area = proc_people$area,
      super_area = proc_people$super_area,
      region = proc_people$region
    )
    
    proc_people_age <- proc_people$age
    
    if (i %% 1000 == 0) {
      print(
        sprintf(
          "%s processing: finished: %d/%d: %.3f%% (batch missing %.3f%%, total missing %.3f%%)",
          data_type, i, total_school_people, 100 * i / total_school_people, 
          batch_missing_people * 100 / 1000, total_missing_people * 100 / total_school_people
        )
      )
      batch_missing_people <- 0
    }
    
    repeat {
      
      proc_schools <- obtain_available_schools(
        school_data, proc_people_location, proc_people_age, possible_area_levels, full_school
      )
      
      if (is.null(proc_schools)) {
        school_population <- school_population[-which(school_population$index == proc_people$index), ]
        batch_missing_people <- batch_missing_people + 1
        total_missing_people <- total_missing_people + 1
        break
      }
      
      proc_school <- get_a_school(proc_schools, school_assigned_people)
      proc_school_name <- proc_school$school_name
      students_in_this_school <- school_assigned_people[[proc_school_name]]
      
      if (students_in_this_school < proc_school$max_students) {
        if (!is.null(address_data) && !(proc_school_name %in% school_address$name)) {
          school_address$name <- c(school_address$name, proc_school_name)
          school_address$latitude <- c(school_address$latitude, as.numeric(proc_school$latitude[1]))
          school_address$longitude <- c(school_address$longitude, as.numeric(proc_school$longitude[1]))
          school_address$area <- c(school_address$area, as.numeric(proc_school$area[1]))
        }
        
        proc_people[[data_type]] <- proc_school_name
        processed_people <- append(processed_people, list(proc_people))
        
        school_population <- school_population[-which(school_population$index == proc_people$index), ]
        school_assigned_people[[proc_school_name]] <- school_assigned_people[[proc_school_name]] + 1
        
        if (school_assigned_people[[proc_school_name]] == proc_school$max_students[1]) {
          full_school <- c(full_school, proc_school_name)
        }
        break
      }
    }
  }

  print(sprintf("Combining %s dataset ...", data_type))

  if (length(processed_people) > 0) {
    processed_school_population <- bind_rows(processed_people)
    rownames(processed_school_population) <- NULL
    
    for (i in seq_len(nrow(processed_school_population))) {
      proc_row <- processed_school_population[i, ]
      pop_data[pop_data$index == proc_row$index, ] <- proc_row
    }
  }

  pop_data <- pop_data %>% select(-super_area, -region)
  
  if (!is.null(address_data)) {
    school_address_df <- as.data.frame(school_address)
    school_address_df$type <- data_type
    address_data <- bind_rows(address_data, distinct(school_address_df))
  }
  
  print(sprintf(
    "%s processing runtime: %.3f minutes", 
    data_type, 
    as.numeric(difftime(now(tz = "UTC"), 
                        start_time, units = "mins"))))
  
  address_data$area <- as.integer(address_data$area)

  return(list(pop_data = pop_data, address_data = address_data))
}


#' Obtain Available Schools for an Individual
#'
#' This function identifies available schools for a person based on their location, age, 
#' and the school's geographic area levels (e.g., area, super_area, region). It considers 
#' the age group and filters schools that can accept the individual within the specified location.
#'
#' @param school_data DataFrame. Contains school data including the school's name, capacity, 
#'        age range (`age_min`, `age_max`), and geographic location.
#' @param proc_people_location List. A list containing the geographic location of the individual, 
#'        with keys such as `area`, `super_area`, and `region`.
#' @param proc_people_age Numeric. The age of the individual for whom a school needs to be selected.
#' @param possible_area_levels Character vector. A list of geographic area levels to search for schools, 
#'        such as \code{"area"}, \code{"super_area"}, and \code{"region"}.
#' @param processed_school Character vector. A list of schools that are already full or have been processed.
#'
#' @details
#' The function loops over the possible geographic area levels (e.g., `area`, `super_area`, `region`) and 
#' checks for available schools in each of these levels, adjusting the search range based on the individual's 
#' age group. For younger individuals (e.g., below 12 years old), it searches within a narrower geographic 
#' range compared to older individuals (e.g., high school students). The function filters out schools that 
#' have already been processed or are full.
#'
#' The function uses a helper function \code{create_area_list()} to generate a list of neighboring areas 
#' to search for available schools.
#'
#' @return A DataFrame of available schools that match the individual's age and location, or \code{NULL} if 
#' no school is available.
#'
#' @examples
#' \dontrun{
#' available_schools <- obtain_available_schools(
#'   school_data = school_data_frame,
#'   proc_people_location = list(area = 1, super_area = 2, region = 3),
#'   proc_people_age = 10,
#'   possible_area_levels = c("area", "super_area", "region"),
#'   processed_school = c("School A", "School B")
#' )
#' }
obtain_available_schools <- function(
    school_data, 
    proc_people_location, 
    proc_people_age, 
    possible_area_levels, 
    processed_school
) {
  # Helper function to create neighboring area list
  create_area_list <- function(initial_value, range_value = 100, area_range_interval = 10) {
    area_list <- c(initial_value)
    
    if (!is.null(range_value)) {
      for (i in seq(0, range_value, by = area_range_interval)) {
        area_list <- c(area_list, initial_value + i, initial_value - i)
      }
    }
    return(area_list[-c(1)])  # Remove the first element, which is the initial_value itself
  }
  
  # Loop over area levels
  for (area_key in possible_area_levels) {
    
    if (proc_people_age < 12) {  # primary, kindergarten, etc.
      if (area_key == "area") {
        area_range_value <- 50
        area_range_interval <- 10
      } else if (area_key == "super_area") {
        area_range_value <- 20
        area_range_interval <- 10
      } else if (area_key == "region") {
        area_range_value <- NULL
        area_range_interval <- NULL
      }
    } else {  # high school, etc.
      if (area_key == "area") {
        area_range_value <- 100
        area_range_interval <- 10
      } else if (area_key == "super_area") {
        area_range_value <- 50
        area_range_interval <- 10
      } else if (area_key == "region") {
        area_range_value <- NULL
        area_range_interval <- NULL
      }
    }
    
    # Create possible area values based on the location and the age group
    possible_area_values <- create_area_list(
      initial_value = proc_people_location[[area_key]],
      range_value = area_range_value,
      area_range_interval = area_range_interval
    )
    
    # Loop over the possible area values
    for (possible_area_value in possible_area_values) {
      
      proc_schools <- school_data %>%
        filter(
          !!sym(area_key) == possible_area_value &
            age_min <= proc_people_age &
            age_max >= proc_people_age &
            !school_name %in% processed_school
        )
      
      if (nrow(proc_schools) > 0) {
        return(proc_schools)
      }
    }
  }
  
  return(NULL)
}


#' Get a School Based on Occupancy
#'
#' This function selects a school from a given list based on its occupancy. If the `use_random` 
#' parameter is set to TRUE, a random school will be selected. Otherwise, the school with the 
#' smallest occupancy ratio will be chosen, which is calculated as the number of assigned people 
#' divided by the maximum capacity of the school.
#'
#' @param schools_to_choose DataFrame. A DataFrame containing the list of schools to choose from, 
#'        with at least the following columns: `school_name` (name of the school) and 
#'        `max_students` (maximum capacity of the school).
#' @param school_assigned_people List. A named list where the names are school names and the values 
#'        are the number of people currently assigned to each school.
#' @param use_random Logical. If TRUE, the function will randomly select a school from the provided 
#'        list instead of selecting the one with the smallest occupancy. Defaults to FALSE.
#'
#' @return DataFrame. A DataFrame containing the details of the selected school.
#'
#' @details
#' The function first checks the `use_random` parameter. If set to TRUE, it randomly selects 
#' one school from the list `schools_to_choose`. If set to FALSE, it calculates the occupancy 
#' ratio for each school in the provided list, then selects and returns the school with the 
#' lowest occupancy ratio. This approach helps in ensuring that schools with less occupancy 
#' are prioritized for new assignments.
#'
#' @examples
#' \dontrun{
#' selected_school <- get_a_school(
#'   schools_to_choose = schools_df,
#'   school_assigned_people = list("School A" = 20, "School B" = 15),
#'   use_random = FALSE
#' )
#' }
get_a_school <- function(schools_to_choose, school_assigned_people, use_random = FALSE) {
  # If use_random is TRUE, select a random school
  if (use_random) {
    return(schools_to_choose[sample(nrow(schools_to_choose), 1), ])
  }
  
  # Calculate the occupancy ratio for each school
  proc_school_ratio <- sapply(1:nrow(schools_to_choose), function(i) {
    proc_school_name <- schools_to_choose$school_name[i]
    school_assigned_people[[proc_school_name]] / schools_to_choose$max_students[i]
  })
  
  # Find the school with the smallest occupancy ratio
  selected_school_name <- schools_to_choose$school_name[which.min(proc_school_ratio)]
  
  # Return the selected school
  return(schools_to_choose[schools_to_choose$school_name == selected_school_name, ])
}
