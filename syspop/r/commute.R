

#' Calculate the percentage of people using each specified travel method in a commute dataset.
#'
#' This function takes a dataset of commute data where each column represents the number of people 
#' using a particular travel method. It computes the percentage of people using each travel method 
#' relative to the total number of people for the given methods, adds these percentages as new columns, 
#' and returns the modified dataset.
#'
#' @param commute_dataset A data.frame containing commute data, where each row represents a different 
#'                        geographic area or time period, and columns represent the number of people 
#'                        using various travel methods.
#' @param travel_methods A character vector of column names representing different travel methods in 
#'                       the dataset.
#'
#' @return A modified data.frame with additional columns showing the percentage of people using 
#'         each travel method. The total number of people column is calculated and then dropped 
#'         after computing the percentages.
#'
#' @examples
#' # Example of usage:
#' # Assume df is a data.frame with columns 'car', 'bike', and 'walk', and those are passed in 
#' # the travel_methods vector. The function will return a dataset with new columns like 
#' # 'car_percentage', 'bike_percentage', and 'walk_percentage'.
#' 
#' # commute_dataset <- get_commute_agents_percentage(df, c('car', 'bike', 'walk'))
#'
get_commute_agents_percentage <- function(commute_dataset, travel_methods) {
  commute_dataset$total_people <- rowSums(commute_dataset[, travel_methods])
  
  # Calculate the percentage for each travel method and add new columns
  for (method in travel_methods) {
    commute_dataset[[paste0(method, "_percentage")]] <- commute_dataset[[
      method]] / commute_dataset$total_people
  }
  
  # Drop the total_people column
  commute_dataset$total_people <- NULL
  
  return(commute_dataset)
}


#' Assign commute data (home to work) to the base population
#'
#' This function assigns individuals from the base population to work areas based on commute data, 
#' while also assigning commuting methods for those individuals of working age.
#' 
#' @param commute_dataset A DataFrame containing the commute dataset, including home and work areas, 
#'        and various travel methods (e.g., bus, car, etc.).
#' @param base_pop A DataFrame containing the base population data, which includes age and area information.
#' @param all_employees A named list or dictionary-like structure that contains the number of employees 
#'        for each home area, with the home area as the key.
#' @param work_age A named list defining the working age range with `min` and `max` values. Defaults to 
#'        a minimum of 16 and a maximum of 75.
#' 
#' @return A DataFrame of the base population with updated work areas and commuting modes.
#' 
#' @details 
#' - The function processes each home area in the base population, selects people within the working 
#'   age range, and assigns them to work areas based on the commute dataset.
#' - For each individual, a work area is assigned, and then a travel method is selected based on 
#'   probabilities derived from the commute data.
#' - The function updates the `area_work` and `travel_mode_work` fields in the base population dataset.
#' 
#' @examples
#' commute_dataset <- data.frame(
#'   area_home = c(1, 1, 2),
#'   area_work = c(2, 3, 1),
#'   bus_percentage = c(0.3, 0.2, 0.4),
#'   car_percentage = c(0.7, 0.8, 0.6)
#' )
#' base_pop <- data.frame(index = 1:10, age = sample(16:75, 10, replace = TRUE), area = rep(1, 10))
#' all_employees <- list("1" = 5, "2" = 5)
#' travel_between_home_and_work(commute_dataset, base_pop, all_employees)
#'
travel_between_home_and_work <- function(
    commute_dataset, 
    base_pop, 
    all_employees, 
    work_age = list(min = 16, max = 75)
) {

  base_pop$area_work <- -9999
  base_pop$travel_mode_work <- NA
  
  working_age_people <- base_pop[
    base_pop$age >= work_age$min & base_pop$age <= work_age$max, 
  ]

  travel_methods <- setdiff(colnames(commute_dataset), c("area_home", "area_work"))
  commute_dataset <- get_commute_agents_percentage(commute_dataset, travel_methods)

  all_areas_home <- unique(base_pop$area)
  
  results <- list()
  
  for (i in seq_along(all_areas_home)) {
    print(sprintf(
      "Commute (work): %d/%d (%d%%)", 
      i, 
      length(all_areas_home), 
      as.integer(i * 100 / length(all_areas_home))))
    
    proc_home_area <- all_areas_home[i]
    proc_working_age_people <- working_age_people[working_age_people$area == proc_home_area, ]
    
    proc_working_age_people <- proc_working_age_people[
      sample(nrow(proc_working_age_people), 
             min(nrow(proc_working_age_people), all_employees[[proc_home_area]])), ]
    
    proc_working_age_people <- assign_people_between_home_and_work(
      proc_working_age_people, 
      commute_dataset, 
      proc_home_area, 
      travel_methods
    )
    
    results <- append(results, list(proc_working_age_people))
  }

  for (result in results) {
    base_pop[result$index, ] <- result
  }
  
  base_pop$area_work <- as.integer(base_pop$area_work)
  
  return(base_pop)
}

#' Assign people between home and work areas based on commuting data
#'
#' This function assigns individuals to different work areas based on commuting data 
#' and assigns travel modes for commuting. It works by splitting the population 
#' into groups based on the commuting dataset, and then sampling individuals to assign 
#' them to specific work areas and commuting modes.
#'
#' @param proc_working_age_people A DataFrame containing people of working age from the population.
#' @param commute_dataset A DataFrame containing commuting data, including home and work areas.
#' @param proc_home_area An integer specifying the home area to be processed.
#' @param travel_methods A list of commuting methods (e.g., bus, car, etc.) to be assigned.
#' 
#' @return A DataFrame of the working-age population with updated work areas and travel modes.
#' 
#' @details 
#' - The function first splits the working-age population into groups based on commuting data 
#'   and the available commuting methods.
#' - For each home area, it randomly assigns individuals to work areas based on the commuting data.
#' - It also assigns a travel mode for each individual based on probabilities provided by the 
#'   commuting dataset.
#' 
#' @examples
#' commute_dataset <- data.frame(
#'   area_home = c(1, 1, 2),
#'   area_work = c(2, 3, 1),
#'   bus_percentage = c(0.3, 0.2, 0.4),
#'   car_percentage = c(0.7, 0.8, 0.6)
#' )
#' proc_working_age_people <- data.frame(index = 1:10, age = sample(18:65, 10, replace = TRUE), area = rep(1, 10))
#' proc_home_area <- 1
#' travel_methods <- c("bus", "car")
#' assign_people_between_home_and_work(proc_working_age_people, commute_dataset, proc_home_area, travel_methods)
#'
assign_people_between_home_and_work <- function(
    proc_working_age_people, 
    commute_dataset, 
    proc_home_area, 
    travel_methods
) {

  split_people <- function(df, df_commute, travel_methods) {
    proc_commute_datasets_total_people <- sum(df_commute[, travel_methods])
    fractions <- numeric()
    
    for (i in seq_len(nrow(df_commute))) {
      fractions <- c(fractions, sum(df_commute[i, travel_methods]) / proc_commute_datasets_total_people)
    }
    
    split_dfs <- list()
    remaining_df <- df
    
    for (frac in fractions[-length(fractions)]) {  # Exclude the last fraction
      sample_df <- remaining_df[sample(nrow(remaining_df), round(nrow(remaining_df) * frac)), ]
      split_dfs <- append(split_dfs, list(sample_df))
      remaining_df <- remaining_df[!(rownames(remaining_df) %in% rownames(sample_df)), ]
    }
    
    # Add remaining rows to the last split
    split_dfs <- append(split_dfs, list(remaining_df))
    
    return(split_dfs)
  }
  
  proc_commute_dataset <- commute_dataset[commute_dataset$area_home == proc_home_area, ]
  proc_commute_dataset_work_areas <- unique(proc_commute_dataset$area_work)
  
  proc_working_age_people_in_work_area <- split_people(
    proc_working_age_people, 
    proc_commute_dataset, 
    travel_methods
  )
  
  for (i in seq_along(proc_commute_dataset_work_areas)) {
    proc_work_area <- proc_commute_dataset_work_areas[i]
    
    proc_commute <- proc_commute_dataset[proc_commute_dataset$area_work == proc_work_area, ]
    
    for (j in seq_len(nrow(proc_working_age_people_in_work_area[[i]]))) {
      proc_people <- proc_working_age_people_in_work_area[[i]][j, ]
      
      proc_people$area_work <- proc_work_area
      travel_methods_prob <- as.numeric(proc_commute[1, paste0(travel_methods, "_percentage")])
      
      tryCatch({
        proc_people$travel_mode_work <- sample(travel_methods, 1, prob = travel_methods_prob)
      }, error = function(e) {
        proc_people$travel_mode_work <- "Unknown"
      })
      proc_working_age_people[proc_working_age_people$index == proc_people$index, ] <- proc_people
    }
  }

  return(proc_working_age_people)
}

