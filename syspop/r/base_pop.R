#' Create Base Population for a Given Area and Age
#'
#' This function generates a synthetic population for a specified area and age group,
#' randomly assigning gender and ethnicity based on provided probability distributions.
#'
#' @param output_area Character string representing the geographic area.
#' @param output_age Integer representing the age group.
#' @param df_gender_melt Data frame containing gender probabilities.
#' @param df_ethnicity_melt Data frame containing ethnicity probabilities.
#' @param ref_population Character string indicating the reference population ("gender" or "ethnicity"). Defaults to "gender".
#'
#' @return A data frame representing the synthetic population with columns for area, age, gender, and ethnicity.
#'
#' @details
#' If the total number of individuals in the specified area and age group is zero, an empty list is returned.
#' If the reference population is not "gender" or "ethnicity", an error is thrown.
#'
#' @examples
#' create_pop("Area1", 25, df_gender_melt, df_ethnicity_melt)
#' 
create_pop <- function(output_area, output_age, df_gender_melt, df_ethnicity_melt, ref_population = "gender") {

  gender_probs <- df_gender_melt %>% 
    filter(area == output_area, age == output_age) %>% 
    select(gender, prob, count)
  
  ethnicity_probs <- df_ethnicity_melt %>% 
    filter(area == output_area, age == output_age) %>% 
    select(ethnicity, prob, count)
  
  # Determine the number of individuals for the current output_area and age
  if (ref_population == "gender") {
    n_individuals <- sum(gender_probs$count)
  } else if (ref_population == "ethnicity") {
    n_individuals <- sum(ethnicity_probs$count)
  } else {
    stop("Total people must be within [gender, ethnicity]")
  }
  
  if (n_individuals == 0) {
    return(list())
  }
  
  # Randomly assign gender and ethnicity to each individual
  genders <- sample(gender_probs$gender, size = n_individuals, prob = gender_probs$prob, replace = TRUE)
  
  ethnicities <- sample(ethnicity_probs$ethnicity, size = n_individuals, prob = ethnicity_probs$prob, replace = TRUE)
  
  # Create population data frame
  population <- data.frame(
    area = rep(output_area, n_individuals),
    age = rep(output_age, n_individuals),
    gender = genders,
    ethnicity = ethnicities
  )
  
  return(population)
}

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
#' @return A data frame containing the generated base population.
#' @export
#' 
base_pop_wrapper <- function(
    gender_data,
    ethnicity_data,
    output_area_filter = NULL,
    ref_population = "gender"
) {

  # Filter data by output area if provided
  if (!is.null(output_area_filter)) {
    gender_data <- gender_data %>% filter(area %in% output_area_filter)
    ethnicity_data <- ethnicity_data %>% filter(area %in% output_area_filter)
  }
  
  # Melt data
  df_gender_melt <- gender_data %>% 
    pivot_longer(cols = -c(area, gender), names_to = "age", values_to = "count")
  
  df_ethnicity_melt <- ethnicity_data %>% 
    pivot_longer(cols = -c(area, ethnicity), names_to = "age", values_to = "count")
  
  # Normalize data
  df_gender_melt <- df_gender_melt %>% 
    group_by(area, age) %>% 
    mutate(prob = count / sum(count))
  
  df_ethnicity_melt <- df_ethnicity_melt %>% 
    group_by(area, age) %>% 
    mutate(prob = count / sum(count))
  
  # Initialize variables
  start_time <- Sys.time()
  population <- list()
  output_areas <- unique(df_gender_melt$area)
  total_output_area <- length(output_areas)
  
  # Loop through each output area and age
  for (i in seq_along(output_areas)) {
    print(paste0("Base population: ", i, "/", total_output_area, " (", round(i * 100 / total_output_area, 2), "%)"))

    for (age in unique(df_gender_melt$age)) {
      result <- create_pop(output_areas[i], age, df_gender_melt, df_ethnicity_melt, ref_population = ref_population)
      population <- rbind(population, result)
    }
  }
  
  # Calculate processing time
  end_time <- Sys.time()
  total_mins <- as.numeric(end_time - start_time, units = "mins")
  print(paste0("Processing time (base population): ", round(total_mins, 2)))

  population$age <- as.integer(population$age)
  
  return(population)
}