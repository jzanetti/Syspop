source("syspop/r/address.R")


#' Assign Remaining Adults and Children to Existing Households
#'
#' This function randomly assigns remaining unassigned adults and children to 
#' existing households in the provided base population dataset. It allows for
#' the specification of whether to assign remaining children and adults 
#' through logical flags. The assignment respects the existing household 
#' structures while ensuring that the total number of adults and children 
#' assigned does not exceed the available pool.
#'
#' @param proc_base_pop A data frame containing the base population information, 
#'                      including existing households, dwelling types, and 
#'                      sources.
#' @param adults A data frame of unassigned adults, which should contain an 
#'               'index' column used for referencing individuals.
#' @param children A data frame of unassigned children, which should also contain
#'                 an 'index' column for referencing.
#' @param assign_children A logical value indicating whether to assign remaining 
#'                        children. Defaults to TRUE.
#' @param assign_adults A logical value indicating whether to assign remaining 
#'                      adults. Defaults to TRUE.
#'
#' @return A data frame of the base population with updated household assignments 
#'         for any assigned adults and children.
#'
#' @examples
#' # Assuming proc_base_pop, adults, and children are already defined data frames
#' updated_population <- assign_any_remained_people(proc_base_pop, adults, children)
#'
#' @export
#' 
assign_any_remained_people <- function(
    proc_base_pop,
    adults,
    children,
    assign_children = TRUE,
    assign_adults = TRUE
) {
  # Randomly assign remaining people to existing households
  
  # Get unique existing households, excluding "NaN" and NA values
  existing_households <- unique(proc_base_pop$household)
  existing_households <- existing_households[!is.na(existing_households) & existing_households != "NaN"]
  
  while (nrow(adults) > 0 && assign_adults) {
    household_id <- sample(existing_households, 1)
    
    num_adults_to_add <- sample(0:2, 1)  # Randomly choose 0, 1, or 2
    
    if (num_adults_to_add > nrow(adults)) {
      num_adults_to_add <- nrow(adults)
    }
    
    adult_ids <- sample(adults$index, num_adults_to_add)
    proc_base_pop$household[proc_base_pop$index %in% adult_ids] <- household_id
    adults <- adults[!adults$index %in% adult_ids, ]
  }
  
  while (nrow(children) > 0 && assign_children) {
    household_id <- sample(existing_households, 1)
    
    num_children_to_add <- sample(0:2, 1)  # Randomly choose 0, 1, or 2
    
    if (num_children_to_add > nrow(children)) {
      num_children_to_add <- nrow(children)
    }
    
    children_ids <- sample(children$index, num_children_to_add)
    proc_base_pop$household[proc_base_pop$index %in% children_ids] <- household_id
    
    children <- children[!children$index %in% children_ids, ]
  }
  
  return(proc_base_pop)
}


#' Assign Household and Dwelling ID to Individuals
#'
#' This function assigns a specified household ID to a list of adult and child
#' indices within the base population dataset. It updates the 'household' 
#' column of the provided data frame for the specified adult and child IDs.
#'
#' @param proc_base_pop A data frame containing the base population information, 
#'                      which includes a column for household assignments.
#' @param household_id A character string representing the household ID to be 
#'                     assigned to the specified individuals.
#' @param adult_ids A numeric vector containing the indices of adults to whom 
#'                  the household ID will be assigned.
#' @param children_ids A numeric vector containing the indices of children to 
#'                     whom the household ID will be assigned.
#'
#' @return A data frame of the base population with updated household 
#'         assignments for the specified adult and child indices.
#'
#' @examples
#' # Assuming proc_base_pop is a defined data frame and 
#' # household_id, adult_ids, and children_ids are defined
#' updated_population <- assign_household_and_dwelling_id(proc_base_pop, 
#'                                                          household_id, 
#'                                                          adult_ids, 
#'                                                          children_ids)
#'
#' @export
#' 
assign_household_and_dwelling_id <- function(
    proc_base_pop,
    household_id,
    adult_ids,
    children_ids
) {
  # Assign the household and dwelling ID
  
  if (length(adult_ids) > 0) {
    proc_base_pop$household[proc_base_pop$index %in% adult_ids] <- as.character(household_id)
  }
  
  if (length(children_ids) > 0) {
    proc_base_pop$household[proc_base_pop$index %in% children_ids] <- as.character(household_id)
  }

  return(proc_base_pop)
}


obtain_adult_index_based_on_ethnicity <- function(
    unassigned_adults, 
    proc_household_composition, 
    ref_ethnicity_prob = 0.7) {
  # Obtain adult index based on ethnicity
  
  # Sample a reference adult
  ref_adult <- unassigned_adults[sample(1:nrow(unassigned_adults), 1), ]
  remained_adults <- unassigned_adults[unassigned_adults$index != ref_adult$index, ]
  
  # Initialize list of adult ids
  adult_ids <- as.list(ref_adult$index)
  
  # Define probabilities for sampling based on ethnicity
  probabilities <- ifelse(
    remained_adults$ethnicity == ref_adult$ethnicity, 
    ref_ethnicity_prob, 
    1 - ref_ethnicity_prob)
  probabilities <- probabilities / sum(probabilities)
  
  # Try to sample more adults, if possible
  tryCatch({
    selected_adults <- remained_adults[sample(
      1:nrow(remained_adults), 
      proc_household_composition$adults[1] - 1, 
      prob = probabilities, 
      replace = FALSE
    ), ]
    adult_ids <- c(adult_ids, selected_adults$index)
  }, error = function(e) {
    # Handle case where there aren't enough adults to sample
  })
  
  # Return adult ids and ethnicity of the reference adult
  return(list(adult_ids = adult_ids, ref_ethnicity = ref_adult$ethnicity))
}

#' Sort Household Dataset by Percentage
#'
#' This function sorts a household dataset by randomly selecting a row 
#' based on the 'percentage' column, after excluding specified rows by their indices.
#'
#' @param proc_household_dataset A data frame containing household data, 
#'                               including a 'percentage' column used for sampling.
#' @param exclude_row_indices A numeric vector of indices to exclude from the dataset.
#'
#' @return A data frame with a single randomly selected row based on the 'percentage' 
#'         column, or NULL if the dataset is empty after exclusions.
#'
#' @examples
#' # Assuming proc_household_dataset is a defined data frame with a 'percentage' column
#' selected_row <- get_current_household_composition(proc_household_dataset, exclude_row_indices = c(1, 3))
#'
#' @export
#' 
get_current_household_composition <- function(proc_household_dataset, exclude_row_indices) {
  # Sorts the household dataset by randomly selecting a row based on the 'percentage' column,
  # after excluding specified rows.
  
  # Exclude specified rows
  if (length(exclude_row_indices) > 0) {
    proc_household_dataset <- proc_household_dataset[!proc_household_dataset$index %in% exclude_row_indices, ]
  }
  
  # Return NULL if the dataset is empty after exclusion
  if (nrow(proc_household_dataset) == 0) {
    return(NULL)
  }
  
  # Sample one row based on weights from the 'percentage' column
  sample_row <- proc_household_dataset[sample(nrow(proc_household_dataset), 1, 
                                              prob = proc_household_dataset$percentage), ]
  
  return(sample_row)
}


#' Sort Household Dataset by Percentage
#'
#' This function sorts a household dataset by randomly selecting a row 
#' based on the 'percentage' column, after excluding specified rows by their indices.
#'
#' @param proc_household_dataset A data frame containing household data, 
#'                               including a 'percentage' column used for sampling.
#' @param exclude_row_indices A numeric vector of indices to exclude from the dataset.
#'
#' @return A data frame with a single randomly selected row based on the 'percentage' 
#'         column, or NULL if the dataset is empty after exclusions.
#'
#' @examples
#' # Assuming proc_household_dataset is a defined data frame with a 'percentage' column
#' selected_row <- sort_household_v2(proc_household_dataset, exclude_row_indices = c(1, 3))
#'
#' @export
#' 
create_household_composition <- function(
    proc_household_dataset,
    proc_base_pop,
    proc_area,
    only_households_with_adults = TRUE
) {
  if (only_households_with_adults) {
    proc_household_dataset <- proc_household_dataset[proc_household_dataset$adults > 0, ]
  }
  
  unassigned_adults <- proc_base_pop[proc_base_pop$age >= 18, ]
  unassigned_children <- proc_base_pop[proc_base_pop$age < 18, ]
  
  unique_base_pop_ethnicity <- unique(proc_base_pop$ethnicity)
  
  household_id <- 0
  exclude_hhd_composition_indices <- c()
  while (TRUE) {

    proc_household_composition <- get_current_household_composition(
      proc_household_dataset, exclude_hhd_composition_indices)
    
    household_id <- substr(uuid::UUIDgenerate(), 1, 6)
    
    if (is.null(proc_household_composition)) {
      break
    }
    
    if (nrow(unassigned_adults) < proc_household_composition$adults ||
        nrow(unassigned_children) < proc_household_composition$children) {
      exclude_hhd_composition_indices <- c(
        exclude_hhd_composition_indices, proc_household_composition$index)
      next
    }
    
    proc_adult <- obtain_adult_index_based_on_ethnicity(
      unassigned_adults,
      proc_household_composition
    )
    
    children_ids <- tryCatch({
      sample(unassigned_children$index[unassigned_children$ethnicity == proc_adult$ref_ethnicity],
             proc_household_composition$children[1])
    }, error = function(e) {
      sample(unassigned_children$index, proc_household_composition$children[1])
    })
    

    proc_base_pop <- assign_household_and_dwelling_id(
      proc_base_pop,
      paste0("household_", proc_area, "_", length(proc_adult$adult_ids), "-", length(children_ids), "_", household_id),
      proc_adult$adult_ids,
      children_ids
    )

    unassigned_adults <- unassigned_adults[!unassigned_adults$index %in% proc_adult$adult_ids, ]
    unassigned_children <- unassigned_children[!unassigned_children$index %in% children_ids, ]
  }

  proc_base_pop <- assign_any_remained_people(
    proc_base_pop, unassigned_adults, unassigned_children
  )
  
  return(proc_base_pop)
}

#' Household Wrapper Function
#'
#' This function processes household data and populates the base population 
#' data with household and dwelling information. It also optionally adds 
#' random address data to the base population.
#'
#' @param household_dataset A data frame containing household data, including 
#' areas and household composition.
#' @param base_pop A data frame representing the base population that will be 
#' modified with household information.
#' @param base_address A data frame to store address data, which will be 
#' updated with random addresses if geo_address_data is provided.
#' @param geo_address_data (Optional) A data frame containing geographical 
#' address data to be used for adding random addresses to the base population.
#' 
#' @return A list containing two elements:
#' \item{base_pop}{The updated base population data frame with household and 
#' dwelling information.}
#' \item{base_address}{The updated address data frame, including random 
#' addresses if geo_address_data was provided.}
#'
#' @examples
#' result <- household_wrapper(household_dataset, base_pop, base_address, 
#'                              geo_address_data)
#'
#' @importFrom stats runif
#' @export
#' 
household_wrapper <- function(
    household_dataset,
    base_pop,
    base_address,
    geo_address_data = NULL
) {
  
  start_time <- Sys.time()
  
  base_pop$household <- NA
  all_areas <- unique(base_pop$area)
  total_areas <- length(all_areas)
  results <- list()

  household_dataset <- household_dataset %>%
    group_by(area) %>%
    mutate(percentage = value / sum(value)) %>%
    ungroup()
  
  for (i in seq_along(all_areas)) {

    proc_area <- all_areas[i]
    print(sprintf("%d/%d: Processing %s", i, total_areas, proc_area))
    
    proc_base_pop <- base_pop[base_pop$area == proc_area, , drop = FALSE]
    
    # proc_household_dataset <- household_prep(household_dataset, proc_base_pop)
    proc_household_dataset <- household_dataset[
      household_dataset$area == proc_area, , drop = FALSE]
    
    if (nrow(proc_base_pop) == 0) {
      next
    }
    proc_base_pop <- create_household_composition(
      proc_household_dataset, proc_base_pop, proc_area
    )
    
    results[[length(results) + 1]] <- proc_base_pop
  }
  
  for (result in results) {
    base_pop[base_pop$index %in% result$index, ] <- result
  }
  
  base_pop[c("area", "age")] <- lapply(base_pop[c("area", "age")], as.integer)
  end_time <- Sys.time()
  
  total_mins <- round(difftime(end_time, start_time, units = "mins"), 3)
  print(sprintf("Processing time (household): %f", total_mins))
  
  if (!is.null(geo_address_data)) {
    proc_address_data <- add_random_address(
      base_pop, 
      geo_address_data, 
      "household"
    )
    base_address <- rbind(base_address, proc_address_data)
    base_address$area <- as.integer(base_address$area)
  }
  
  return(list(base_pop = base_pop, base_address = base_address))
}
