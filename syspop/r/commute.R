#' Calculate Commute Probabilities
#'
#' This function calculates commute probabilities for specified areas based on
#' the provided commute dataset. It computes the probabilities for various travel
#' methods from home areas to work or school areas.
#'
#' @param commute_dataset A data frame containing commute data with columns 
#'                       'area_home' and 'area_work' (or 'area_school').
#' @param areas A vector of areas to filter the commute data by.
#' @param commute_type A string indicating the type of commute, e.g., 
#'                     'work' or 'school'. Defaults to "work".
#'
#' @return A data frame containing the commute probabilities for each travel 
#'         method, area home, and area work/school.
#'
#' @notes The function filters the commute data by the specified areas, 
#'        calculates the total number of people commuting from each home area, 
#'        and computes probabilities by dividing the number of people using 
#'        each travel method by the total number of commuters from each home area.
#'
#' @import dplyr
#'
create_commute_probability <- function(commute_dataset, areas, commute_type = "work") {
  
  # Filter the commute dataset by specified areas
  commute_dataset <- commute_dataset %>%
    filter(area_home %in% areas)
  
  # Identify travel method columns excluding 'area_home' and 'area_work/school'
  travel_methods <- setdiff(names(commute_dataset), c("area_home", paste0("area_", commute_type)))
  
  # Calculate total number of people commuting from each area home
  total_people <- commute_dataset %>%
    group_by(area_home) %>%
    summarise(across(all_of(travel_methods), sum, .names = "total_{col}")) %>%
    rowwise() %>%
    mutate(total = sum(c_across(starts_with("total_")))) %>%
    select(area_home, total)
  
  # Calculate the sums for each area_home and area_work/school
  area_sums <- commute_dataset %>%
    group_by(area_home, !!sym(paste0("area_", commute_type))) %>%
    summarise(across(all_of(travel_methods), sum, .names = "sum_{col}"), .groups = "drop")
  
  # Calculate probabilities by dividing the sum by total number of commuters
  area_sums <- area_sums %>%
    left_join(total_people, by = "area_home") %>%
    mutate(across(starts_with("sum_"), ~ .x / total, .names = "prob_{col}")) %>%
    select(-starts_with("sum_"), -total)
  
  return(area_sums)
}

#' Assign Agent to Commute
#'
#' This function assigns a commuting area and travel method to an agent based on the provided 
#' commute dataset. It updates the agent's attributes while considering any inclusion filters 
#' that may apply.
#'
#' @param commute_dataset A data frame containing commuting data, including areas and corresponding 
#'                        travel methods.
#' @param agent A named vector representing the agent, which includes information such as 
#'              area and any relevant attributes for filtering.
#' @param commute_type A string indicating the type of commute (e.g., "work" or "school"). 
#'                     This determines the attributes that will be assigned to the agent. 
#'                     Default is "work".
#' @param include_filters A list of filters to include certain agents from being assigned a 
#'                        commute area and method. Each key corresponds to an attribute of the 
#'                        agent, and the value is a list of two-element vectors defining the 
#'                        ranges to include.
#'
#' @return A named vector representing the updated agent with the assigned commuting area and 
#'         travel method. The following attributes will be modified:
#'         - area_{commute_type}: integer, the area assigned for the commute
#'         - travel_method_{commute_type}: character, the travel method assigned for the commute
#'
#' @examples
#' # Assuming commute_dataset is a data frame and agent is a named vector
#' updated_agent <- assign_agent_to_commute(commute_dataset, 
#'                                           agent, 
#'                                           commute_type = "work", 
#'                                           include_filters = list(age = list(c(18, 65))))
#'
#' @export
assign_agent_to_commute <- function(commute_dataset, agent, commute_type = "work", include_filters = list()) {
  
  # Check inclusion filters
  for (include_key in names(include_filters)) {
    proc_filters <- include_filters[[include_key]]
    for (proc_filter in proc_filters) {
      if (agent[[include_key]] < proc_filter[1] || agent[[include_key]] > proc_filter[2]) {
        agent[[paste0("area_", commute_type)]] <- NULL
        agent[[paste0("travel_method_", commute_type)]] <- NULL
        return(agent)
      }
    }
  }
  
  # Filter the commute dataset based on the agent's area
  proc_commute_dataset <- subset(commute_dataset, area_home == agent$area)
  proc_commute_dataset$total <- rowSums(proc_commute_dataset[, -which(names(proc_commute_dataset) %in% c("area_home", paste0("area_", commute_type)))])
  
  # Select a row based on the 'total' column
  selected_row <- proc_commute_dataset[sample(nrow(proc_commute_dataset), 1, prob = proc_commute_dataset$total), ]
  selected_area <- as.integer(selected_row[[paste0("area_", commute_type)]])

  # Remove unnecessary columns for travel method selection
  selected_row <- selected_row[, !names(selected_row) %in% c("area_home", paste0("area_", commute_type), "total")]
  travel_method <- sample(names(selected_row), 1, prob = selected_row / sum(selected_row))
  
  # Update agent's attributes
  agent[[paste0("area_", commute_type)]] <- selected_area
  agent[[paste0("travel_method_", commute_type)]] <- travel_method
  
  return(agent)
}
