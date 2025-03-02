#' Create Households from Aggregated Data
#'
#' This function generates a data frame of individual households from aggregated
#' household data. Each household is represented by a record containing the area,
#' number of adults, number of children, and a unique household identifier.
#'
#' @param household_data A data frame containing aggregated household data with 
#'                       columns ['area', 'adults', 'children', 'value'], where 
#'                       'value' indicates the number of households for the 
#'                       given combination of adults and children.
#' @param address_data A data frame containing address data, which includes 
#'                     latitude and longitude information.
#' @param areas A vector of areas to be included in the output.
#'
#' @return A data frame with individual household records, containing the columns:
#'         ['area', 'adults', 'children', 'latitude', 'longitude', 'household'],
#'         where 'household' is a unique ID generated for each household.
#'
#' @examples
#' # Example of using the create_households function
#' household_data <- data.frame(area = c(241300, 241300),
#'                               adults = c(2, 3),
#'                               children = c(1, 0),
#'                               value = c(5, 4))
#' address_data <- data.frame(area = c(241300, 241300),
#'                             latitude = c(-41.292, -41.295),
#'                             longitude = c(174.777, 174.778))
#' households <- create_households(household_data, address_data, areas = c(241300))
#' print(households)
#'
create_households <- function(household_data, address_data, areas) {
  households <- list()  # Initialize a list to store household records

  # Filter household and address data based on the specified areas
  household_data <- household_data[household_data$area %in% areas, ]
  address_data <- address_data[address_data$area %in% areas, ]
  
  # Loop through each row in the household data
  for (i in seq_len(nrow(household_data))) {
    area <- household_data$area[i]
    adults <- household_data$adults[i]
    children <- household_data$children[i]
    count <- household_data$value[i]
    
    ethnicity <- NULL
    if ("ethnicity" %in% names(household_data)) {
      ethnicity <- household_data$ethnicity[i]
    }
    # Filter address data for the current area
    proc_address_data_area <- address_data[address_data$area == area, ]

    # Create individual records for each household
    for (j in seq_len(count)) {
      proc_address_data <- proc_address_data_area[
        sample(nrow(proc_address_data_area), 1), ]
      
      proc_households_data <- data.frame(
        area = as.integer(area),
        adults = as.integer(adults),
        children = as.integer(children),
        latitude = as.numeric(proc_address_data$latitude),
        longitude = as.numeric(proc_address_data$longitude),
        household = substr(uuid::UUIDgenerate(), 1, 6)  # Create a 6-digit unique ID
      )
      
      if (!is.null(ethnicity)) {
        proc_households_data[["ethnicity"]] <- ethnicity
      }
      
      households[[length(households) + 1]] <- proc_households_data

    }
  }
  
  # Combine the list of data frames into a single data frame
  return(do.call(rbind, households))
}

#' Place Agent into a Household
#'
#' This function assigns an agent to a household based on the agent's age and the availability of
#' suitable households in the specified area. It first determines the agent type (adult or child)
#' and then attempts to place the agent in a household that meets the criteria. If no suitable
#' households are found, the agent is assigned to a random household.
#'
#' @param households A data frame representing the household information, which includes
#'                   the columns for household identifiers, areas, and counts of adults and children.
#' @param agent A list or data frame representing the agent information, which must contain
#'              an 'age' field to determine the type of agent and an 'area' field to match
#'              with household locations.
#'
#' @return A list containing:
#'         - `agent`: The modified agent object with the assigned household.
#'         - `households`: The updated households data frame reflecting the change in household counts.
#'
#' @examples
#' # Assuming households and agent are already defined:
#' result <- place_agent_to_household(households, agent)
#'
place_agent_to_household <- function(households, agent) {
  # Determine agent type based on age
  agent_type <- ifelse(agent$age >= 18, "adults", "children")
  
  # Filter selected households based on agent type and area
  selected_households <- households %>%
    filter(!!sym(agent_type) >= 1 & area == agent$area)
  
  if (nrow(selected_households) > 0) {
    # Randomly select a household from the available options
    selected_household <- selected_households %>% sample_n(1)
    households[households$household == selected_household$household, agent_type] <- 
      households[households$household == selected_household$household, agent_type] - 1
  } else {
    # If no suitable households are available, select a random household
    selected_household <- households %>% sample_n(1)
  }
  
  # Assign the household to the agent
  agent$household <- selected_household$household
  return(list(agent = agent, households = households))
}
