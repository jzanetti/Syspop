#' Create Shared Space Data
#'
#' This function creates a DataFrame of shared space data by transforming the input DataFrame.
#'
#' It extracts the 'area', 'latitude', and 'longitude' columns from the provided DataFrame. 
#' It then generates a new DataFrame where each row corresponds to an entry in the input data, 
#' with an added unique identifier for each entry.
#'
#' @param shared_space_data A data frame containing at least the 'area', 
#'                          'latitude', and 'longitude' columns.
#'
#' @return A new data frame containing the transformed shared space data, 
#'         with 'area' as an integer, a unique 'id' as a string (first 6 
#'         characters of a UUID), and 'latitude' and 'longitude' as floats.
#'
create_shared_data <- function(shared_space_data, proc_shared_space_name) {
  
  # Select relevant columns
  shared_space_data <- shared_space_data %>%
    select(area, latitude, longitude)
  
  # Initialize an empty list to store the shared space records
  shared_space_datas <- list()
  
  # Loop through each row in the shared_space_data
  for (i in seq_len(nrow(shared_space_data))) {
    row <- shared_space_data[i, ]
    
    # Append the new record to the list
    shared_space_datas[[i]] <- data.frame(
      area = as.integer(row$area),
      id = substr(UUIDgenerate(), 1, 6),  # Create a unique 6-character ID
      latitude = as.numeric(row$latitude),
      longitude = as.numeric(row$longitude),
      stringsAsFactors = FALSE  # Avoid factors in data frame
    )
    colnames(shared_space_datas[[i]])[
      colnames(shared_space_datas[[i]]) == "id"] <- proc_shared_space_name
  }
  
  # Bind rows into a single data frame
  return(bind_rows(shared_space_datas))
}

#' Find Nearest Shared Space from Households
#'
#' This function identifies the nearest shared spaces to households based on geographic location.
#' It computes the Euclidean distances between households and shared spaces and returns a data
#' frame that includes the specified number of nearest shared spaces for each household.
#'
#' @param household_data A data frame containing household information, including an area column.
#'                       The data frame should have at least the columns: 'area'.
#' @param shared_space_address A data frame with shared space details, including their latitude
#'                             and longitude coordinates, and an 'id' column for identification.
#' @param geography_location A data frame containing geographic data, which includes 'area',
#'                           'latitude', and 'longitude' columns.
#' @param shared_space_type A string that specifies the name of the column to store the nearest
#'                          shared space information in the returned data frame.
#' @param n An integer specifying the number of nearest shared spaces to return for each household.
#'          Default is 2.
#'
#' @return A data frame that contains the original `geography_location` data along with an additional
#'         column specified by `shared_space_type`, listing the nearest shared spaces to each household.
#'         If no shared spaces are found, it returns "Unknown" for that household.
#'
#' @examples
#' # Assuming household_data, shared_space_address, and geography_location are already defined:
#' result <- find_nearest_shared_space_from_household(household_data, 
#'                                                    shared_space_address, 
#'                                                    geography_location, 
#'                                                    shared_space_type = "nearest_spaces", 
#'                                                    n = 3)
#'
find_nearest_shared_space_from_household <- function(household_data, 
                                                     shared_space_address, 
                                                     geography_location, 
                                                     shared_space_type, 
                                                     n = 2) {
  
  # Filter geography_location for areas present in household_data
  updated_src_data <- geography_location %>%
    filter(area %in% unique(household_data$area))
  
  # Extract latitude and longitude as a matrix
  coords1 <- as.matrix(updated_src_data[, c("latitude", "longitude")])
  coords2 <- as.matrix(shared_space_address[, c("latitude", "longitude")])
  
  # Combine coordinates into a single matrix
  combined_coordinates <- rbind(coords1, coords2)
  
  # Compute distances using dist() for Euclidean distance
  distance_matrix <- as.matrix(dist(combined_coordinates, method = "euclidean"))
  
  # The distance_matrix will contain distances between all points in combined_coordinates.
  # Extract the relevant distances for the households.
  n_households <- nrow(coords1)
  distances <- distance_matrix[1:n_households, (n_households + 1):ncol(distance_matrix)]
  
  # Find the nearest n indices for each household
  nearest_indices <- apply(distances, 1, order)[1:n, ]
  
  # Process the nearest names
  nearest_names <- vector("list", nrow(coords1))
  for (i in 1:nrow(coords1)) {
    proc_names <- shared_space_address[[shared_space_type]][nearest_indices[i,]]
    nearest_names[[i]] <- ifelse(length(proc_names) == 0, "Unknown", paste(proc_names, collapse = ", "))
  }
  
  # Add the nearest names to updated_src_data
  updated_src_data[[shared_space_type]] <- sapply(nearest_names, function(x) x)
  
  return(updated_src_data)
}


#' Place Agent to Shared Space Based on Area
#'
#' Assigns an agent to a shared space based on specified area criteria. This function 
#' selects a shared space from the provided data based on the agent's area and other 
#' filtering criteria. If multiple shared spaces meet the criteria, one is randomly 
#' selected, optionally weighted by a specified key.
#'
#' @param shared_space_data A data frame containing shared space information, including 
#'                          area and filter criteria.
#' @param agent A named vector representing an agent with area and filter values.
#' @param shared_space_type A string indicating the type of shared space to which the 
#'                          agent is being assigned.
#' @param filter_keys A character vector of keys used for additional filtering of 
#'                    shared spaces. Defaults to an empty character vector.
#' @param weight_key A string indicating the key used for weighting the selection of 
#'                   shared spaces. If NULL, selection is uniform. Defaults to NULL.
#' @param shared_space_type_convert A list (named vector) for converting shared space types. 
#'                                   If NULL, no conversion is applied. Defaults to NULL.
#'
#' @return A named vector representing the updated agent with the selected shared space 
#'         ID assigned to the corresponding shared_space_type.
#'
#' @examples
#' # Assuming shared_space_data is a data frame and agent is a named vector
#' updated_agent <- place_agent_to_shared_space_based_on_area(
#'     shared_space_data,
#'     agent,
#'     shared_space_type = "office",
#'     filter_keys = c("capacity"),
#'     weight_key = "popularity"
#' )
#'
#' @export
place_agent_to_shared_space_based_on_area <- function(shared_space_data, agent, shared_space_type,
                                                      filter_keys = character(0), weight_key = NULL, name_key = "id",
                                                      shared_space_type_convert = NULL) {
  
  selected_space_id <- NULL
  
  if (!is.null(agent[[paste0("area_", shared_space_type)]])) {
    
    selected_spaces <- shared_space_data[
      shared_space_data[[paste0("area_", shared_space_type)]] == 
        agent[[paste0("area_", shared_space_type)]], 
    ]
    
    if (nrow(selected_spaces) == 0) {
      selected_space_id <- "Unknown"
    } else {
      for (proc_filter_key in filter_keys) {
        if (proc_filter_key %in% names(shared_space_data)) {
          selected_spaces <- selected_spaces[
            agent[[proc_filter_key]] == selected_spaces[[proc_filter_key]], 
          ]
        } else {
          selected_spaces <- selected_spaces[
            (agent[[proc_filter_key]] >= selected_spaces[[paste0(proc_filter_key, "_min")]]) &
              (agent[[proc_filter_key]] <= selected_spaces[[paste0(proc_filter_key, "_max")]]), 
          ]
        }
      }
      if (nrow(selected_spaces) == 0) {
        selected_space_id <- "Unknown"
      }
      else {
        if (is.null(weight_key)) {
          selected_space_id <- sample(selected_spaces[[name_key]], size = 1)
        } else {
          weights <- selected_spaces[[weight_key]]
          selected_space_id <- selected_spaces[[name_key]][
            sample(nrow(selected_spaces), size = 1, prob = weights / sum(weights))
          ]
        }
      }
    }
  }
  
  if (!is.null(shared_space_type_convert)) {
    shared_space_type <- shared_space_type_convert[[shared_space_type]]
  }
  
  agent[[shared_space_type]] <- selected_space_id
  
  return(agent)
}


# Function to place agent to shared space based on distance

place_agent_to_shared_space_based_on_distance <- function(agent, shared_space_loc) {
  #' Assign an agent to a shared space based on their area attribute.
  #'
  #' This function updates an agent's location based on shared space data for a given area.
  #' It finds the corresponding shared space location that matches the agent's area and 
  #' assigns the location attributes (like latitude and longitude) to the agent.
  #'
  #' @param agent A dataframe or named vector representing the agent, which contains an 'area' attribute.
  #' @param shared_space_loc A dataframe containing shared space locations, including an 'area' column.
  #'
  #' @return The updated agent with shared space location details (e.g., latitude, longitude).
  
  # Iterate over each column in shared_space_loc
  for (proc_shared_space_name in names(shared_space_loc)) {
    
    # Extract the shared space data for the current shared space name
    proc_shared_space_loc <- shared_space_loc[[proc_shared_space_name]]
    
    # Assign the relevant location to the agent where area matches
    agent[[proc_shared_space_name]] <- shared_space_loc[[proc_shared_space_name]][
      shared_space_loc[[proc_shared_space_name]]$area == agent$area, proc_shared_space_name][[1]]
  }
  
  return(agent)
}

