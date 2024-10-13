source("syspop/r/global_vars.R")

#' Calculate Distances between Population and Shared Space Coordinates
#'
#' This function calculates the Euclidean distance between the coordinates
#' of the population data and the coordinates of the specified shared space data.
#'
#' @param pop_data A data frame containing the population data with columns 
#'        for source latitude (src_latitude) and source longitude 
#'        (src_longitude).
#' @param shared_space_data A data frame containing the shared space 
#'        data with latitude and longitude columns named based on the 
#'        specified `shared_space_name`.
#' @param shared_space_name A character string indicating the name of the 
#'        shared space whose coordinates will be used for distance 
#'        calculation.
#'
#' @return A matrix of distances where each entry corresponds to the 
#'         Euclidean distances between each point in `pop_data` and 
#'         each point in `shared_space_data`. The output is structured so
#'         that rows correspond to the `pop_data` entries and columns 
#'         correspond to the `shared_space_data` entries.
#'
#' @examples
#' pop_data <- data.frame(src_latitude = c(34.05, 36.16), 
#'                         src_longitude = c(-118.25, -115.15))
#' shared_space_data <- data.frame(latitude_space = c(34.00, 36.20), 
#'                                   longitude_space = c(-118.20, -115.10))
#' distances <- get_dis(pop_data, shared_space_data, "space")
get_dis <- function(
    pop_data,
    shared_space_data,
    shared_space_name) {
  
  src_coordinates <- pop_data[, c("src_latitude", "src_longitude")]
  shared_coordinates <- shared_space_data[, c(paste0("latitude_", shared_space_name), paste0("longitude_", shared_space_name))]
  colnames(shared_coordinates) <- c("src_latitude", "src_longitude")
  
  # Combine both sets of coordinates
  combined_coordinates <- rbind(src_coordinates, shared_coordinates)
  
  # Calculate the distance matrix
  distance_matrix <- as.matrix(dist(combined_coordinates, method = "euclidean"))
  
  # Extract the distances corresponding to the two sets of coordinates
  # The first n rows correspond to pop_data, the next rows correspond to shared_space_data
  distance_matrix <- distance_matrix[1:nrow(src_coordinates), (nrow(src_coordinates) + 1):ncol(distance_matrix)]
}


#' Add Shared Space Address to Address Data
#'
#' This function extracts unique shared space values from the provided 
#' population data, retrieves the corresponding latitude and longitude 
#' from the shared space data, and appends this information to the 
#' existing address data.
#'
#' @param pop_data A data frame containing population data, which includes 
#'        a column specified by `shared_space_name` containing shared 
#'        space identifiers as comma-separated values.
#' @param shared_space_data A data frame containing shared space data 
#'        with columns for shared space names, latitude, and longitude.
#' @param address_data A data frame to which the shared space address 
#'        information will be added.
#' @param shared_space_name A character string indicating the name of the 
#'        column in both `pop_data` and `shared_space_data` that contains 
#'        shared space identifiers.
#'
#' @return A data frame containing the updated address data with the 
#'         shared space addresses included.
#'
#' @examples
#' pop_data <- data.frame(shared_space_name = c("Space1,Space2", "Space3"))
#' shared_space_data <- data.frame(name = c("Space1", "Space2", "Space3"),
#'                                   latitude_space1 = c(34.05, 36.16, 40.71),
#'                                   longitude_space1 = c(-118.25, -115.15, -74.00))
#' address_data <- data.frame(existing_address = c("Address1", "Address2"))
#' updated_data <- add_shared_space_address(pop_data, shared_space_data, 
#'                                           address_data, "shared_space_name")
add_shared_space_address <- function(pop_data, shared_space_data, address_data, shared_space_name) {
  # Extract unique shared space values
  
  if(all(sapply(pop_data[[shared_space_name]], is.na))) {
    return(address_data)
  }
  else{
    unique_shared_space <- unique(unlist(strsplit(pop_data[[shared_space_name]], ",")))
    
    # Get the lat/lon for unique shared space
    unique_shared_space_data <- shared_space_data %>%
      filter(sapply(shared_space_data[[shared_space_name]], function(x) {
        any(unique_shared_space %in% strsplit(x, ",")[[1]])
      })) %>%
      distinct()
    
    # Rename columns
    unique_shared_space_data <- unique_shared_space_data %>%
      rename(
        name = !!sym(shared_space_name),
        latitude = !!sym(paste0("latitude_", shared_space_name)),
        longitude = !!sym(paste0("longitude_", shared_space_name))
      )
    
    # Add type column
    unique_shared_space_data$type <- shared_space_name
    
    # Concatenate address_data and unique_shared_space_data
    updated_address_data <- bind_rows(address_data, unique_shared_space_data)
    
    return(updated_address_data)
  }
}


#' Remove Duplicate Shared Space Values
#'
#' This function takes a comma-separated string, splits it into individual 
#' values, removes any duplicates, and then joins the unique values back 
#' into a single comma-separated string.
#'
#' @param row A character string containing comma-separated values, 
#'        which may include duplicates.
#'
#' @return A character string with duplicates removed, containing only 
#'         unique values separated by commas.
#'
#' @examples
#' result <- remove_duplicates_shared_space("x1,x2,x1,x3,x2")
#' # result will be "x1,x2,x3"
remove_duplicates_shared_space <- function(row) {
  # Split the row into individual values using the comma as a separator
  values <- unlist(strsplit(row, ","))
  
  # Remove duplicates by converting to a unique set
  unique_values <- unique(values)
  
  # Join the unique values back into a single string, separated by commas
  return(paste(unique_values, collapse = ","))
}

#' Process Shared Space Data for Population and Address Information
#'
#' This function processes shared space data to enrich population data 
#' and address data with geographic information. It computes distances 
#' from population locations to shared spaces and appends the nearest 
#' shared spaces to the population data. If specified, it can also 
#' update the address data with shared space information.
#'
#' @param shared_space_name A character string representing the name 
#'        of the shared space column.
#' @param shared_space_data A data frame containing shared space data 
#'        with latitude, longitude, and name columns.
#' @param pop_data A data frame containing population data that will 
#'        be enriched with shared space information.
#' @param address_data A data frame containing address data that may 
#'        be updated with shared space addresses.
#' @param household_address A data frame containing household addresses 
#'        with latitude and longitude for merging.
#' @param geography_location_data A data frame containing geographic 
#'        location data to merge with population data based on the area.
#' @param num_nearest An integer specifying the number of nearest shared 
#'        spaces to find for each population entry (default is 3).
#' @param assign_address_flag A logical flag indicating whether to 
#'        assign shared space addresses to the address data (default is 
#'        FALSE).
#' @param area_name_key A character string representing the key for 
#'        the area name (default is "area").
#'
#' @return A list containing the updated population data and address 
#'         data. The population data includes the nearest shared spaces 
#'         for each entry, while the address data is updated if the 
#'         `assign_address_flag` is TRUE.
#'
#' @examples
#' shared_space_data <- data.frame(name = c("Space1", "Space2"), 
#'                                   latitude = c(34.05, 36.16), 
#'                                   longitude = c(-118.25, -115.15))
#' pop_data <- data.frame(household = c("HH1", "HH2"), 
#'                         area_work = c("Area1", "Area2"))
#' address_data <- data.frame(existing_address = c("Address1", "Address2"))
#' household_address <- data.frame(household = c("HH1", "HH2"), 
#'                                   latitude = c(34.05, 36.16), 
#'                                   longitude = c(-118.25, -115.15))
#' geography_location_data <- data.frame(area = c("Area1", "Area2"),
#'                                       latitude = c(34.00, 36.00),
#'                                       longitude = c(-118.00, -115.00))
#' result <- shared_space_wrapper("shared_space", shared_space_data, 
#'                                 pop_data, address_data, 
#'                                 household_address, 
#'                                 geography_location_data, 
#'                                 num_nearest = 2, 
#'                                 assign_address_flag = TRUE)
shared_space_wrapper <- function(
    shared_space_name,
    shared_space_data,
    pop_data,
    address_data,
    household_address,
    geography_location_data,
    num_nearest = 3,
    area_name_key = "area"
) {
  
  if(num_nearest == 0){
    return(list(pop_data = pop_data, address_data = address_data))
  }
  
  # Rename columns in shared_space_data
  names(shared_space_data)[names(shared_space_data) == "latitude"] <- paste0("latitude_", shared_space_name)
  names(shared_space_data)[names(shared_space_data) == "longitude"] <- paste0("longitude_", shared_space_name)
  names(shared_space_data)[names(shared_space_data) == "name"] <- shared_space_name
  
  if (area_name_key == "area") {
    pop_data <- merge(
      pop_data,
      household_address[, c("household", "latitude", "longitude")],
      by = "household",
      all.x = TRUE
    )
    names(pop_data)[names(pop_data) == "latitude"] <- "src_latitude"
    names(pop_data)[names(pop_data) == "longitude"] <- "src_longitude"
  } else {
    geography_location_data_updated <- geography_location_data
    names(geography_location_data_updated)[names(geography_location_data_updated) == "area"] <- area_name_key
    names(geography_location_data_updated)[names(geography_location_data_updated) == "latitude"] <- paste0(area_name_key, "_latitude")
    names(geography_location_data_updated)[names(geography_location_data_updated) == "longitude"] <- paste0(area_name_key, "_longitude")
    
    pop_data <- merge(
      pop_data,
      geography_location_data_updated,
      by = "area_work",
      all.x = TRUE
    )
    
    names(pop_data)[names(pop_data) == paste0("area_work_latitude")] <- "src_latitude"
    names(pop_data)[names(pop_data) == paste0("area_work_longitude")] <- "src_longitude"
  }
  
  for (i in 0:(num_nearest - 1)) {
    if (i == 0) {
      distance_matrix <- get_dis(pop_data,
        shared_space_data,
        shared_space_name)
    } else {
      distance_matrix[cbind(seq_along(nearest_indices), nearest_indices)] <- Inf
    }
    
    nearest_indices <- apply(distance_matrix, 1, which.min)
    
    nearest_rows <- shared_space_data[nearest_indices, , drop = FALSE]
    colnames(nearest_rows)[colnames(nearest_rows) == shared_space_name] <- paste0("tmp_", i)
    nearest_rows <- nearest_rows[, !colnames(nearest_rows) %in% "area"]
    
    # Remove shared space too far away
    dis_value <- distance_matrix[cbind(seq_along(nearest_indices) , nearest_indices)]
    dis_indices <- which(
      dis_value > global_vars$shared_space_nearest_distance_km[[shared_space_name]] / 110.0)
    
    print(sprintf(
      "%s(%d, %s): Removing %.2f%% due to distance", 
      shared_space_name, 
      i, 
      area_name_key, 
      (length(dis_indices) / length(dis_value)) * 100.0))
    
    for (j in dis_indices) {
      nearest_rows[j, ] <- NA  # Use double brackets to assign NA to the list element
    }
    
    pop_data <- cbind(pop_data, nearest_rows)
    
    pop_data[pop_data[[area_name_key]] == -9999, paste0("tmp_", i)] <- ""
    
    pop_data <- pop_data[, !colnames(pop_data) %in% c(paste0("latitude_", shared_space_name), paste0("longitude_", shared_space_name))]
  }
  
  for (i in 0:(num_nearest - 1)) {
    # Convert the column tmp_i to character and concatenate to shared_space_name
    pop_data[[shared_space_name]] <- paste(
      pop_data[[shared_space_name]],
      as.character(pop_data[[paste0("tmp_", i)]]), 
      sep = ","
    )
    # Remove the temporary column
    pop_data[[paste0("tmp_", i)]] <- NULL
  }
  
  pop_data[[shared_space_name]] <- gsub("^,", "", pop_data[[shared_space_name]])
  pop_data[[shared_space_name]] <- ifelse(
    pop_data[[shared_space_name]] == "NA,NA", 
    NA, 
    gsub(",NA$", "", pop_data[[shared_space_name]])
  )
  pop_data <- pop_data[, !colnames(pop_data) %in% c("src_latitude", "src_longitude")]

  if (!is.null(address_data)) {
    address_data <- add_shared_space_address(
      pop_data, 
      shared_space_data, 
      address_data, 
      shared_space_name)
  }

  if(!all(sapply(pop_data[[shared_space_name]], is.na))){
    pop_data[[shared_space_name]] <- sapply(
      pop_data[[shared_space_name]], remove_duplicates_shared_space)
  }

  return(list(pop_data = pop_data, address_data = address_data))
}
