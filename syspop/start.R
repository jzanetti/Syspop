source("syspop/r/create_pop_wrapper.R")
source("syspop/r/household.R")
source("syspop/r/commute.R")
source("syspop/r/work.R")
source("syspop/r/school.R")

create <- function(
    syn_areas,
    output_dir,
    population = NULL,
    geography = NULL,
    household = NULL,
    work = NULL,
    commute = NULL,
    education = NULL,
    shared_space = NULL
) {
  # Create output directory if it doesn't exist
  if (!dir_exists(output_dir)) {
    dir_create(output_dir)
  }
  
  print("----------------------------")
  print("Creating base population ...")
  print("----------------------------")
  population_data <- base_pop_wrapper(population$structure, syn_areas)
  all_areas <- unique(population_data$area)

  print("----------------------------")
  print("Creating required data ...")
  print("----------------------------")
  print("Creating household data ...")
  household_data <- create_households(household$composition, geography$address, all_areas)

  print("Creating work-related data ...")
  commute_data_work <- create_commute_probability(
    commute$travel_to_work, all_areas, commute_type = "work"
  )

  employee_data <- create_employee(
    work$employee, unique(commute_data_work$area_work)
  )
  employer_data <- create_employer(
    work$employer, geography$address, unique(commute_data_work$area_work)
  )
  
  print("Creating school-related data ...")
  commute_data_school <- create_commute_probability(
    commute$travel_to_school, all_areas, commute_type = "school"
  )
  
  school_data <- create_school(
    rbindlist(
      list(education$school, 
           education$kindergarten), 
      use.names=TRUE)
  )
  
  print("Creating shared space-related data ...")
  shared_space_data <- list()
  shared_space_loc <- list()
  for (proc_shared_space_name in names(shared_space)) {
    shared_space_data[[proc_shared_space_name]] <- create_shared_data(shared_space[[proc_shared_space_name]])
    shared_space_loc[[proc_shared_space_name]] <- find_nearest_shared_space_from_household(
      household_data, shared_space_data[[proc_shared_space_name]], geography$location, proc_shared_space_name
    )
  }

  print("----------------------------")
  print("Creating agents ...")
  print("----------------------------")
  updated_agents <- list()
  updated_household_data <- copy(household_data)
  total_people <- nrow(population_data)
  
  for (i in seq_len(nrow(population_data))) {
    if (i %% 500 == 0) {
      print(sprintf("Completed: %d / %d: %d%%", i, total_people, as.integer(i * 100 / total_people)))
    }
    
    # Household
    proc_agent <- population_data[i, ]
    output <- place_agent_to_household(
      updated_household_data, proc_agent)
    proc_agent <- output$agent
    updated_household_data <- output$households

    # Work
    proc_agent <- assign_agent_to_commute(
      commute_data_work, proc_agent, "work", include_filters = list(age = list(c(18, 999))))
    proc_agent <- place_agent_to_employee(employee_data, proc_agent)
    proc_agent <- place_agent_to_shared_space_based_on_area(
      employer_data, 
      proc_agent, 
      "work", 
      filter_keys = "business_code", 
      shared_space_type_convert = list(work = "employer")
    )
    
    # School
    proc_agent <- assign_agent_to_commute(
      commute_data_school, proc_agent, "school", include_filters = list(age = list(c(0, 17))))
    proc_agent <- place_agent_to_shared_space_based_on_area(
      school_data, 
      proc_agent, 
      "school", 
      filter_keys = "age", 
      weight_key = "max_students"
    )
    # Shared space
    proc_agent <- place_agent_to_shared_space_based_on_distance(proc_agent, shared_space_loc)
    
    updated_agents[[i]] <- proc_agent
  }
  
  updated_agents <- rbindlist(updated_agents, fill=TRUE)
  
  # Add ID for each agents
  updated_agents[, id := .I]

  output_files <- list(
    syspop_base = c("area", "age", "gender", "ethnicity"),
    syspop_household = c("household"),
    syspop_travel = c("travel_method_work", "travel_method_school"),
    syspop_work = c("area_work", "business_code", "employer"),
    syspop_school = c("area_school", "school"),
    syspop_shared_space = c(
      "hospital", "supermarket", "restaurant", "cafe", "department_store", 
      "wholesale", "fast_food", "pub", "park"
    )
  )
  
  for (name in names(output_files)) {
    output_path <- file.path(output_dir, paste0(name, ".parquet"))
    tryCatch({
      write_parquet(
        updated_agents %>% select(all_of(c("id", output_files[[name]]))), 
        output_path
      )
    }, 
    error = function(e) { 
      cat("Skipping file:", output_path, "\n") }
    )
  }
  
  write_parquet(household_data, file.path(output_dir, "household_data.parquet"))
  write_parquet(employer_data, file.path(output_dir, "employer_data.parquet"))
  write_parquet(school_data, file.path(output_dir, "school_data.parquet"))
  
  for (shared_space_name in names(shared_space_data)) {
    write_parquet(shared_space_data[[shared_space_name]], file.path(output_dir, paste0(shared_space_name, ".parquet")))
  }
}
