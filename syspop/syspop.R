source("syspop/r/create_pop_wrapper.R")

create_synthetic_population <- function(
    syn_areas = NULL,
    output_dir = "",
    population = NULL,
    household = NULL,
    work = NULL,
    commute = NULL,
    education = NULL,
    shared_space = NULL,
    geography = NULL) {
  
  # Create temporary directory
  tmp_dir <- file.path(output_dir, "tmp")
  dir.create(tmp_dir, showWarnings = FALSE, recursive=TRUE)

  # --------------------
  # Creating base population
  # --------------------
  print("Creating base population ...")
  create_base_pop(
    tmp_dir,
    population$structure,
    syn_areas)
  
  # --------------------
  # Creating household: composition
  # --------------------
  if(!is.null(household)){
    if(!is.null(household$composition)){
      print("Creating household - composition ...")
      create_household_composition(
        tmp_dir, 
        household$composition, 
        geography$address)}
  }
  
  # --------------------
  # Creating work
  # --------------------
  if(!is.null(work)){
    print("Creating work ...")
    create_work(
      tmp_dir, 
      work$employer,
      work$employee,
      commute$travel_to_work, 
      geography$hierarchy, 
      geography$address
    )
  }
  
  # --------------------
  # Creating education
  # --------------------
  if(!is.null(education)){
    if(!is.null(education$school)){
      print("Creating school ...")
      create_school_and_kindergarten(
        tmp_dir, 
        education$school, 
        "school", 
        geography$hierarchy,
        possible_area_levels = c("area", "super_area", "region")
      )
    }
    if(!is.null(education$kindergarten)){
      print("Creating kindergarten ...")
      create_school_and_kindergarten(
        tmp_dir, 
        education$kindergarten, 
        "kindergarten", 
        geography$hierarchy,
        possible_area_levels = c("area")
      )
    }
  }
  
  # --------------------
  # Creating shared space
  # --------------------
  for (shared_space_name in c(
    "supermarket", 
    "restaurant", 
    "cafe", 
    "department_store", 
    "wholesale", 
    "fast_food", 
    "pub", 
    "park")){
    if(!is.null(shared_space[[shared_space_name]])){
      print(paste0("Creating ", shared_space_name))
      create_shared_space(
        tmp_dir,
        shared_space[[shared_space_name]],
        shared_space_name,
        geography$location,
        area_name_keys_and_selected_nums = shared_place_area_nums[[shared_space_name]]
      )
    }
  }
  
  # --------------------
  # Creating final outputs
  # --------------------
  base_pop <- read_parquet(file.path(tmp_dir, "syspop_base.parquet"))
  base_address <- read_parquet(file.path(tmp_dir, "syspop_location.parquet"))

  output_files <- list(
    syspop_base = c("area", "age", "gender", "ethnicity"),
    syspop_household = c("household"),
    syspop_travel = c("travel_mode_work"),
    syspop_work_and_school = c("area_work", "company", "school", "kindergarten"),
    syspop_lifechoice = c("supermarket", "restaurant", "cafe", "department_store", "wholesale", "fast_food", "pub", "park")
  )

  # Add an 'id' column to synpop_data$synpop using row numbers
  base_pop$id <- seq_len(nrow(base_pop))
  for (name in names(output_files)) {
    cols <- output_files[[name]]
    selected_data <- base_pop[, c("id", cols), drop = FALSE]
    write_parquet(selected_data, file.path(output_dir, paste0(name, ".parquet")))
  }
  write_parquet(base_address, file.path(output_dir, "syspop_location.parquet"))
}
