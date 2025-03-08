

get_data <- function(
    base_dir = "/tmp/syspop_test17/Wellington_test_v2.0/",
    base_dir_truth = "~/Github/Syspop/etc/data/test_data/") {
  
  # -------------------------
  # Population
  # -------------------------
  # read population - sim
  df_pop_sim <- read_parquet(paste0(base_dir, "syspop_base.parquet"))
  
  # read population - truth
  df_pop_truth <- read_parquet(paste0(base_dir_truth, "population_structure.parquet"))
  df_pop_truth <- df_pop_truth[df_pop_truth$area %in% unique(df_pop_sim$area), ]

  # -------------------------
  # Household
  # -------------------------
  # read household - sim
  df_household_sim <- read_parquet(paste0(base_dir, "syspop_household.parquet"))
  df_household_info <- read_parquet(paste0(base_dir, "household_data.parquet"))
  df_household_sim <- left_join(df_household_sim, df_household_info[,c("adults", "children", "household", "area")], by = "household")
  df_household_sim <- df_household_sim %>%
    mutate(composition = paste(adults, children, sep = "-"))
  
  # read household - truth
  df_household_truth <- read_parquet(paste0(base_dir_truth, "household_composition.parquet"))
  df_household_truth <- df_household_truth[df_household_truth$area %in% unique(df_pop_sim$area), ]
  df_household_truth <- df_household_truth %>%
    mutate(composition = paste(adults, children, sep = "-"))
  df_household_truth = df_household_truth[,c("area", "composition", "value")]
  
  # -------------------------
  # Work
  # -------------------------
  # Employee
  df_employee_sim <- read_parquet(paste0(base_dir, "syspop_work.parquet"))
  df_employee_sim <- na.omit(df_employee_sim)
  colnames(df_employee_sim)[colnames(df_employee_sim) == "area_work"] <- "area"
  df_employee_sim <- df_employee_sim[, c("area", "business_code")]
  
  df_work_truth <- list()
  df_work_truth$employee <- read_parquet(paste0(base_dir_truth, "work_employee.parquet"))
  df_work_truth$employee <- df_work_truth$employee[df_work_truth$employee$area %in% unique(df_employee_sim$area), ]
  df_work_truth$employee <- df_work_truth$employee[, c("area", "business_code", "employee")]
  colnames(df_work_truth$employee)[colnames(df_work_truth$employee) == "employee"] <- "value"
  
  # Employer
  df_employer_sim <- read_parquet(paste0(base_dir, "syspop_work.parquet"))
  df_employer_sim <- na.omit(df_employer_sim)
  df_employer_sim <- df_employer_sim %>%
    group_by(area_work, business_code) %>%
    summarize(value = n_distinct(employer)) %>%
    ungroup()
  colnames(df_employer_sim)[colnames(df_employer_sim) == "area_work"] <- "area"
  df_employer_sim <- df_employer_sim[, c("area", "business_code")]

  df_work_truth$employer <- read_parquet(paste0(base_dir_truth, "work_employer.parquet"))
  df_work_truth$employer <- df_work_truth$employer[df_work_truth$employer$area %in% unique(df_employer_sim$area), ]
  df_work_truth$employer <- df_work_truth$employer[, c("area", "business_code", "employer")]
  colnames(df_work_truth$employer)[colnames(df_work_truth$employer) == "employer"] <- "value"
  
  # Income
  df_income_sim <- read_parquet(paste0(base_dir, "syspop_work.parquet"))
  df_pop_sim <- read_parquet(paste0(base_dir, "syspop_base.parquet"))
  df_income_sim <- na.omit(df_income_sim)
  df_income_sim <- df_income_sim[, c("id", "business_code", "income")]
  df_income_sim <- df_income_sim[df_income_sim$income != "Unknown", ]
  df_income_sim <- left_join(df_income_sim, df_pop_sim[,c("id", "age", "gender", "ethnicity")], by = "id")
  df_income_sim <- df_income_sim[, c("business_code", "income", "age", "gender", "ethnicity")]
  breaks <- c(15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 1000)
  labels <- c("15-19", "20-24", "25-29", "30-34", "35-39", "40-44", "45-49", "50-54", "55-59", "60-64", "65-999")
  df_income_sim$age <- cut(df_income_sim$age, breaks = breaks, labels = labels, right = FALSE)
  df_income_sim$income <- as.numeric(df_income_sim$income)
  df_income_sim <- df_income_sim %>%
    group_by(business_code, age, gender, ethnicity) %>%
    summarize(value = mean(income, na.rm = TRUE)) %>%
    ungroup()  # ungroup the data if needed
  
  df_work_truth$income <- read_parquet(paste0(base_dir_truth, "work_income.parquet"))

  # -------------------------
  # Address
  # -------------------------
  df_address_data <- list()
  
  address_types <- c("household", "employer", "school", "supermarket")
  
  # For loop to print each number
  for (address_type in address_types) {
    df_address_data[[address_type]] <- read_parquet(
      paste0(base_dir, paste0(address_type, "_data.parquet")))
    
    if (address_type == "household") {
      df_sim_to_use <- read_parquet(paste0(base_dir, "syspop_household.parquet"))
    }
    else if (address_type == "employer") {
      df_sim_to_use <- read_parquet(paste0(base_dir, "syspop_work.parquet"))
    }
    else if (address_type == "school") {
      df_sim_to_use <- read_parquet(paste0(base_dir, "syspop_school.parquet"))
    }
    else if (address_type %in% c("supermarket")) {
      df_sim_to_use <- read_parquet(paste0(base_dir, "syspop_shared_space.parquet"))
      df_sim_to_use[] <- lapply(df_sim_to_use, function(col) {
        sub(",.*", "", col)  # Replace everything after the first comma
      })
      df_sim_to_use$id <- as.integer(df_sim_to_use$id)
    }
    
    df_sim_to_use <- na.omit(df_sim_to_use)
    
    df_address_data[[address_type]] <- na.omit(left_join(
      left_join(
        df_sim_to_use %>% select(id, all_of(address_type)),
        df_pop_sim[,c("id")], 
        by = "id"),
      df_address_data[[address_type]] %>% select(latitude, longitude, all_of(address_type)),
      by = address_type))
    
  }

  return(list(
    sim = list(
      df_pop = df_pop_sim, 
      df_household = df_household_sim,
      df_employee = df_employee_sim,
      df_employer = df_employer_sim,
      df_income = df_income_sim
    ),
    truth = list(
      df_pop = df_pop_truth,
      df_household = df_household_truth,
      df_employee = df_work_truth$employee,
      df_employer = df_work_truth$employer,
      df_income = df_work_truth$income
    ),
    address = list(
      df_household = df_address_data$household,
      df_employer = df_address_data$employer,
      df_school = df_address_data$school,
      df_supermarket = df_address_data$supermarket
    )
    )
  )
}