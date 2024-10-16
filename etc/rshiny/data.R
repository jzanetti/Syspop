

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
  
  # browser()
  return(list(
    sim = list(
      df_pop = df_pop_sim, 
      df_household = df_household_sim,
      df_employee = df_employee_sim,
      df_employer = df_employer_sim
    ),
    truth = list(
      df_pop = df_pop_truth,
      df_household = df_household_truth,
      df_employee = df_work_truth$employee,
      df_employer = df_work_truth$employer
    )
    ))
}