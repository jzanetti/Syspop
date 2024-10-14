

get_data <- function(
    base_dir = "/tmp/syspop/",  # /tmp/syspop_test17/Wellington_test_v2.0, /tmp/syspop/
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
  df_household_sim$composition <- str_extract(df_household_sim$household, "(?<=_)[0-9]+-[0-9]+(?=_)")
  df_household_sim$area <- str_extract(df_household_sim$household, "(?<=household_)\\d+")
  df_household_sim <- unique(df_household_sim[,c("area", "composition", "household")])
  
  # read household - truth
  df_household_truth <- read_parquet(paste0(base_dir_truth, "household_composition.parquet"))
  df_household_truth <- df_household_truth[df_household_truth$area %in% unique(df_pop_sim$area), ]
  df_household_truth <- df_household_truth %>%
    mutate(composition = paste(adults, children, sep = "-"))
  df_household_truth = df_household_truth[,c("area", "composition", "value")]
  
  # -------------------------
  # Work
  # -------------------------
  df_work_sim <- read_parquet(paste0(base_dir, "syspop_work_and_school.parquet"))
  
  colnames(df_work_sim)[colnames(df_work_sim) == "area_work"] <- "area"
  df_work_employee_sim <- df_work_sim[, c("area", "company")]
  df_work_employee_sim <- na.omit(df_work_employee_sim)
  df_work_employee_sim$business_code <- substr(df_work_employee_sim$company, 1, 1)
  
  df_work_truth <- list()
  df_work_truth$employee <- read_parquet(paste0(base_dir_truth, "work_employee.parquet"))
  df_work_truth$employee <- df_work_truth$employee[df_work_truth$employee$area %in% unique(df_work_sim$area), ]
  df_work_truth$employee <- df_work_truth$employee[, c("area", "business_code", "employee")]
  colnames(df_work_truth$employee)[colnames(df_work_truth$employee) == "employee"] <- "value"
  
  return(list(
    sim = list(
      df_pop = df_pop_sim, 
      df_household = df_household_sim,
      df_employee = df_work_employee_sim
    ),
    truth = list(
      df_pop = df_pop_truth,
      df_household = df_household_truth,
      df_employee = df_work_truth$employee
    )
    ))
}