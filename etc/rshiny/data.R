

get_data <- function(
    base_dir = "/tmp/syspop/", 
    base_dir_truth = "~/Github/Syspop/etc/data/test_data/") {

  # read population - sim
  df_pop_sim <- read_parquet(paste0(base_dir, "syspop_base.parquet"))
  
  # read household - sim
  df_household_sim <- read_parquet(paste0(base_dir, "syspop_household.parquet"))
  df_household_sim$composition <- str_extract(df_household_sim$household, "(?<=_)[0-9]+-[0-9]+(?=_)")
  df_household_sim$area <- str_extract(df_household_sim$household, "(?<=household_)\\d+")
  df_household_sim <- unique(df_household_sim[,c("area", "composition", "household")])
  
  # read population - truth
  df_pop_truth <- read_parquet(paste0(base_dir_truth, "population_structure.parquet"))
  df_pop_truth <- df_pop_truth[df_pop_truth$area %in% unique(df_pop_sim$area), ]
  
  # read household - truth
  df_household_truth <- read_parquet(paste0(base_dir_truth, "household_composition.parquet"))
  df_household_truth <- df_household_truth[df_household_truth$area %in% unique(df_pop_sim$area), ]
  df_household_truth <- df_household_truth %>%
    mutate(composition = paste(adults, children, sep = "-"))
  df_household_truth = df_household_truth[,c("area", "composition", "value")]
  
  return(list(
    sim = list(
      df_pop = df_pop_sim, 
      df_household = df_household_sim
    ),
    truth = list(
      df_pop = df_pop_truth,
      df_household = df_household_truth
    )
    ))
}