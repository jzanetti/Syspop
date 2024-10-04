
source("syspop/r/base_pop.R")

create_base_pop <- function(
    tmp_data_path,
    pop_gender,
    pop_ethnicity,
    syn_areas,
    ref_population = "gender"
) {
  # Create base population
  base_pop_list <- base_pop_wrapper(pop_gender, pop_ethnicity, syn_areas, ref_population = ref_population)
  write_parquet(base_pop_list, tmp_data_path)
}