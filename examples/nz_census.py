from process.data.sample import load_sample_data
from process.model.stochastic_impute import stochastic_impute
from process.postp.vis import plot_distribution

data, task_list = load_sample_data(refresh=True)

syn_pop = stochastic_impute(data, task_list)

for proc_key in [
    ["age"],
    ["work_hours"],
    ["travel_to_work"],
    ["occupation"],
    ["occupation", "income"],
    ["age", "work_hours"],
    ["gender", "work_hours"],
    ["industry", "income"],
]:
    plot_distribution(syn_pop, proc_key)
