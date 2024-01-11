
from pandas import DataFrame
from numpy import NaN

from logging import getLogger

logger = getLogger()

def social_economic_wrapper(base_pop: DataFrame, social_economic_dataset: DataFrame):
    """Assign social economics value to different area

    Args:
        base_pop (DataFrame): Base population
        social_economic_dataset (DataFrame): Area dependant social economical data
    """

    base_pop["social_economics"] = NaN

    all_areas = list(base_pop["area"].unique())

    for i, proc_area in enumerate(all_areas):

        logger.info(f"Processing the area {i}/{len(all_areas)}")

        proc_base_pop = base_pop[base_pop["area"] == proc_area]

        try:
            proc_social_economic = social_economic_dataset[
                social_economic_dataset["area"] == proc_area]["socioeconomic_centile"].values[0]
        except IndexError:
            proc_social_economic = NaN

        proc_base_pop["social_economics"] = proc_social_economic

        base_pop.loc[proc_base_pop.index] = proc_base_pop
    
    return base_pop
        