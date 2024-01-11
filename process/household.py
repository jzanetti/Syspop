from pandas import DataFrame, isna, concat
from numpy import NaN
from numpy.random import randint
from random import choices as random_choices
from random import choice as random_choice
from copy import deepcopy
from process.vis import validate_households
from numpy import isnan
import ray
from logging import getLogger
from datetime import datetime
from process.address import add_random_address


logger = getLogger()

def add_people(
        pop_input: DataFrame, 
        total_households: int, 
        proc_num_children: int, 
        proc_area: int, 
        all_ethnicities: list, 
        parents_age_limits = {"min": 18, "max": 65},
        single_parent: bool = False) -> DataFrame:
    """Add people to household

    Args:
        pop_input (DataFrame): Base population
        total_households (int): Total households to be assigned
        proc_num_children (int): Number of children to be assigned
        proc_area (int): Area name
        all_ethnicities (list): All ethnicities
        parents_age_limits (dict, optional): Parents age range. 
            Defaults to {"min": 18, "max": 65}.

    Returns:
        Dataframe: Updated population information
    """

    def _select_female_ethnicity(target_ethnicity: str, input_ethnicity: list) -> str:
        """Select female ethnicity based on male

        Args:
            target_ethnicity (str): The male ethnicity
            input_ethnicity (list): potential ethnicities to be chosen from 

        Returns:
            str: randomly selected ethnicity
        """

        input_ethnicity.remove(target_ethnicity)

        individual_percetnage = 0.3 / len(input_ethnicity)

        all_ethnicities = [target_ethnicity] + input_ethnicity 
        weights = [0.7] + [individual_percetnage] * len(input_ethnicity)

        return random_choices(all_ethnicities, weights, k=1)[0]


    for proc_household in range(total_households):

        proc_household_id = f"{proc_area}_{proc_num_children}_{proc_household}"

        if single_parent:
            
            proc_household_id += "_sp"

            selected_single_parent = pop_input[
                (pop_input['age'] >= 18) & 
                (pop_input['age'] <= 65) & 
                isna(pop_input['household'])]
            
            if len(selected_single_parent) == 0:
                continue

            selected_single_parent = selected_single_parent.sample(n=1)
            selected_single_parent["household"] = proc_household_id
            pop_input.loc[selected_single_parent.index] = selected_single_parent
            selected_children_ethnicity = selected_single_parent["ethnicity"].values[0]
        else:
            selected_parents_male = pop_input[
                (pop_input['gender'] == 'male') & 
                (pop_input['age'] >= 18) & 
                (pop_input['age'] <= 65) & 
                isna(pop_input['household'])]
            
            if len(selected_parents_male) == 0:
                continue

            selected_parents_male = selected_parents_male.sample(n=1)
            selected_parents_male["household"] = proc_household_id
            pop_input.loc[selected_parents_male.index] = selected_parents_male

            selected_parents_female_ethnicity = _select_female_ethnicity(
                selected_parents_male["ethnicity"].values[0], deepcopy(all_ethnicities))
            selected_parents_female = pop_input[
                (pop_input['gender'] == 'female') & 
                (pop_input["ethnicity"] == selected_parents_female_ethnicity) &
                (pop_input['age'] >= max(0.7 * selected_parents_male["age"].values[0], parents_age_limits["min"])) & 
                (pop_input['age'] <= min(1.3 * selected_parents_male["age"].values[0], parents_age_limits["max"])) &
                isna(pop_input['household'])]

            if len(selected_parents_female) == 0:
                continue

            selected_parents_female = selected_parents_female.sample(n=1)
            selected_parents_female["household"] = proc_household_id
            pop_input.loc[selected_parents_female.index] = selected_parents_female

            selected_children_ethnicity = selected_parents_female_ethnicity

        selected_children = pop_input[(pop_input['age'] >= 0) & 
                                        (pop_input['age'] < 18) &
                                        (pop_input["ethnicity"] == selected_children_ethnicity) & 
                                        isna(pop_input['household'])]
        
        if len(selected_children) < proc_num_children:
            continue

        selected_children = selected_children.sample(n=proc_num_children)
        selected_children["household"] = proc_household_id
        pop_input.loc[selected_children.index] = selected_children

    return pop_input

def compared_synpop_household_with_census(
        houshold_dataset: DataFrame, 
        pop_input: DataFrame, 
        proc_area: int) -> dict:
    """Compared simulated household number with census

    Args:
        houshold_dataset (DataFrame): census household data
        pop_input (DataFrame): simulated population data with household information
        proc_area (int): area name

    Returns:
        dict: difference between simulation and census
    """

    def _get_household_children_num(household_data_result: DataFrame) -> dict:
        """Get the number of household against the number of children

        Args:
            household_data_result (DataFrame): Census household data

        Returns:
            dict: Census houshold information
        """
        household_data_result['household'] = household_data_result['household'].fillna('default_9999_9999')
        household_data_result["children_num"] = household_data_result['household'].str.split('_').str[1].astype(int)
        household_data_result['household'] = household_data_result['household'].replace('default_9999_9999', NaN)
        household_data_result['children_num'] = household_data_result['children_num'].replace(9999, NaN)

        return household_data_result


    pop_input = _get_household_children_num(pop_input)

    orig_children_num = list(houshold_dataset.columns)
    orig_children_num.remove("area")
    all_possible_children_num = list(set(list(pop_input["children_num"].unique()) +  orig_children_num))

    truth_all_households = {}
    syspop_all_households = {}
    for pro_children_num in all_possible_children_num:

        if isnan(pro_children_num):
            continue
        
        try:
            truth_all_households[pro_children_num] = int(houshold_dataset[
                houshold_dataset["area"] == proc_area][pro_children_num].values[0])
        except KeyError:
            truth_all_households[pro_children_num] = 0
        syspop_all_households[pro_children_num] = len(
            pop_input[pop_input["children_num"] == pro_children_num]["household"].unique())

    return {
        "truth": truth_all_households,
        "synpop": syspop_all_households
    }

def randomly_assign_people_to_household(
        proc_base_pop: DataFrame, 
        proc_area: str, 
        household_size = {"min": 1, "max": 10}) -> DataFrame:
    """Randomly assign people to household with size defined in household_size

    Args:
        proc_base_pop (DataFrame): base population
        proc_area (str): the area name
        household_size (dict, optional): household size to be used. 
            Defaults to {"min": 1, "max": 10}.

    Returns:
        Dataframe: Updated population
    """
    unassigned_people = proc_base_pop[isna(proc_base_pop["household"])]
    index_unassigned = 0
    while len(unassigned_people) > 0: # up to 5 people in a household
        # Randomly select x rows
        sample_size = randint(household_size["min"], household_size["max"])

        try:
            selected_rows = unassigned_people.sample(n=sample_size)
        except ValueError:
            selected_rows = unassigned_people.sample(n=len(unassigned_people))

        # Check if there is a row with age < 18
        if (selected_rows["age"] < 18).any():
            # If there is, check if there is also a row with age > 18
            if (selected_rows["age"] >= 18).any():
                # If there is, assign the label to these rows
                mask = selected_rows['age'] < 18
                children_num = len(selected_rows[mask])
                proc_base_pop.loc[selected_rows.index, "household"] = f"{proc_area}_{children_num}_random{index_unassigned}"
                no_adult_family_tries = 0
            else:
                # If there isn't a row with age > 18, put the rows back and try again
                try:
                    no_adult_family_tries += 1
                except NameError:
                    no_adult_family_tries = 0

                if no_adult_family_tries == 5: # if we are not able to find adults any more ...
                    children_num = len(selected_rows)
                    proc_base_pop.loc[selected_rows.index, "household"] = f"{proc_area}_{children_num}_noadult"
                    no_adult_family_tries = 0
                else:
                    continue
        else:
            # If there isn't a row with age < 18, assign the label to these rows
            proc_base_pop.loc[selected_rows.index, 'household'] = f"{proc_area}_0_random{index_unassigned}"
            no_adult_family_tries = 0

        unassigned_people = proc_base_pop[isna(proc_base_pop["household"])]
        index_unassigned += 1
    
    proc_base_pop = send_remained_children_to_household(proc_base_pop)

    return proc_base_pop

def send_remained_children_to_household(proc_base_pop: DataFrame) -> DataFrame:
    """Send remained children (those children in a household without adults) 
    to different households

    Args:
        proc_base_pop (DataFrame): Base population

    Returns:
        DataFrame: Updated population
    """

    def _add_one_child_to_name(input_name: str) -> str:
        """Add one child to the household name

        Args:
            input_name (str): Input name such as 110500_3_random132

        Returns:
            str: such as 110500_4_random132
        """
        input_name = input_name.split("_")
        input_name[1] = str(int(input_name[1]) + 1)
        return "_".join(input_name)


    children_only_data = proc_base_pop[proc_base_pop['household'].str.endswith('_noadult')]
    ramdom_household_data = proc_base_pop[proc_base_pop['household'].str.contains('_random.*$', regex=True)]

    for i in range(len(children_only_data)):
        proc_child = children_only_data.iloc[[i]]
        selected_household = ramdom_household_data.sample(n=1)
        proc_base_pop.loc[proc_child.index, "household"] = _add_one_child_to_name(
            selected_household["household"].values[0])
    
    return proc_base_pop




@ray.remote
def create_household_composition_remote(
        houshold_dataset: DataFrame,
        proc_base_pop: DataFrame,
        num_children: list, 
        all_ethnicities: list, 
        proc_area: str) -> DataFrame:
    return create_household_composition(
        houshold_dataset,
        proc_base_pop,
        num_children, 
        all_ethnicities, 
        proc_area)

def create_household_composition(
        houshold_dataset: DataFrame,
        proc_base_pop: DataFrame,
        num_children: list, 
        all_ethnicities: list, 
        proc_area: str) -> DataFrame:
    """Create household composistion using 3 steps:
        - step 1: two parents family (following census)
        - step 2: single parent family (for remaining people)
        - step 3: randomly assign rest people to families (for remaining people)

    Args:
        houshold_dataset (DataFrame): _description_
        num_children (list): _description_
        all_ethnicities (list): _description_
        proc_area (str): _description_

    Returns:
        DataFrame: _description_
    """
    # Step 1: First round assignment (two parents)
    logger.info("Start step 1 ....")
    for proc_num_children in num_children:

        total_households = int(houshold_dataset[
            houshold_dataset["area"] == proc_area
        ][proc_num_children].values[0])

        proc_base_pop = add_people(
            deepcopy(proc_base_pop), 
            total_households, 
            proc_num_children, 
            proc_area, 
            all_ethnicities)

    synpop_validation_data_after_step1 = compared_synpop_household_with_census(
        houshold_dataset, proc_base_pop, proc_area)

    logger.info("Start step 2 ....")
    # Step 2: Second round assignment (single parent)
    for proc_num_children in num_children:

        total_households = synpop_validation_data_after_step1["truth"][proc_num_children] - \
            synpop_validation_data_after_step1["synpop"][proc_num_children]

        if total_households <= 0:
            continue

        proc_base_pop = add_people(
            proc_base_pop, 
            total_households, 
            proc_num_children, 
            proc_area, 
            all_ethnicities,
            single_parent=True)

    logger.info("Start step 3 ....")
    # Step 3: randomly assigned the rest people
    proc_base_pop = randomly_assign_people_to_household(proc_base_pop, proc_area)

    proc_base_pop.drop("children_num", axis=1, inplace=True)

    return proc_base_pop

def household_wrapper(
        houshold_dataset: DataFrame, 
        base_pop: DataFrame,
        base_address: DataFrame,
        geo_address_data: DataFrame or None = None,
        use_parallel: bool = False, 
        n_cpu: int = 8) -> DataFrame:
    """Assign people to different households

    Args:
        houshold_dataset (DataFrame): _description_
        base_pop (DataFrame): _description_
    """
    start_time = datetime.utcnow()

    base_pop["household"] = NaN

    all_ethnicities = list(base_pop["ethnicity"].unique())

    num_children = list(houshold_dataset.columns)
    num_children.remove("area")

    if use_parallel:
        ray.init(num_cpus=n_cpu, include_dashboard=False)

    all_areas = list(base_pop["area"].unique())
    total_areas = len(all_areas)
    results = []
    for i, proc_area in enumerate(all_areas):

        logger.info(f"{i}/{total_areas}: Processing {proc_area}")

        proc_base_pop = base_pop[base_pop["area"] == proc_area]

        if len(proc_base_pop) == 0:
            continue
        
        if use_parallel:
            proc_base_pop = create_household_composition_remote.remote(
                houshold_dataset,
                proc_base_pop,
                num_children, 
                all_ethnicities, 
                proc_area)
        else:
            proc_base_pop = create_household_composition(
                houshold_dataset,
                proc_base_pop,
                num_children, 
                all_ethnicities, 
                proc_area)

        results.append(proc_base_pop)

    if use_parallel:
        results = ray.get(results)
        ray.shutdown()

    for result in results:
        base_pop.loc[result.index] = result

    end_time = datetime.utcnow()

    total_mins = round((end_time - start_time).total_seconds() / 60.0 , 3)
    logger.info(f"Processing time (household): {total_mins}")

    if geo_address_data is not None:
        proc_address_data = add_random_address(
            deepcopy(base_pop),
            geo_address_data,
            "household",
            use_parallel=use_parallel)
        base_address = concat([base_address, proc_address_data])

    return base_pop, base_address
        
