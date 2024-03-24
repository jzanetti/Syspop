from logging import getLogger

from funcs import DEFAULT_MODEL_NAME, LOCATIONS_CFG, PROMPT_QUESTION
from llama_cpp import Llama
from numpy.random import choice as numpy_choice
from pandas import DataFrame
from pandas import concat as pandas_concat
from pandas import date_range, to_datetime

logger = getLogger()


def prompt_llm(
    agent_features: dict = {
        "age": "18-65",
        "gender": "male",
        "work_status": "employed",
        "income": "middle income",
        "time": "weekend",
        "others": "",
    },
    model_path: str = DEFAULT_MODEL_NAME,
    max_tokens: int = 1500,
    temperature: float = 0.7,
    top_p: float = 0.7,
    top_k: float = 75,
    n_gpu_layers: int = 256,
    gender: str = numpy_choice(["male", "female"]),
    print_log: bool = False,
) -> dict:
    """Prompt LLM

    Args:
        agent_features (_type_, optional): agent features to be used.
            Defaults to { "age": "18-65", "gender": "male", "work_status": "employed", "income": "middle income", "time": "weekend", "others": "", }.
        model_path (str, optional): LLAMA model path. Defaults to DEFAULT_MODEL_NAME.
        max_tokens (int, optional): token number. Defaults to 450.
        temperature (float, optional): If the temperature is set to a high value (e.g., 1.0),
            the model might generate a more diverse and unexpected output. Defaults to 0.7.
        top_p (float, optional): If top_p is set to a high value (e.g., 0.9), the model considers a wider
            range of tokens for the next word, leading to more diverse outputs. Defaults to 1.0.
        top_k (int, optional): If top_k is set to a high value (e.g., 50),
            the model considers the top 50 most likely next words. Defaults to 120.
        n_gpu_layers (int, optional): Using GPU for some tasks. Defaults to 256.
        gender (_type_, optional): choosing gender. Defaults to numpy_choice(["male", "female"]).

    Returns:
        _type_: _description_
    """

    if agent_features["time"] == "weekend":
        agent_features["work_status"] = "not working or studying today"

    if agent_features["work_status"] == "retired":
        agent_features["work_status"] = ""

    question_fmt = PROMPT_QUESTION.format(
        gender=gender,
        age=agent_features["age"],
        work_status=agent_features["work_status"],
        income=agent_features["income"],
        others=agent_features["others"],
        locations_list=agent_features["locations"],
    ).replace(" ,", "")

    my_llm = Llama(
        model_path=model_path, verbose=False, seed=-1, n_gpu_layers=n_gpu_layers
    )

    texts = my_llm(
        question_fmt,
        max_tokens=max_tokens,
        temperature=temperature,
        top_p=top_p,
        top_k=top_k,
    )["choices"]

    s = texts[0]["text"]

    if print_log:
        print(s)

    s = s[s.index("\n\n|") + len("\n\n|") - 1 : s.index("|\n\n") + 1]

    # Split the string into lines
    lines = s.strip().split("\n")

    # Get the headers
    headers = lines[0].split("|")[1:-1]
    headers = [h.strip() for h in headers]

    # Initialize the dictionary
    data = {h: [] for h in headers}

    # Fill the dictionary
    for line in lines[2:]:
        values = line.split("|")[1:-1]
        values = [v.strip() for v in values]
        for h, v in zip(headers, values):
            data[h].append(v)

    return data


def dict2df(data: dict):

    # Create DataFrame
    df = DataFrame(data)

    # Convert 'Hour' to datetime and set it as index
    df["Hour"] = to_datetime(df["Hour"], format="%H:%M").dt.time
    df.set_index("Hour", inplace=True)

    # Create a new DataFrame with all hours
    all_hours = date_range(start="00:00", end="23:59", freq="H").time
    df_all = DataFrame(index=all_hours)

    # Join the original DataFrame with the new one
    df = df_all.join(df)

    df.iloc[0]["Location"] = "Home"
    df.iloc[0]["Activity"] = "Sleep"
    # Forward fill the missing values
    df.fillna(method="ffill", inplace=True)

    return data_postp(df)


def extract_keyword(value):
    # Split the value into words and filter out the keywords
    words = value.replace(",", "").split()

    keywords_in_value = [word for word in words if word in list(LOCATIONS_CFG.keys())]

    if len(keywords_in_value) == 0:
        keywords_in_value = ["others"]

    # If there are keywords, return a random one; otherwise, return None
    return numpy_choice(keywords_in_value) if keywords_in_value else None


def data_postp(data: DataFrame):
    """Postprocessing the output

    Args:
        data (DataFrame): _description_

    Returns:
        _type_: _description_
    """
    data["Location"] = data["Location"].str.lower()
    data["Location"] = data["Location"].apply(extract_keyword)

    data = data.reset_index()
    data = data.rename(columns={"index": "Time"})

    data["Hour"] = data["Time"].apply(lambda x: x.hour)

    return data[["Time", "Hour", "Activity", "Location"]]


def combine_data(data_list: list) -> DataFrame:
    """Combine all data together

    Args:
        data_list (list): _description_

    Returns:
        DataFrame: _description_
    """
    all_data = []
    for people_id in range(len(data_list)):
        proc_data = data_list[people_id]
        proc_data = proc_data.reset_index()
        proc_data = proc_data.rename(columns={"index": "Time"})
        proc_data["People_id"] = people_id
        all_data.append(proc_data)

    all_data = pandas_concat(all_data, ignore_index=True)

    return all_data[["Time", "Hour", "Activity", "Location", "People_id"]]


def update_locations_with_weights(
    data: DataFrame, day_type: str, base_value: str = "home"
) -> DataFrame:
    """Update location occurence based on weight

    Args:
        data (DataFrame): Data to be updated
        day_type (str): weekday or weekend
        base_value (str, optional): The base value to use [Defaults to "home"].
    """
    for proc_loc in LOCATIONS_CFG:
        proc_weight = LOCATIONS_CFG[proc_loc]["weight"]

        if proc_weight is None:
            continue

        indices_to_replace = data.index[data["Location"] == proc_loc].tolist()

        if len(indices_to_replace) == 0:
            continue

        num_values_to_replace = int(
            len(indices_to_replace) * (1.0 - proc_weight[day_type])
        )
        indices_to_replace_random = numpy_choice(
            indices_to_replace, size=num_values_to_replace, replace=False
        )

        data.loc[indices_to_replace_random, "Location"] = base_value

    return data


def update_location_name(data: DataFrame) -> DataFrame:
    """Update data locations

    Args:
        data (DataFrame): Data to be updated

    Returns:
        DataFrame: Data has been updated
    """

    def _replace_with_prob(row, convert_map: dict, target_value: str):
        if row["Location"] == target_value:
            return numpy_choice(
                list(convert_map.keys()),
                p=list(convert_map.values()),
            )
        else:
            return row["Location"]

    all_data_locs = data["Location"].unique()

    for proc_data_loc in all_data_locs:

        if proc_data_loc in list(LOCATIONS_CFG.keys()):
            proc_weight = LOCATIONS_CFG[proc_data_loc]["convert_map"]

            if proc_weight is not None:
                data["Location"] = data.apply(
                    _replace_with_prob,
                    args=(
                        proc_weight,
                        proc_data_loc,
                    ),
                    axis=1,
                )

    return data
