DEFAULT_MODEL_NAME = "llama-2-7b-chat.ggmlv3.q8_0.gguf"

MAX_ALLOWED_FAILURE = 30

PEOPLE_CFG = {
    "toddler": {
        "default": {
            "age": "2-5",
            "work_status": "kindergarten",
            "income": "",
            "others": "there is a chance this person may go to kindy",
        },
        "weekend": {"others": ""},
    },
    "student": {
        "default": {
            "age": "6-18",
            "work_status": "student",
            "income": "",
            "others": "off school time is usually between 3pm and 5pm, but not necessary",
        },
        "weekend": {"others": ""},
    },
    "worker1": {
        "default": {
            "age": "18-65",
            "work_status": "employed",
            "income": "low income",
            "others": "",
        },
        "weekend": {"others": ""},
    },
    "worker2": {
        "default": {
            "age": "18-65",
            "work_status": "employed",
            "income": "middle income",
            # "others": "flexible working hours between T08-T20 for 7 hours a day",
            "others": "",
        },
        "weekend": {"others": ""},
    },
    "worker3": {
        "default": {
            "age": "18-65",
            "work_status": "employed",
            "income": "high income, rich",
            "others": "flexible working hours between T08-T20 for 7 hours a day",
            "others": "",
        },
        "weekend": {"others": ""},
    },
    "retiree": {
        "default": {
            "age": "65-99",
            "work_status": "retired",
            "income": "",
            "others": "poor health",
        }
    },
}

LOCATIONS_AND_COLORS = {
    "home": "#ededed",
    "gym": "blue",
    "office": "green",
    "supermarket": "orange",
    "mall": "purple",
    "restaurant": "cyan",
    "cafe": "magenta",
    "pub": "yellow",
    # "playground": "brown",
    "school": "lime",
    "park": "pink",
    "travel": "teal",
    "others": "#b1bfa4",
}

PROMPT_QUESTION = (
    "Guess a 24-hour likely diary for a person "
    + "({age} year old, {gender}, {work_status}, {income}, {others}). "
    + "Use a table with 'hour', 'activity', and 'location' columns. "
    + f"The value for the column Locations are chosen from {list(LOCATIONS_AND_COLORS.keys())}. "
    + "Activities should be one word. The schedule should run from 00:00 to 23:00."
)


DAY_TYPE_WEIGHT = {"weekday": {"weight": 5}, "weekend": {"weight": 2}}
