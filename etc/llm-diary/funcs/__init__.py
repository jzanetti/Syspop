DEFAULT_MODEL_NAME = "llama-2-7b-chat.ggmlv3.q8_0.gguf"

MAX_ALLOWED_FAILURE = 30

PEOPLE_CFG = {
    "toddler": {
        "default": {
            "age": "1-5",
            "work_status": "kindergarten or childcare",
            "income": "",
            "others": "there is a chance this person may go to kindy, kindergarten time starts from 9am to 3pm",
            "locations": [
                "home",
                "supermarket",
                "mall",
                "restaurant",
                "kindergarten",
                "childcare",
                "park",
                "others",
            ],
        },
        "weekend": {
            "others": "",
            "locations": [
                "home",
                "supermarket",
                "mall",
                "restaurant",
                "park",
                "others",
            ],
        },
    },
    "student": {
        "default": {
            "age": "6-18",
            "work_status": "student",
            "income": "",
            "others": "school time starts from 9am to 3pm",
            "locations": [
                "home",
                "supermarket",
                "mall",
                "restaurant",
                "school",
                "park",
                "others",
            ],
        },
        "weekend": {
            "others": "",
            "locations": [
                "home",
                "supermarket",
                "mall",
                "restaurant",
                "park",
                "others",
            ],
        },
    },
    "worker1": {
        "default": {
            "age": "18-65",
            "work_status": "employed",
            "income": "low income",
            "others": "",
            "locations": [
                "home",
                "supermarket",
                "mall",
                "pub",
                "office",
                "restaurant",
                "cafe",
                "park",
                "others",
            ],
        },
        "weekend": {
            "others": "",
            "locations": [
                "home",
                "supermarket",
                "mall",
                "pub",
                "restaurant",
                "cafe",
                "park",
                "others",
            ],
        },
    },
    "worker2": {
        "default": {
            "age": "18-65",
            "work_status": "employed",
            "income": "middle income",
            "others": "",
            "locations": [
                "home",
                "gym",
                "supermarket",
                "mall",
                "pub",
                "office",
                "restaurant",
                "cafe",
                "park",
                "others",
            ],
        },
        "weekend": {
            "others": "",
            "locations": [
                "home",
                "gym",
                "supermarket",
                "mall",
                "pub",
                "restaurant",
                "cafe",
                "park",
                "others",
            ],
        },
    },
    "worker3": {
        "default": {
            "age": "18-65",
            "work_status": "employed",
            "income": "high income, rich",
            "others": "flexible working hours between T08-T20 for 7 hours a day",
            "others": "",
            "locations": [
                "home",
                "gym",
                "supermarket",
                "mall",
                "pub",
                "office",
                "restaurant",
                "cafe",
                "park",
                "others",
            ],
        },
        "weekend": {
            "others": "",
            "locations": [
                "home",
                "gym",
                "supermarket",
                "mall",
                "pub",
                "restaurant",
                "cafe",
                "park",
                "others",
            ],
        },
    },
    "retiree": {
        "default": {
            "age": "65-99",
            "work_status": "retired",
            "income": "",
            "others": "poor health",
            "locations": [
                "home",
                "supermarket",
                "mall",
                "restaurant",
                "cafe",
                "park",
                "others",
            ],
        }
    },
    "not_in_employment": {
        "default": {
            "age": "18-64",
            "work_status": "not in employment",
            "income": "",
            "others": "",
            "locations": [
                "home",
                "supermarket",
                "mall",
                "restaurant",
                "cafe",
                "park",
                "others",
            ],
        }
    },
}

# ---------------------------------
# Weight is calculated by: how many days it takes for a person to carry out an activity
# For example, on average, a person may go to gym 1 time every 4 weeks during weekdays,
#              the weight is calculated by: 1.0 / (5.0*4.0)
# ---------------------------------
LOCATIONS_CFG = {
    "home": {"color": "#ededed", "weight": None, "convert_map": {"household": 1.0}},
    "household": {
        "color": "#ededed",
        "weight": None,
        "convert_map": None,
    },  # since home is renamed to household, so we need define it's color here ...
    "gym": {
        "color": "blue",
        "weight": {"weekday": 1.0 / 7.0, "weekend": 1.0 / 7.0},
        "convert_map": {"outdoor": 0.75, "gym": 0.25},
    },
    "office": {"color": "green", "weight": None, "convert_map": {"company": 1.0}},
    "company": {
        "color": "green",
        "weight": None,
        "convert_map": None,
    },  # since office is renamed to company, so we need define it's color here ...
    "supermarket": {
        "color": "orange",
        "weight": {"weekday": 1.0 / 7.0, "weekend": 1.0 / 7.0},
        "convert_map": None,
    },
    "mall": {
        "color": "purple",
        "weight": {"weekday": 1.0 / 7.0, "weekend": 1.0 / 7.0},
        "convert_map": {"department_store": 0.6, "wholesale": 0.4},
    },
    "department_store": {"color": "#c28686", "weight": None, "convert_map": None},
    "wholesale": {"color": "#c6cca5", "weight": None, "convert_map": None},
    "restaurant": {
        "color": "cyan",
        "weight": {"weekday": 1.0 / 7.0, "weekend": 1.0 / 7.0},
        "convert_map": {"restaurant": 0.4, "fast_food": 0.6},
    },
    "cafe": {
        "color": "magenta",
        "weight": {"weekday": 1.0 / 7.0, "weekend": 1.0 / 7.0},
        "convert_map": None,
    },
    "fast_food": {"color": "brown", "weight": None, "convert_map": None},
    "pub": {
        "color": "yellow",
        "weight": {
            "weekday": 1.0 / 7.0,
            "weekend": 1.0 / 7.0,
        },
        "convert_map": None,
    },
    "kindergarten": {"color": "lime", "weight": None, "convert_map": None},
    "childcare": {
        "color": "lime",
        "weight": None,
        "convert_map": {"kindergarten": 1.0},
    },
    "school": {"color": "lime", "weight": None, "convert_map": None},
    "park": {
        "color": "pink",
        "weight": None,
        "convert_map": {"park": 0.35, "outdoor": 0.65},
    },
    "outdoor": {"color": "#657f80", "weight": None, "convert_map": None},
    "others": {"color": "#b1bfa4", "weight": None, "convert_map": None},
}

PROMPT_QUESTION = (
    "Guess a 24-hour likely diary for a person "
    + "({age} year old, {gender}, {work_status}, {income}, {others}). "
    + "Use a table with 'hour', 'activity', and 'location' columns. "
    + "The value for the column Locations are chosen from {locations_list}. "
    + "Activities should be one word. The schedule should run from 00:00 to 23:00."
)

DAY_TYPE_WEIGHT = {"weekday": {"weight": 5}, "weekend": {"weight": 2}}
