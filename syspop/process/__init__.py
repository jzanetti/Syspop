MAPING_DIARY_CFG_LLM_DIARY = {
    "company": ["office"],
    "household": ["home"],
}

DIARY_CFG = {
    "worker": {
        "random_seeds": ["household", "travel", "company"],
        "household": {
            "weight": 0.75,  # every 1.0 people, how many will do such an acitivity
            "time_ranges": [(0, 8), (15, 24)],
            "age_weight": None,
            "time_weight": {"0-5": 2.0, "5-6": 1.5, "20-23": 1.5},
            "max_occurrence": None,
        },
        "travel": {
            "weight": 0.15,
            "time_ranges": [(7, 9), (16, 19)],
            "age_weight": None,
            "time_weight": None,
            "max_occurrence": 6,
        },
        "company": {
            "weight": 0.8,
            "time_ranges": [(7, 18)],
            "age_weight": None,
            "time_weight": {"7-8": 0.5, "17-18": 0.3},
            "max_occurrence": None,
        },
        "supermarket": {
            "weight": 0.01,
            "time_ranges": [(17, 20)],
            "age_weight": None,
            "time_weight": {"17-18": 1.5},
            "max_occurrence": 1,
        },
        "restaurant": {
            "weight": 0.001,
            "time_ranges": [(11, 13), (18, 20)],
            "age_weight": None,
            "time_weight": {"11-13": 2.0, "18-20": 3.0},
            "max_occurrence": 1,
        },
        # "pharmacy": {
        #    "weight": 0.000001,
        #    "time_ranges": [(11, 17)],
        #    "age_weight": None,
        #    "time_weight": None,
        # },
    },
    "student": {
        "random_seeds": ["household"],
        "household": {
            "weight": 0.75,
            "time_ranges": [(0, 8), (15, 24)],
            "age_weight": None,
            "time_weight": {"0-6": 2.0, "20-21": 1.25, "21-23": 1.5},
            "max_occurrence": None,
        },
        "school": {
            "weight": 0.85,
            "time_ranges": [(9, 15)],
            "age_weight": None,
            "time_weight": None,
            "max_occurrence": None,
        },
        "supermarket": {
            "weight": 0.01,
            "time_ranges": [(12, 13), (16, 18)],
            "age_weight": None,
            "time_weight": {"16-17": 1.5},
            "max_occurrence": 1,
        },
        "restaurant": {
            "weight": 0.001,
            "time_ranges": [(11, 13), (18, 20)],
            "age_weight": None,
            "time_weight": {"11-13": 2.0, "18-20": 3.0},
            "max_occurrence": 1,
        },
        # "pharmacy": {
        #    "weight": 0.000001,
        #    "time_ranges": [(11, 17)],
        #    "age_weight": None,
        #    "time_weight": None,
        # },
    },
    "default": {
        "random_seeds": ["household", "supermarket"],
        "household": {
            "weight": 0.75,
            "time_ranges": [(0, 24)],
            "age_weight": {"0-5": 1.2, "70-80": 1.2, "80-999": 1.5},
            "time_weight": {"0-5": 2.0, "5-6": 1.5, "20-21": 1.5, "21-23": 2.0},
            "max_occurrence": None,
        },
        "supermarket": {
            "weight": 0.01,
            "time_ranges": [(8, 20)],
            "age_weight": {"0-5": 0.3, "60-70": 0.75, "70-80": 0.15, "80-999": 0.001},
            "time_weight": {"9-15": 3.0, "17-18": 0.15},
            "max_occurrence": 1,
        },
        "restaurant": {
            "weight": 0.001,
            "time_ranges": [(11, 13), (18, 20)],
            "age_weight": {"0-5": 0.1, "60-70": 1.2, "70-80": 0.3, "80-999": 0.00001},
            "time_weight": {"11-13": 1.5, "17-20": 3.0},
            "max_occurrence": 1,
        },
        # "pharmacy": {
        #    "weight": 0.000001,
        #    "time_ranges": [(11, 17)],
        #    "age_weight": None,
        #    "time_weight": None,
        # },
    },
}
