DIARY_CFG = {
    "worker": {
        "household": {
            "weight": 0.5,
            "time_ranges": [(0, 8), (15, 24)],
            "age_weight": None,
            "time_weight": {"0-6": 1.5, "21-23": 1.25},
        },
        "travel": {
            "weight": 0.1,
            "time_ranges": [(7, 9), (16, 19)],
            "age_weight": None,
            "time_weight": None,
        },
        "company": {
            "weight": 0.6,
            "time_ranges": [(7, 18)],
            "age_weight": None,
            "time_weight": {"7-8": 0.5, "17-18": 0.3},
        },
        "supermarket": {
            "weight": 0.01,
            "time_ranges": [(17, 20)],
            "age_weight": None,
            "time_weight": {"17-18": 1.5},
        },
        "restaurant": {
            "weight": 0.001,
            "time_ranges": [(11, 13), (18, 20)],
            "age_weight": None,
            "time_weight": {"11-13": 10.0, "18-20": 30.0},
        },
        # "pharmacy": {
        #    "weight": 0.000001,
        #    "time_ranges": [(11, 17)],
        #    "age_weight": None,
        #    "time_weight": None,
        # },
    },
    "student": {
        "household": {
            "weight": 0.5,
            "time_ranges": [(0, 8), (15, 24)],
            "age_weight": None,
            "time_weight": {"0-6": 1.5, "21-23": 1.25},
        },
        "school": {
            "weight": 0.5,
            "time_ranges": [(9, 15)],
            "age_weight": None,
            "time_weight": None,
        },
        "supermarket": {
            "weight": 0.01,
            "time_ranges": [(12, 13), (16, 18)],
            "age_weight": None,
            "time_weight": {"16-17": 1.0},
        },
        "restaurant": {
            "weight": 0.001,
            "time_ranges": [(11, 13), (18, 20)],
            "age_weight": None,
            "time_weight": {"11-13": 10.0, "18-20": 15.0},
        },
        # "pharmacy": {
        #    "weight": 0.000001,
        #    "time_ranges": [(11, 17)],
        #    "age_weight": None,
        #    "time_weight": None,
        # },
    },
    "default": {
        "household": {
            "weight": 0.6,
            "time_ranges": [(0, 24)],
            "age_weight": {"0-5": 1.2, "70-80": 1.2, "80-999": 1.5},
            "time_weight": {"0-6": 1.5, "21-23": 1.25},
        },
        "supermarket": {
            "weight": 0.01,
            "time_ranges": [(8, 20)],
            "age_weight": {"0-5": 0.3, "60-70": 0.75, "70-80": 0.15, "80-999": 0.001},
            "time_weight": {"9-15": 7.5, "17-18": 0.15},
        },
        "restaurant": {
            "weight": 0.001,
            "time_ranges": [(11, 13), (18, 20)],
            "age_weight": {"0-5": 0.1, "60-70": 1.2, "70-80": 0.3, "80-999": 0.00001},
            "time_weight": {"11-13": 5.0, "17-20": 15.0},
        },
        # "pharmacy": {
        #    "weight": 0.000001,
        #    "time_ranges": [(11, 17)],
        #    "age_weight": None,
        #    "time_weight": None,
        # },
    },
}
