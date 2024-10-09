DEPENDENT_CHILDREN_COUNT_CODE = {
    11: 0,
    111: 1,
    211: 2,
    311: 3,
    411: 4,
    412: 5,
    413: 6,
    414: 7,
    415: 8,
    416: 9,
    417: 10,
    418: 11,
    419: 12,
    420: 13,
    421: 14,
    422: 15,
    911: None,
}


# ---------------------------------
# Synthentic data information
# ---------------------------------
RAW_DATA_DIR = "etc/data/raw_wellington_latest"
PROJ_DATA_DIR = "etc/data/projection_data/nz"

RAW_DATA = {
    "projection": {
        "population": {
            "population_by_age_by_gender": f"{PROJ_DATA_DIR}/POPPR_SUB_007.csv",
            "population_by_ethnicity": f"{PROJ_DATA_DIR}/POPPR_ETH_010.csv",
        },
        "business": {"labours": f"{PROJ_DATA_DIR}/POPPR_LAB_001.csv"},
    },
}

REGION_CODES = {
    "North Island": [1, 2, 3, 4, 5, 6, 7, 8, 9],
    "South Island": [12, 13, 14, 15, 16, 17, 18],
    "Others": [99],
}
REGION_NAMES_CONVERSIONS = {
    1: "Northland",
    14: "Otago",
    3: "Waikato",
    99: "Area Outside",
    2: "Auckland",
    4: "Bay of Plenty",
    8: "Manawatu-Whanganui",
    15: "Southland",
    6: "Hawke's Bay",
    5: "Gisborne",
    7: "Taranaki",
    9: "Wellington",
    17: "Nelson",
    18: "Marlborough",
    13: "Canterbury",
    16: "Tasman",
    12: "West Coast",
}


RAW_DATA_INFO = {
    "base": {
        "business": {"employee_by_gender_by_sector": {"employment_rate": 0.7}},
        "venue": {
            "school": {
                "school_age_table": {
                    "Secondary (Year 9-15)": "secondary (14-19)",
                    "Composite": "primary_secondary (5-19)",
                    "Full Primary": "primary (5-13)",
                    "Secondary (Year 7-15)": "secondary (11-19)",
                    "Contributing": "primary (5-11)",
                    "Special School": "primary_secondary (8-15)",
                    "Secondary (Year 7-10)": "secondary (11-14)",
                    "Intermediate": "secondary (11-13)",
                    "Secondary (Year 11-15)": "secondary (15-19)",
                    "Restricted Composite (Year 7-10)": "secondary (11-14)",
                    "Composite (Year 1-10)": "primary_secondary (5-14)",
                    "Activity Centre": "secondary (14-17)",
                }
            },
            "leisures": {
                "osm_query_key": {
                    "cinema": {"amenity": ["cinema"]},
                    "pub": {"amenity": ["pub"]},
                    "gym": {"leisure": ["fitness_centre"]},
                    "grocery": {"shop": ["convenience", "supermarket"]},
                    "dormitory": {"building": ["dormitory"]},
                }
            },
        },
    }
}


AREAS_CONSISTENCY_CHECK = {
    "geography_hierarchy_data": {"super_area": "super_area", "area": "area"},
    "geography_location_super_area_data": {"super_area": "super_area"},
    "geography_location_area_data": {"area": "area"},
    "geography_name_super_area_data": {"super_area": "super_area"},
    "socialeconomic_data": {"area": "area"},
    "female_ratio_data": {"area": "output_area"},
    "ethnicity_and_age_data": {"area": "output_area"},
    "age_data": {"area": "output_area"},
    "employee_by_gender_by_sector_data": {"area": "oareas"},
    "employers_by_employees_number_data": {"super_area": "MSOA"},
    "employers_by_sector_data": {"super_area": "MSOA"},
    "population_travel_to_work_by_method_data": {"area": "geography"},
    "household_number_data": {"area": "output_area"},
    "workplace_and_home_locations_data": {
        "super_area": ["Area of residence", "Area of workplace"]
    },
}


POPULATION_STRUCTURE_CODE = {
    "ethnicity": {1: "European", 2: "Maori", 3: "Pacific", 4: "Asian", 5: "MELAA"},
    "gender": {1: "male", 2: "female", 3: "other"}
}