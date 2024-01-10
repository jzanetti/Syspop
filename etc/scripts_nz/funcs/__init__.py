# ------------------------
# Fixed value
# ------------------------
LOC_INDEX = {
    "household": 0,
    "city_transport": 1,
    "inter_city_transport": 2,
    "gym": 3,
    "grocery": 4,
    "pub": 5,
    "cinema": 6,
    "school": 7,
    "company": 8,
}

AGE_INDEX = {
    "0-10": 0,
    "11-20": 1,
    "21-30": 2,
    "31-40": 3,
    "41-50": 4,
    "51-60": 5,
    "61-999": 6,
}

ETHNICITY_INDEX = {"European": 0, "Maori": 1, "Pacific": 2, "Asian": 3, "MELAA": 4}

SEX_INDEX = {"m": 0, "f": 1}

TRAINING_ENS_MEMBERS = 1

RANDOM_ENSEMBLES = 2


# ---------------------------------
# Synthentic data information
# ---------------------------------
RAW_DATA = {
    "geography": {
        "geography_hierarchy": "etc/data/raw_nz/geography_hierarchy-2023.csv",
        "geography_location": "etc/data/raw_nz/geography_location-2018.csv",
        # due to the sizes of address and sa2 shapefile data, we are not able to save the data in git
        # the data can be downloaded in https://koordinates.com/data/?q=sa2+area
        "address_data": "etc/data/raw_nz/address_dataset/nz-addresses/nz-addresses.shp",
        "sa2_area_data": "etc/data/raw_nz/address_dataset/statistical-area-2-2022-generalised/statistical-area-2-2022-generalised.shp"
    },
    "population": {
        "socialeconomics": "etc/data/raw_nz/socialeconomics-2018.csv",
        "total_population": "etc/data/raw_nz/total_population-2018.xlsx",
        "population_by_age_by_gender": "etc/data/raw_nz/population_age_gender-2022.csv",
        "population_by_age_by_ethnicity": {
            0: "etc/data/raw_nz/population_age_ethnicity_under_15years-2018.xlsx",
            15: "etc/data/raw_nz/population_age_ethnicity_between_15_and_29years-2018.xlsx",
            30: "etc/data/raw_nz/population_age_ethnicity_between_30_and_64years-2018.xlsx",
            65: "etc/data/raw_nz/population_age_ethnicity_between_over_65years-2018.xlsx",
        },
        "population_by_age": "etc/data/raw_nz/population_by_age-2022.xlsx",
    },
    "business": {
        "employee_by_gender_by_sector": {
            "employee_by_area": "etc/data/raw_nz/employee_by_area-2022.csv",
            "leed": "etc/data/raw_nz/leed-2018.xlsx",
            "anzsic_code": "etc/data/raw_nz/anzsic06_code.csv",
            "geography_hierarchy": "etc/data/raw_nz/geography_hierarchy-2023.csv",
        },
        # "employers_by_employees_number": "etc/data/raw_nz/employers_by_employees_number-2022.csv",
        "employers_by_sector": "etc/data/raw_nz/employers_by_sectors-2022.csv",
    },
    "commute": {
        "workplace_and_home_locations": {
            "travel-to-work-info": "etc/data/raw_nz/travel-to-work-info-2018.csv",
            "population_by_gender": "etc/data/raw_nz/population_by_gender-2022.csv",
        },
    },
    "household": {"household_number": "etc/data/raw_nz/household_by_children_number-2018.csv"},
    "venue": {
        "hospital": "etc/data/raw_nz/nz-facilities-2022.csv",
        "school": "etc/data/raw_nz/nz-facilities-2022.csv",
        "supermarket": "etc/data/raw_nz/shop_supermarket.csv",
        "restaurant": "etc/data/raw_nz/amenity_restaurant.csv"
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

