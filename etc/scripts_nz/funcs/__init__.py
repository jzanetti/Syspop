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
RAW_DATA_DIR = "etc/data/raw_nz_latest"

RAW_DATA = {
    "geography": {
        "geography_hierarchy": f"{RAW_DATA_DIR}/geography_hierarchy-2023.csv",
        "geography_location": f"{RAW_DATA_DIR}/geography_location-2018.csv",
        # due to the sizes of address and sa2 shapefile data, we are not able to save the data in git
        # the data can be downloaded in https://koordinates.com/data/?q=sa2+area
        "address_data": f"{RAW_DATA_DIR}/address_dataset/nz-addresses/nz-addresses.shp",
        "sa2_area_data": f"{RAW_DATA_DIR}/address_dataset/statistical-area-2-2022-generalised/statistical-area-2-2022-generalised.shp",
    },
    "population": {
        "socialeconomics": f"{RAW_DATA_DIR}/socialeconomics-2018.csv",
        "total_population": f"{RAW_DATA_DIR}/total_population-2018.xlsx",
        "population_by_age_by_gender": f"{RAW_DATA_DIR}/population_age_gender-2022.csv",
        "population_by_age_by_ethnicity": {
            0: f"{RAW_DATA_DIR}/population_age_ethnicity_under_15years-2018.xlsx",
            15: f"{RAW_DATA_DIR}/population_age_ethnicity_between_15_and_29years-2018.xlsx",
            30: f"{RAW_DATA_DIR}/population_age_ethnicity_between_30_and_64years-2018.xlsx",
            65: f"{RAW_DATA_DIR}/population_age_ethnicity_between_over_65years-2018.xlsx",
        },
        "population_by_age": f"{RAW_DATA_DIR}/population_by_age-2022.xlsx",
    },
    "business": {
        "employee_by_gender_by_sector": {
            "employee_by_area": f"{RAW_DATA_DIR}/employee_by_area-2022.csv",
            "leed": f"{RAW_DATA_DIR}/leed-2018.xlsx",
            "anzsic_code": f"{RAW_DATA_DIR}/anzsic06_code.csv",
            "geography_hierarchy": f"{RAW_DATA_DIR}/geography_hierarchy-2023.csv",
        },
        # "employers_by_employees_number": "etc/data/raw_nz/employers_by_employees_number-2022.csv",
        "employers_by_sector": f"{RAW_DATA_DIR}/employers_by_sectors-2022.csv",
    },
    "commute": {
        "workplace_and_home_locations": {
            "travel-to-work-info": f"{RAW_DATA_DIR}/travel-to-work-info-2018.csv",
            "population_by_gender": f"{RAW_DATA_DIR}/population_by_gender-2022.csv",
        },
    },
    "household": {
        "household_number": f"{RAW_DATA_DIR}/household_composition.csv"
    },  # "etc/data/raw_nz/household_by_children_number-2018.csv"},
    "venue": {
        "hospital": f"{RAW_DATA_DIR}/nz-facilities-2022.csv",
        "school": f"{RAW_DATA_DIR}/nz-facilities-2022.csv",
        "supermarket": f"{RAW_DATA_DIR}/shop_supermarket.csv",
        "restaurant": f"{RAW_DATA_DIR}/amenity_restaurant.csv",
        "pharmacy": f"{RAW_DATA_DIR}/amenity_pharmacy.csv",
        "cafe": f"{RAW_DATA_DIR}/amenity_cafe.csv",
        "pub": f"{RAW_DATA_DIR}/amenity_pub.csv",
        "fast_food": f"{RAW_DATA_DIR}/amenity_fast_food.csv",
        "museum": f"{RAW_DATA_DIR}/tourism_museum.csv",
        "events_venue": f"{RAW_DATA_DIR}/amenity_events_venue.csv",
        "wholesale": f"{RAW_DATA_DIR}/shop_wholesale.csv",
        "department_store": f"{RAW_DATA_DIR}/shop_department_store.csv",
        "park": f"{RAW_DATA_DIR}/leisure_park.csv",
        "kindergarten": f"{RAW_DATA_DIR}/amenity_kindergarten.csv",
        "childcare": f"{RAW_DATA_DIR}/amenity_childcare.csv",
    },
    "others": {"vaccine": f"{RAW_DATA_DIR}/nz-vaccine.csv"},
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
