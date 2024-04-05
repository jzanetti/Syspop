from pandas import read_excel

imms_data = read_excel(
    "etc/data/confidential_data/RT-356 Childhood imms - low SA2s.xlsx"
)

imms_data = imms_data[
    [
        "sa22018_code",
        "ethnic_code",
        "age_group",
        "fully_imms",
        "partial_imms",
        "pop",
    ]
]

mapping_dict = {
    "A": "Asian",
    "M": "Maori",
    "O": "Others",
    "P": "Pacific",
    "X": "Unknown",
}
imms_data["ethnic_code"] = imms_data["ethnic_code"].map(mapping_dict)

# Convert the columns to percentages
imms_data["fully_imms"] = imms_data["fully_imms"] / imms_data["pop"]
imms_data["partial_imms"] = imms_data["partial_imms"] / imms_data["pop"]

mapping_dict = {
    "1-4 Years": "1-4",
    "5-12 Years": "5-12",
    "13-17 Years": "13-17",
}

imms_data = imms_data[imms_data["age_group"].isin(list(mapping_dict.keys()))]

imms_data["age_group"] = imms_data["age_group"].map(mapping_dict)

imms_data = imms_data.rename(
    columns={"sa22018_code": "sa2", "ethnic_code": "ethnicity", "age_group": "age"}
)

imms_data = imms_data[["sa2", "ethnicity", "age", "fully_imms", "partial_imms"]]

imms_data.to_csv("etc/data/confidential_data/mmr_immunisation.csv", index=False)
