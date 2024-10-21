from pandas import read_csv

all_df = read_csv("/Users/sijinzhang/Downloads/STATSNZ,CEN23_TBT_008,1.0+all.csv")

all_df = all_df[["CEN23_TBT_IND_003: Variable codes", "CEN23_TBT_GEO_006: Area", "CEN23_YEAR_001: Census year", "OBS_VALUE"]]

all_df = all_df.rename(columns={
    "CEN23_TBT_IND_003: Variable codes": "var",
    "CEN23_TBT_GEO_006: Area": "area",
    "CEN23_YEAR_001: Census year": "year",
    "OBS_VALUE": "value"
})

all_df = all_df[(all_df['area'].str.match(r'^[^:]{6}:')) & (all_df["year"] == 2023)]
all_df['area'] = all_df['area'].str.extract(r'^([^:]{6}):')
all_df = all_df[~all_df['area'].isin(["999999", "HTotal"])]
all_df['area'] = all_df['area'].astype("int")

# ---------------------------------
# Process industrial/business code
# ---------------------------------
df = all_df[all_df["var"].str.startswith("inu")]
df = df[~df['var'].isin([
    "inuTotal: Industry, by usual residence address - total employed census usually resident population count aged 15 years and over",
    "inuTS: Total stated - industry, by usual residence address"
])]
df["var"] = df["var"].str.extract(r"inu(\w):")
df = df.rename(columns={"var": "business_code"})
df = df[["area", "business_code", "value"]]
df = df.reset_index(drop=True)

df.to_csv("etc/data/open_data/employee_structure_2023.csv")

# ---------------------------------
# Process income
# ---------------------------------
df = all_df[all_df["var"].str.startswith("ib")]
df = df[~df['var'].isin([
    "ibmed: Median ($) - total personal income",
    "ibTotal: Total personal income - total census usually resident population count aged 15 years and over",
    "ibTS: Total stated - total personal income"
])]
df["var"] = df["var"].replace({
    "ib1: $10,000 or less": "<10000",
    "ib2: $10,001-$20,000": "10000-20000",
    "ib3: $20,001-$30,000": "20000-30000",
    "ib4: $30,001-$50,000": "30000-50000",
    "ib5: $50,001-$70,000": "50000-70000",
    "ib6: $70,001-$100,000": "70000-100000",
    "ib7: $100,001 or more": ">100000",
    "ib9: Not stated": "unknown"
    })
df = df.rename(columns={"var": "income"})
df = df[["area", "income", "value"]]
df = df.reset_index(drop=True)
df.to_csv("etc/data/open_data/income_structure_2023.csv")

# ---------------------------------
# Process employment status
# ---------------------------------
df = all_df[all_df["var"].str.startswith("se")]
df = df[~df['var'].isin([
    "seTotal: Status in employment - total employed census usually resident population count aged 15 years and over"
])]
df["var"] = df["var"].replace({
    "se1: Paid employee": "employee",
    "se2: Employer": "employer",
    "se3: Self-employed and without employees": "self-employed",
    "se4: Unpaid family worker": "unpaid family worker",
    "se9: Not elsewhere included": "unknown",
    "seTS: Total stated - status in employment": "total"
    })

df = df.rename(columns={"var": "employment_status"})
df = df[["area", "employment_status", "value"]]
df = df.reset_index(drop=True)
df.to_csv("etc/data/open_data/employment_status_2023.csv", index=False)

