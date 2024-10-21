from pandas import read_csv

df = read_csv("/Users/sijinzhang/Downloads/STATSNZ,INC_INC_002,1.0+all.csv")
df = df.drop("DATAFLOW", axis=1)

BUSINESS_CONVERTER = {
    '1: Agriculture, Forestry and Fishing': "A",
    '2: Mining': "B",
    '3: Manufacturing': "C",
    '4: Electricity, Gas, Water and Waste Services': "D",
    '5: Construction': "E",
    '6: Wholesale Trade': "F",
    '7: Retail Trade and Accommodation': "G",
    '8: Transport, Postal and Warehousing': "I",
    '9: Information, Media and Telecommunications': "J",
    '10: Financial and Insurance Services': "K",
    '11: Rental, Hiring and Real Estate Services': "L",
    '12: Professional and Administrative Services': "M, N",
    '13: Public Administration and Safety': "O",
    '14: Education and Training': "P",
    '15: Health': "Q",
    '16: Art, Recreation and Other Services': "R",
    '17: Not Specified': "999"
}

AGE_CONVERTER = {
    "15 to 19": "15-19",
    '20 to 24': "20-24",
    '25 to 29': "25-29",
    '30 to 34': "30-34",
    '35 to 39': "35-39",
    '40 to 44': "40-44",
    '45 to 49': "45-49",
    '50 to 54': "50-54",
    '55 to 59': "55-59",
    "60 to 64": "60-64",
    "65 plus": "65-999"
}

df = df.rename(columns={
    "PERIOD_INC_INC_002: Year": "year",
    "IND2_INC_INC_002: Industry": "business_code",
    "SEX_INC_INC_002: Sex": "sex",
    "AGEGP_INC_INC_002: Age Group": "age",
    "ETHGP_INC_INC_002: Ethnic Group": "ethnicity",
    "MEASURE_INC_INC_002: Measure": "var",
    "OBS_VALUE": "value"
})
df = df[df['business_code'] != '98: Total All Industry Groups']
df = df[df['sex'] != '98: Total Both Sexes']
df = df[df['age'] != '98: Total Age Groups']
df = df[df['ethnicity'] != '98: Total Ethnic Groups']
df = df[df['var'] == 'MED_WEEK_INC']
df = df.drop("var", axis=1)

df["year"] = df["year"].str.split(':').str[1]
# df["business_code"] = df["business_code"].str.split(':').str[0]
df["sex"] = df["sex"].str.split(':').str[0]
df["age"] = df["age"].str.split(':').str[1]
df["ethnicity"] = df["ethnicity"].str.split(':').str[0]

df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

df = df[df["year"] == "2024"]
df = df.drop("year", axis=1)
df = df.replace(BUSINESS_CONVERTER)
df = df.replace(AGE_CONVERTER)

for item in ["sex", "ethnicity", "value"]:
    df[item] = df[item].astype(int)

df = df.reset_index(drop=True)

df.to_csv("etc/data/open_data/income.csv", index=False)