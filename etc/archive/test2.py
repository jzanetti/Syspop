import pandas

data1 = pandas.read_parquet("/tmp/syspop_test/Auckland/syspop_diaries.parquet")
data2 = pandas.read_parquet("/tmp/syspop_test/Auckland/tmp/syspop_diaries_type.parquet")

df_melted1 = data1.melt(id_vars="id", var_name="hour", value_name="spec")
df_melted2 = data2.melt(id_vars="id", var_name="hour", value_name="spec")
merged_df = pandas.merge(df_melted1, df_melted2, on=["id", "hour"], how="left")

merged_df = merged_df.rename(columns={"spec_x": "location", "spec_y": "type"})
merged_df = merged_df[["id", "hour", "type", "location"]]
