import cartopy.crs as ccrs
import matplotlib.pyplot as plt
from pandas import read_parquet

data_dir = "/tmp/syspop_test12/Wellington3/2023/median"  # /DSC/digital_twin/abm/PHA_report_202405/syspop/NZ/2023/median/
syspop_base_path = f"{data_dir}/syspop_base.parquet"
syspop_base_path = f"{data_dir}/syspop_base.parquet"
syspop_diary_path = f"{data_dir}/syspop_diaries.parquet"
syspop_address_path = f"{data_dir}/syspop_location.parquet"
syspop_household_path = f"{data_dir}/syspop_household.parquet"
sa2_id = 247900  # 241800

# /tmp/syspop_test12/Wellington2/2023/median/syspop_diaries.parquet
syspop_base = read_parquet(syspop_base_path)
syspop_diary = read_parquet(syspop_diary_path)
syspop_address = read_parquet(syspop_address_path)
syspop_hhd = read_parquet(syspop_household_path)


syspop_address = syspop_address[syspop_address["area"] == sa2_id]

syspop_diary = syspop_diary.dropna()
syspop_diary = syspop_diary[syspop_diary["location"].isin(syspop_address["name"])]
# syspop_diary = syspop_diary[syspop_diary["type"].isin(["school", "kindergarten"])]
syspop_diary_id = list(syspop_diary["id"].unique())

syspop_base = syspop_base[syspop_base["id"].isin(syspop_diary_id)]
not_from_the_same_sa2 = syspop_base[syspop_base["area"] != sa2_id]
from_the_same_sa2 = syspop_base[syspop_base["area"] == sa2_id]

syspop_hhd_not_from_the_same_sa2 = syspop_hhd[
    syspop_hhd["id"].isin(not_from_the_same_sa2["id"])
]["household"]
syspop_address_not_from_the_same_sa2 = syspop_address[
    syspop_address["name"].isin(syspop_hhd_not_from_the_same_sa2)
]

syspop_hhd_from_the_same_sa2 = syspop_hhd[
    syspop_hhd["id"].isin(from_the_same_sa2["id"])
]["household"]
syspop_address_from_the_same_sa2 = syspop_address[
    syspop_address["name"].isin(syspop_hhd_from_the_same_sa2)
]


fig = plt.figure(figsize=(10, 10))

# Create a map projection
ax = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())

# Add coastlines
ax.coastlines()

# Plot the data
plt.scatter(
    syspop_address_from_the_same_sa2["longitude"],
    syspop_address_from_the_same_sa2["latitude"],
    color="red",
    transform=ccrs.PlateCarree(),
)

# Plot the data
"""
plt.scatter(
    syspop_address_not_from_the_same_sa2["longitude"],
    syspop_address_not_from_the_same_sa2["latitude"],
    color="blue",
    transform=ccrs.PlateCarree(),
)
"""
plt.savefig("test.png")
plt.close()
