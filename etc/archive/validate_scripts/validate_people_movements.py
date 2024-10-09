import cartopy.crs as ccrs
import cartopy.io.img_tiles as cimgt
import matplotlib.pyplot as plt
from pandas import read_parquet

data_dir = "/DSC/digital_twin/abm/synthetic_population/v3.0/Wellington"  # /DSC/digital_twin/abm/PHA_report_202405/syspop/NZ/2023/median/
syspop_base_path = f"{data_dir}/syspop_base.parquet"
syspop_base_path = f"{data_dir}/syspop_base.parquet"
syspop_diary_path = f"{data_dir}/syspop_diaries.parquet"
syspop_address_path = f"{data_dir}/syspop_location.parquet"
syspop_household_path = f"{data_dir}/syspop_household.parquet"
sa2_id = 242800  # 241800 	242800 247900 251400 (Wellington Central)

# /tmp/syspop_test12/Wellington2/2023/median/syspop_diaries.parquet
syspop_base = read_parquet(syspop_base_path)
syspop_diary = read_parquet(syspop_diary_path)
syspop_address = read_parquet(syspop_address_path)
syspop_hhd = read_parquet(syspop_household_path)


syspop_address_sa2 = syspop_address[syspop_address["area"] == sa2_id].dropna()

syspop_diary_sa2 = syspop_diary[
    syspop_diary["location"].isin(syspop_address_sa2["name"])
]
syspop_diary_sa2_id = list(syspop_diary_sa2["id"].unique())

syspop_base_sa2 = syspop_base[syspop_base["id"].isin(syspop_diary_sa2_id)]
not_from_the_same_sa2 = syspop_base_sa2[syspop_base_sa2["area"] != sa2_id]
from_the_same_sa2 = syspop_base_sa2[syspop_base_sa2["area"] == sa2_id]

not_from_the_same_sa2_diary = syspop_diary_sa2[
    syspop_diary_sa2["id"].isin(not_from_the_same_sa2["id"])
]

from_the_same_sa2_diary = syspop_diary_sa2[
    syspop_diary_sa2["id"].isin(from_the_same_sa2["id"])
]

for proc_type in list(syspop_diary_sa2["type"].unique()):
    xx = not_from_the_same_sa2_diary[not_from_the_same_sa2_diary["type"] == proc_type]
    yy = from_the_same_sa2_diary[from_the_same_sa2_diary["type"] == proc_type]
    total_people_xx = len(xx["id"].unique())
    total_people_yy = len(yy["id"].unique())
    print(
        f"{proc_type}: {total_people_yy} (from inside) / {total_people_xx} (from outside)"
    )

all_diary_types = list(syspop_diary_sa2.type.unique())

all_diary_types.remove("household")

color_dict = {
    "park": "b",
    "company": "g",
    "school": "r",
    "cafe": "c",
    "kindergarten": "m",
    "supermarket": "y",
    "fast_food": "k",
    "restaurant": "#808080",
    "pub": "#ff00ff",
    "default": "#5f4373",
}


fig = plt.figure(figsize=(10, 10))
request = cimgt.OSM()
# Create a map projection
ax = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())
ax.add_image(request, 15)

lon_min = -9999.0
lon_max = 9999.0
lat_min = -9999.0
lat_max = 9999.0

for diary_type in all_diary_types:

    df = syspop_diary_sa2[syspop_diary_sa2["type"] == diary_type]
    selected_hhd = syspop_hhd[syspop_hhd["id"].isin(df.id)]

    try:
        proc_color = color_dict[diary_type]
    except KeyError:
        proc_color = color_dict["default"]
    merged_df = selected_hhd.merge(
        syspop_address[["latitude", "longitude", "name"]],
        left_on="household",
        right_on="name",
        how="left",
    )

    cur_lon_min = merged_df.longitude.min() - 0.05
    cur_lon_max = merged_df.longitude.max() + 0.05
    cur_lat_min = merged_df.latitude.min() - 0.05
    cur_lat_max = merged_df.latitude.max() + 0.05

    if cur_lon_min > lon_min:
        lon_min = cur_lon_min

    if cur_lon_max < lon_max:
        lon_max = cur_lon_max

    if cur_lat_min > lat_min:
        lat_min = cur_lat_min

    if cur_lat_max < lat_max:
        lat_max = cur_lat_max

    print(lon_min, lon_max, lat_min, lat_max)
    # ax.set_extent([lon_min, lon_max, lat_min, lat_max])

    # Plot the data
    plt.scatter(
        merged_df["longitude"],
        merged_df["latitude"],
        color=proc_color,
        label=diary_type,
        alpha=0.3,
        s=5,
        transform=ccrs.PlateCarree(),
    )
plt.legend()
plt.title(f"where agents come from for doing things in the SA2: {sa2_id}")
plt.savefig("test.png", bbox_inches="tight")
plt.close()
