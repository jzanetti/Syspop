from random import randint as random_randint

from funcs import LOCATIONS_CFG
from matplotlib.pyplot import close, cm, gca, legend, savefig, title, xlabel, ylabel
from matplotlib.ticker import FuncFormatter
from pandas import DataFrame


def plot_diary_percentage(
    data_to_plot: DataFrame,
    diary_vis_path: str,
    title_str: str or None = None,
    color_unknown: str = "#bfbfbd",
):
    """Plot diary percentage

    Args:
        output_path (str): _description_
    """

    df_grouped = data_to_plot.groupby(["Hour", "Location"]).size().unstack(fill_value=0)
    df_percentage = df_grouped.divide(df_grouped.sum(axis=1), axis=0)

    colors = []
    for col in df_percentage.columns:
        if col not in LOCATIONS_CFG:
            colors.append(color_unknown)
        else:
            colors.append(LOCATIONS_CFG[col]["color"])

    df_percentage.plot(
        kind="bar",
        stacked=True,
        figsize=(10, 7),
        color=colors,
    )

    def to_percentage(y, _):
        return "{:.0%}".format(y)

    # Apply percentage formatting to y-axis ticks
    formatter = FuncFormatter(to_percentage)
    gca().yaxis.set_major_formatter(formatter)

    title_str_base = "Percentage of Different Locations for Each Hour"

    if title_str is not None:
        title_str_base += f" \n {title_str}"

    title(f"{title_str_base}")
    xlabel("Hour")
    ylabel("Percentage")
    legend(title="Location")

    savefig(
        diary_vis_path,
        bbox_inches="tight",
    )
    close()
