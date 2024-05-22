from collections import Counter
from os.path import join

from folium import Map as folium_map
from folium import PolyLine as folium_polyline
from folium.plugins import HeatMap
from matplotlib.pyplot import (
    axvline,
    bar,
    clim,
    close,
    colorbar,
    legend,
    plot,
    savefig,
    scatter,
    subplots,
    title,
    xlabel,
    xlim,
    xticks,
    ylabel,
    ylim,
)
from numpy import arange as numpy_arange
from numpy import polyfit as numpy_polyfit
from pandas import DataFrame, merge
from seaborn import histplot as sns_hisplot


def validate_vis_movement(
    val_dir: str,
    model_data: DataFrame,
    truth_data: DataFrame,
    merge_method: str = "inner",  # left, inner
    apply_factor: bool = False,
):

    x = model_data[model_data["total"] > 50]
    y = truth_data[truth_data["total"] > 50]
    x = x[x["area_home"] != x["area_work"]]
    y = y[y["area_home"] != y["area_work"]]

    merged_df = merge(
        x, y, on=["area_home", "area_work"], suffixes=("_x", "_y"), how=merge_method
    )

    if len(merged_df) == 0:
        return

    if apply_factor:
        factor = merged_df["total_x"].sum() / merged_df["total_y"].sum()
        merged_df["total_y"] = merged_df["total_y"] * factor

    min_value = min(merged_df[["area_home", "area_work"]].min())
    max_value = min(merged_df[["area_home", "area_work"]].max())

    plot([min_value, max_value], [min_value, max_value], "k")

    scatter(
        merged_df["area_home"],
        merged_df["area_work"],
        c=merged_df["total_x"],
        s=merged_df["total_x"],
        cmap="jet",
        alpha=0.5,
    )
    title("Synthetic population")
    colorbar()
    xlim(min_value - 1000, max_value + 1000)
    ylim(min_value - 1000, max_value + 1000)
    clim([50, 450])
    xlabel("SA2")
    ylabel("SA2")
    savefig(join(val_dir, "validation_work_commute_pop.png"), bbox_inches="tight")
    close()

    plot([min_value, max_value], [min_value, max_value], "k")
    scatter(
        merged_df["area_home"],
        merged_df["area_work"],
        c=merged_df["total_y"],
        s=merged_df["total_y"],
        cmap="jet",
        alpha=0.5,
    )
    title("Census 2018")
    colorbar()
    xlim(min_value - 1000, max_value + 1000)
    ylim(min_value - 1000, max_value + 1000)
    clim([50, 450])
    xlabel("SA2")
    ylabel("SA2")
    savefig(join(val_dir, "validation_work_commute_census.png"), bbox_inches="tight")
    close()


def validate_vis_plot(
    output_dir: str,
    err_data: dict,
    data_title: str,
    output_filename: str,
    x_label: str = None,
    y_label: str = None,
    plot_ratio: bool = True,
):
    """Validate vis for plot

    Args:
        output_dir (str): Output directory
        err_ratio (dict): Error ratio
        data_title (str): Data type, e.g., error for age vs gender
        x_label (str, optional): x label in text. Defaults to None.
        y_label (str, optional): y label in text. Defaults to None.
    """
    if plot_ratio:
        plot(err_data, "k.")
    else:
        plot(err_data["truth"], "b.", label="truth", alpha=0.5)
        plot(err_data["model"], "r.", label="model", alpha=0.5)
        legend()
    title(data_title)
    xlabel(x_label)
    ylabel(y_label)
    savefig(join(output_dir, f"{output_filename}.png"), bbox_inches="tight")
    close()


def validate_vis_barh(
    output_dir: str,
    err_data: dict,
    data_title: str,
    output_filename: str,
    x_label: str = None,
    y_label: str = None,
    plot_ratio: bool = True,
    add_polyfit: bool = False,
    figure_size: tuple or None = None,
):
    """Validdate vis for barh

    Args:
        output_dir (str): Output directory
        err_ratio (dict): Error ratio
        data_title (str): Data type, e.g., error for age vs gender
        x_label (str, optional): x label in text. Defaults to None.
        y_label (str, optional): y label in text. Defaults to None.
    """
    # Create figure and axes
    if figure_size is None:
        fig, ax = subplots()
    else:
        fig, ax = subplots(figsize=figure_size)
    fig.tight_layout()

    # Set bar width
    bar_width = 0.35

    if plot_ratio:
        # Get keys and values
        keys = err_data.keys()
        x_vals = err_data.values()

        # Arrange keys on x-axis
        index = range(len(keys))
        ax.set_yticks(index)

        # Create bars for 'x' and 'y'
        ax.barh(index, list(x_vals), bar_width, color="b", label="Error percentage")
        axvline(x=0, color="red", linestyle="--", linewidth=2)
    else:
        keys = list(err_data["truth"].keys())
        truth_values = list(err_data["truth"].values())
        model_values = list(err_data["model"].values())

        # Create an array with the positions of each bar along the y-axis
        y_pos = numpy_arange(len(keys))

        # Create a horizontal bar chart
        ax.barh(y_pos - 0.2, truth_values, 0.4, color="blue", label="truth")
        ax.barh(y_pos + 0.2, model_values, 0.4, color="red", label="model")

        ax.set_yticks(y_pos, keys)

        if add_polyfit:
            # Fit a line to the truth values
            truth_fit = numpy_polyfit(y_pos, truth_values, 3)
            truth_fit_fn = numpy_polyfit(truth_fit)
            plot(truth_fit_fn(y_pos), y_pos, color="blue")

            # Fit a line to the model values
            model_fit = numpy_polyfit(y_pos, model_values, 3)
            model_fit_fn = numpy_polyfit(model_fit)
            plot(model_fit_fn(y_pos), y_pos, color="red")

    # Labeling
    if x_label is not None:
        ax.set_xlabel(x_label)

    if y_label is not None:
        ax.set_ylabel(y_label)

    ax.set_title(f"{data_title}")
    ax.set_yticklabels(keys)
    ax.legend()
    savefig(join(output_dir, f"{output_filename}.png"), bbox_inches="tight")
    close()


def plot_pie_charts(output_dir: str, df: DataFrame):
    """Plot pie charts for a dataframe

    Args:
        output_dir (str): Where to save the plots
        df (DataFrame): dataframe to plot
    """
    for column in df.columns:
        if df[column].dtype in ["int64", "float64"]:
            sns_hisplot(df[column], kde=True)  # For numerical columns, use a histogram
        else:
            df[column].value_counts().plot(
                kind="pie"
            )  # For categorical columns, use a pie chart
        title(f"Distribution for {column}")
        savefig(join(output_dir, f"{column}.png"), bbox_inches="tight")
        close()


def plot_map_html(output_dir: str, df: DataFrame, data_name: str):
    # Create a map centered at an average location
    m = folium_map(
        location=[df["latitude"].mean(), df["longitude"].mean()], zoom_start=14
    )

    # Add a heatmap to the map
    HeatMap(data=df, radius=8, max_zoom=13).add_to(m)

    # Display the map
    m.save(join(output_dir, f"{data_name}.html"))


def plot_travel_html(output_dir: str, df: DataFrame, data_name: str):
    """Plot travel trips

    Args:
        output_dir (str): Where to store the data
        df (DataFrame): data to be plotted
        data_name (str): data name to be stored
    """

    m = folium_map(
        location=[df["start_lat"].mean(), df["start_lon"].mean()],
        zoom_start=13,
        prefer_canvas=True,
    )

    for idx, row in df.iterrows():
        folium_polyline(
            [(row["start_lat"], row["start_lon"]), (row["end_lat"], row["end_lon"])],
            color="red",
            weight=2.5,
            opacity=1,
        ).add_to(m)

    # Display the map
    m.save(join(output_dir, f"{data_name}.html"))


def plot_location_timeseries_charts(output_dir: str, location_counts: dict):

    for proc_loc in location_counts:
        proc_data = location_counts[proc_loc]
        # Create lists for the plot
        hours = list(proc_data.keys())
        people = list(proc_data.values())

        plot(hours, people, marker="o")  # Plot the data
        xticks(range(0, max(hours)))  # Set the x-ticks to be hourly
        xlabel("Hour")  # Set x-axis label
        ylabel("Number of People")  # Set y-axis label
        title(f"Number of People by Hour \n {proc_loc}")  # Set title
        savefig(join(output_dir, f"{proc_loc}_distribution.png"), bbox_inches="tight")
        close()


def plot_location_occurence_charts_by_hour(
    output_dir: str,
    location_counts: dict,
    hour: int,
    data_type: str,
):
    counts = list(location_counts.values())

    counts_processed = Counter(counts)

    # Extract unique values and their counts
    values = list(counts_processed.keys())
    occurrences = list(counts_processed.values())

    # Plotting
    bar(values, occurrences)
    title(
        f"Number of People distribution \n Hour: {hour}, Data: {data_type}"
    )  # Set title
    xlabel("Number of people")
    ylabel(f"Number of places ({data_type})")
    savefig(join(output_dir, f"{data_type}_{hour}_hist.png"), bbox_inches="tight")
    close()


def plot_average_occurence_charts(output_dir: str, data_counts: list, data_type: str):

    plot(data_counts)
    title(f"Average number of people per {data_type}")
    xlabel("Hour")
    ylabel("Number of people")
    savefig(
        join(output_dir, f"average_{data_type}_timeseries.png"), bbox_inches="tight"
    )
    close()
