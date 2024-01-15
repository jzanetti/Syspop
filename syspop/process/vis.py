from os.path import join

from matplotlib.pyplot import (
    axvline,
    close,
    legend,
    plot,
    savefig,
    subplots,
    title,
    xlabel,
    ylabel,
)
from numpy import arange as numpy_arange
from numpy import polyfit as numpy_polyfit


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
    _, ax = subplots()

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
