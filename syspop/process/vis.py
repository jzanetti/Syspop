from os.path import join

from matplotlib.pyplot import (
    axvline,
    close,
    plot,
    savefig,
    subplots,
    title,
    xlabel,
    ylabel,
)


def validate_vis_plot(
    output_dir: str,
    err_ratio: dict,
    data_title: str,
    output_filename: str,
    x_label: str = None,
    y_label: str = None,
):
    """Validate vis for plot

    Args:
        output_dir (str): Output directory
        err_ratio (dict): Error ratio
        data_title (str): Data type, e.g., error for age vs gender
        x_label (str, optional): x label in text. Defaults to None.
        y_label (str, optional): y label in text. Defaults to None.
    """
    plot(err_ratio, "k.")
    title(data_title)
    xlabel(x_label)
    ylabel(y_label)
    savefig(join(output_dir, f"{output_filename}.png"), bbox_inches="tight")
    close()


def validate_vis_barh(
    output_dir: str,
    err_ratio: dict,
    data_title: str,
    output_filename: str,
    x_label: str = None,
    y_label: str = None,
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

    # Get keys and values
    keys = err_ratio.keys()
    x_vals = err_ratio.values()

    # Arrange keys on x-axis
    index = range(len(keys))

    # Create bars for 'x' and 'y'
    ax.barh(index, list(x_vals), bar_width, color="b", label="Error percentage")
    axvline(x=0, color="red", linestyle="--", linewidth=2)

    # Labeling
    if x_label is not None:
        ax.set_xlabel(x_label)

    if y_label is not None:
        ax.set_ylabel(y_label)

    ax.set_title(f"{data_title}")
    ax.set_yticks(index)
    ax.set_yticklabels(keys)
    ax.legend()
    savefig(join(output_dir, f"{output_filename}.png"), bbox_inches="tight")
    close()
