from os.path import join

from matplotlib.pyplot import close, savefig, subplots

"""
def validate_households(truth: dict, data: dict):
    # Create a figure and a set of subplots
    fig, ax = plt.subplots()

    # Plot data
    ax.bar(truth.keys(), truth.values(), width=0.4, label='True')
    ax.bar(data.keys(), data.values(), width=0.4, label='Syn population', align='edge')

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_xlabel('Children number')
    ax.set_ylabel('Household number')
    ax.set_title('Comparison of census and synthetic population')
    ax.legend()

    plt.savefig("test.png",  bbox_inches='tight')
    plt.close()
"""


def validate_vis_barh(
    output_dir: str,
    err_ratio: dict,
    data_title: str,
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
    ax.barh(index, list(x_vals), bar_width, color="b", label="x")

    # Labeling
    if x_label is not None:
        ax.set_xlabel(x_label)

    if y_label is not None:
        ax.set_ylabel(y_label)

    ax.set_title(f"{data_title}")
    ax.set_yticks(index)
    ax.set_yticklabels(keys)
    ax.legend()
    data_title = data_title.replace(" ", "_")
    savefig(join(output_dir, f"{data_title}.png"), bbox_inches="tight")
    close()
