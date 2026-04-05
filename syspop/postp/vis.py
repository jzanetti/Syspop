import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from os.path import join as os_path_join


def plot_distribution(df, columns, output_dir="./output", dropna=True):
    """
    Plots the distribution of 1 or 2 columns from a DataFrame and saves it as a PNG.

    Parameters:
    - df: The pandas DataFrame.
    - columns: A single string (column name) or a list of 1 or 2 column names.
    - dropna: Boolean, whether to drop NaN values before plotting.
    """
    # Ensure columns is a list
    if isinstance(columns, str):
        columns = [columns]

    # Drop NAs if requested
    plot_df = df.dropna(subset=columns) if dropna else df.copy()

    output_filename = os_path_join(output_dir, f"distribution_{'_'.join(columns)}.png")

    if len(columns) == 1:
        col = columns[0]
        fig, ax = plt.subplots(figsize=(10, 6))

        # Check if the column is categorical/discrete (< 20 unique values) or text
        if plot_df[col].dtype == "object" or plot_df[col].nunique() < 20:
            # Sorted Bar Chart (X-axis sorted alphabetically/numerically)
            order = sorted(plot_df[col].unique())

            sns.countplot(data=plot_df, x=col, order=order, ax=ax, palette="viridis")
            ax.set_title(f"Distribution of {col}", fontsize=14)
            ax.set_ylabel("Count")
            plt.xticks(rotation=45, ha="right")  # Rotate labels to prevent overlapping
        else:
            # Histogram for continuous variables (e.g., income)
            sns.histplot(data=plot_df, x=col, kde=True, bins=30, ax=ax, color="blue")
            ax.set_title(f"Distribution of {col}", fontsize=14)
            ax.set_ylabel("Frequency")

        plt.tight_layout()

    elif len(columns) == 2:
        col1, col2 = columns
        fig, ax = plt.subplots(figsize=(12, 8))

        # Create a 2D cross-tabulation (count matrix)
        crosstab = pd.crosstab(plot_df[col1], plot_df[col2])

        # Plot Heatmap
        sns.heatmap(
            crosstab, annot=False, cmap="YlGnBu", ax=ax, cbar_kws={"label": "Count"}
        )
        ax.set_title(f"Heatmap of {col1} vs {col2}", fontsize=14)

        # Rotate labels nicely
        plt.xticks(rotation=45, ha="right")
        plt.yticks(rotation=0)

        plt.tight_layout()

    # Save the figure and close the plot to free up memory
    plt.savefig(output_filename)
    plt.close()
