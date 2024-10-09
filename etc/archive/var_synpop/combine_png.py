import os

import imageio

png_dir = "test/"  # Directory of png files

for proc_filename in [
    "census_comparisons",
    "household_composition_survey_comparisons",
    "imms_survey_comparisons",
    "loss",
]:
    images = []

    file_names = []
    for i in range(0, 100, 5):
        file_names.append(f"{proc_filename}_{i}.png")

    for filename in file_names:
        images.append(imageio.imread(png_dir + filename))

    # Save frames/images as a GIF
    imageio.mimsave(
        f"test/{proc_filename}.gif", images, fps=1.0, loop=0
    )  # duration is in seconds
