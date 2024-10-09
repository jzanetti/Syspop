from syspop.start import vis as syspop_vis

from warnings import filterwarnings
filterwarnings("ignore")

# output_dir = "/tmp/syspop_test17/Wellington_test_v2.0"
output_dir = "/tmp/syspop"
syspop_vis(
    output_dir=output_dir,
    plot_distribution=True,
    plot_travel=True,
    plot_location=True,
    plot_diary=True,
)