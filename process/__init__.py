from logging import basicConfig
from logging import INFO

TMP_DIR = "/tmp"

# Configure logging
basicConfig(
    filename="progress.log",  # or use None to print to console
    level=INFO,  # DEBUG, INFO, WARNING, ERROR, CRITICAL
    format="%(asctime)s - %(levelname)s - %(message)s",
)
