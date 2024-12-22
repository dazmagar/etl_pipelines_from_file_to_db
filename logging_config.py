import logging
from pathlib import Path

log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)


logger = logging.getLogger("etl_pipeline")
logger.setLevel(logging.DEBUG)


console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_format = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
console_handler.setFormatter(console_format)
logger.addHandler(console_handler)


file_handler = logging.FileHandler(log_dir / "etl_pipeline.log")
file_handler.setLevel(logging.DEBUG)
file_format = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
file_handler.setFormatter(file_format)
logger.addHandler(file_handler)
