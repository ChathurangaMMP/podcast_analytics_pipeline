import logging
from logging import handlers
from pathlib import Path
import os

current_dir = Path(__file__).resolve().parent
logs_dir = current_dir.parent / "logs"

if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    '%(asctime)s|%(levelname)s|%(filename)s|%(funcName)s|%(message)s')
handler = handlers.TimedRotatingFileHandler(
    logs_dir / "log_file.log", when="H", interval=24)
handler.setFormatter(formatter)
logger.addHandler(handler)
