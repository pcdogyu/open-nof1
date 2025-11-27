import copy
import logging.config
from pathlib import Path

import uvicorn
from uvicorn.config import LOGGING_CONFIG

custom_logging = copy.deepcopy(LOGGING_CONFIG)
log_format = "%(asctime)s | %(levelprefix)s %(name)s | %(message)s"
custom_logging["formatters"]["default"]["fmt"] = log_format
custom_logging["formatters"]["access"]["fmt"] = (
    "%(asctime)s | %(levelprefix)s %(client_addr)s - \"%(request_line)s\" %(status_code)s"
)
custom_logging["formatters"]["default"]["datefmt"] = "%Y-%m-%d %H:%M:%S"
custom_logging["formatters"]["access"]["datefmt"] = "%Y-%m-%d %H:%M:%S"
# Elevate all uvicorn loggers to DEBUG level
for logger_name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
    if logger_name in custom_logging["loggers"]:
        custom_logging["loggers"][logger_name]["level"] = "DEBUG"

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIRECTORIES = (
    "data",
    "data/logs",
)

reload_excludes = [
    pattern
    for directory in DATA_DIRECTORIES
    for pattern in (
        directory,
        f"{directory}/*",
        f"{directory}/**/*",
    )
]
# Prevent JSONL/CSV log streams from triggering reloads
reload_excludes.extend(["*.jsonl", "*.csv", "*.log"])

if __name__ == "__main__":
    logging.config.dictConfig(custom_logging)
    uvicorn.run(
        "services.webapp.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        reload_dirs=[str(PROJECT_ROOT)],
        reload_excludes=reload_excludes,
        log_config=None,
    )
