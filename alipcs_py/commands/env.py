from pathlib import Path

# Local data directory
DATA_DIR = Path("~").expanduser() / ".alipcs-py"

# Account data path
ACCOUNT_DATA_PATH = DATA_DIR

# The prefix and suffix of account data file
ACCOUNT_DATA_FILE_PREFIX = "account-"
ACCOUNT_DATA_FILE_SUFFIX = ".pk"

# Localstorage data path
RAPIDUPLOADINFO_PATH = DATA_DIR / "rapiduploadinfo.sqlite3"

# Logging path
LOG_PATH = DATA_DIR / "running.log"

# Logging level
LOG_LEVEL = "CRITICAL"
