from pathlib import Path

# Local data directory
DATA_DIR = Path("~").expanduser() / ".alipcs-py"

# Account data path
ACCOUNT_DATA_PATH = DATA_DIR / "accounts.pk"

# Rapiduploadinfo database path
RAPIDUPLOADINFO_PATH = DATA_DIR / "rapiduploadinfo.sqlite3"

# Shared store database path
SHARED_STORE_PATH = DATA_DIR / "shared-store.sqlite3"

# Logging path
LOG_PATH = DATA_DIR / "running.log"

# Logging level
LOG_LEVEL = "CRITICAL"

# Configuration path
CONFIG_PATH = DATA_DIR / "config.toml"
