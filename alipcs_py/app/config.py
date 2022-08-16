from alipcs_py.config import AppConfig
from alipcs_py.storage.tables import (
    connect_sqlite,
    bind_tables,
    create_tables,
    modify_table,
    PcsSharedLinkInfoTable,
    PcsFileTable,
)
from alipcs_py.commands.env import CONFIG_PATH, SHARED_STORE_PATH


def init_config(app_config: AppConfig):
    # Connect to shared store database
    if app_config.share.store:
        db, migrator = connect_sqlite(str(SHARED_STORE_PATH))
        bind_tables([PcsFileTable, PcsSharedLinkInfoTable], db)
        create_tables([PcsFileTable, PcsSharedLinkInfoTable], db)

        modify_table(PcsFileTable, db, migrator)
        modify_table(PcsSharedLinkInfoTable, db, migrator)

    if not CONFIG_PATH.exists():
        save_config(app_config)


def save_config(app_config: AppConfig):
    app_config.dump(str(CONFIG_PATH))
