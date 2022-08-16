import typing
from typing import Tuple, List, Dict, Any
from peewee import (
    Model,
    CharField,
    IntegerField,
    Database,
    SqliteDatabase,
    TextField,
    SmallIntegerField,
    ForeignKeyField,
)
from playhouse.migrate import SchemaMigrator, SqliteMigrator, migrate

from alipcs_py.alipcs.inner import PcsFile, PcsSharedLinkInfo
from alipcs_py.common.util import json_dump_values


class Deserializer:
    @classmethod
    def pcs_item(cls) -> Any:
        raise NotImplementedError()

    def to_pcs(self) -> PcsSharedLinkInfo:
        pcs_item = self.pcs_item()
        data: Dict[str, Any] = {}
        columns = self.__class__._meta.columns  # type: ignore
        pcs_fields = pcs_item.__dataclass_fields__
        for col in columns.keys():
            if col in pcs_fields:
                val = getattr(self, col)
                if (
                    pcs_fields[col].type == typing.Optional[bool]
                    or pcs_fields[col].type == bool
                ):
                    if val == None:
                        data[col] = None
                    else:
                        data[col] = bool(val)
                else:
                    data[col] = val
        return pcs_item(**data)


class PcsSharedLinkInfoTable(Deserializer, Model):
    share_id = CharField(null=False, unique=True)
    share_pwd = CharField(null=True)
    share_name = CharField(null=True)
    display_name = CharField(null=True)
    file_count = IntegerField(null=True)
    file_infos = TextField(null=True)  # json
    expiration = IntegerField(null=True)
    updated_at = IntegerField(null=True)
    vip = CharField(null=True)
    avatar = CharField(null=True)
    is_following_creator = SmallIntegerField(null=True)
    creator_id = CharField(null=True)
    creator_name = CharField(null=True)
    creator_phone = CharField(null=True)

    class Meta:
        indexes = (
            # create a non-unique
            (("share_id",), False),
            (("share_name",), False),
            (("display_name",), False),
        )

    @classmethod
    def get_or_create(cls, **kwargs):
        kwargs = json_dump_values(kwargs)
        return super().get_or_create(**kwargs)

    @classmethod
    def pcs_item(cls) -> Any:
        return PcsSharedLinkInfo


class PcsFileTable(Deserializer, Model):
    file_id = CharField(null=False, unique=True)
    name = CharField(null=False)
    parent_file_id = CharField(null=False)
    type = CharField(null=False)
    is_dir = SmallIntegerField(null=False)
    is_file = SmallIntegerField(null=False)
    size = IntegerField(null=True)
    path = CharField(null=True)

    created_at = IntegerField(null=True)
    updated_at = IntegerField(null=True)

    file_extension = CharField(null=True)
    content_type = CharField(null=True)
    mime_type = CharField(null=True)
    mime_extension = CharField(null=True)
    labels = CharField(null=True)

    status = CharField(null=True)
    hidden = IntegerField(null=True)
    starred = IntegerField(null=True)
    category = CharField(null=True)
    punish_flag = IntegerField(null=True)
    encrypt_mode = CharField(null=True)

    drive_id = CharField(null=True)
    domain_id = CharField(null=True)
    upload_id = CharField(null=True)
    async_task_id = CharField(null=True)

    rapid_upload_info = CharField(null=True)
    download_url = CharField(null=True)

    shared_link_info_id = ForeignKeyField(PcsSharedLinkInfoTable)

    class Meta:
        indexes = (
            # create a non-unique
            (("file_id",), False),
            (("name",), False),
            (("is_dir",), False),
            (("is_file",), False),
            (("path",), False),
            (("file_extension",), False),
        )

    @classmethod
    def get_or_create(cls, **kwargs):
        kwargs = json_dump_values(kwargs)
        return super().get_or_create(**kwargs)

    @classmethod
    def pcs_item(cls) -> Any:
        return PcsFile


def connect_sqlite(path: str) -> Tuple[Database, SchemaMigrator]:
    db = SqliteDatabase(path)
    return db, SqliteMigrator(db)


def bind_tables(tables: List[type], db: Database):
    db.bind(tables)


def create_tables(tables: List[type], db: Database):
    db.create_tables(tables)


def get_db_field(tp: type):
    if tp == str:
        return CharField(null=True)
    elif tp == typing.Optional[str]:
        return CharField(null=True)

    elif tp == typing.List[str]:
        return TextField(null=True)

    elif tp == int:
        return IntegerField(null=True)
    elif tp == typing.Optional[int]:
        return IntegerField(null=True)

    elif tp == bool:
        return IntegerField(null=True)
    elif tp == typing.Optional[int]:
        return IntegerField(null=True)

    else:
        raise ValueError("Unsupported type: %s", tp)


def modify_table(table: Any, db: Database, migrator: SchemaMigrator):
    table_name = table._meta.name  # type: ignore
    pcs_fields = table.pcs_item().__dataclass_fields__
    columns = set([c.name for c in db.get_columns(table_name)])

    for name, field in pcs_fields.items():
        if name not in columns:
            db_field = get_db_field(field.type)
            migrate(migrator.add_column(table_name, name, db_field))
