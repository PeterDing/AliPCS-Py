from typing import Optional, Tuple, List, Any

from peewee import SQL

from alipcs_py.alipcs.api import AliPCSApiMix
from alipcs_py.alipcs.inner import PcsSharedLinkInfo, PcsFile
from alipcs_py.storage.tables import PcsSharedLinkInfoTable, PcsFileTable


class SharedStore:
    def __init__(self) -> None:
        pass

    def get_shared_link_info(self, share_id: str) -> Optional[PcsSharedLinkInfo]:
        ins = PcsSharedLinkInfoTable.get_or_none(share_id=share_id)
        if ins:
            return ins.to_pcs()
        else:
            return None

    def add_shared_link_info(self, pcs_shared_link_info: PcsSharedLinkInfo) -> Any:
        pcs_shared_link_info_ins, _ = PcsSharedLinkInfoTable.get_or_create(
            **{k: getattr(pcs_shared_link_info, k) for k in pcs_shared_link_info.__dataclass_fields__}
        )
        return pcs_shared_link_info_ins

    def add_shared_file(self, share_id: str, pcs_file: PcsFile) -> Any:
        if PcsFileTable.get_or_none(file_id=pcs_file.file_id):
            # The file exists
            return

        pcs_shared_link_info_ins = PcsSharedLinkInfoTable.get_or_none(share_id=share_id)

        pcs_file_ins, _ = PcsFileTable.get_or_create(
            **{k: getattr(pcs_file, k) for k in pcs_file.__dataclass_fields__},
            shared_link_info_id=pcs_shared_link_info_ins.id,
        )
        return pcs_file_ins

    def delete_shared_links(self, *share_ids: str) -> None:
        PcsSharedLinkInfoTable.delete().where(PcsSharedLinkInfoTable.share_id.in_(share_ids)).execute()

    def delete_shared_files(self, *file_ids: str) -> None:
        PcsFileTable.delete().where(PcsFileTable.file_id.in_(file_ids)).execute()

    def search_shared_links(
        self, *keywords: str, fields: List[str] = ["share_name", "display_name"]
    ) -> List[PcsSharedLinkInfo]:
        sql = " OR ".join([f"`{f}` like ?" for f in fields * len(keywords)])
        query = PcsSharedLinkInfoTable.select().where(SQL(sql, [f"%{keyword}%" for keyword in keywords] * len(fields)))
        return [item.to_pcs() for item in query]

    def search_shared_files(
        self,
        *keywords: str,
        fields: List[str] = ["name", "path"],
        share_ids: List[str] = [],
    ) -> List[Tuple[PcsFile, PcsSharedLinkInfo]]:
        sql = " OR ".join([f"`{f}` like ?" for f in fields * len(keywords)])
        query = PcsFileTable.select().join(
            PcsSharedLinkInfoTable,
            on=(PcsFileTable.shared_link_info_id == PcsSharedLinkInfoTable.id),
        )
        if share_ids:
            query = query.where(
                PcsSharedLinkInfoTable.share_id.in_(share_ids)
                & SQL(sql, [f"%{keyword}%" for keyword in keywords] * len(fields))
            )
        else:
            query = query.where(SQL(sql, [f"%{keyword}%" for keyword in keywords] * len(fields)))
        return [(item.to_pcs(), item.shared_link_info_id.to_pcs()) for item in query]

    def list_shared_links(
        self,
        by_id: bool = False,
        by_name: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> List[PcsSharedLinkInfo]:
        query = PcsSharedLinkInfoTable.select()
        if by_name:
            query = query.order_by(PcsSharedLinkInfoTable.share_name)
        else:
            # Default by id
            query = query.order_by(PcsSharedLinkInfoTable.id)

        query = query.limit(limit).offset(offset)

        return [item.to_pcs() for item in query]

    def list_shared_files(
        self,
        share_ids: List[str] = [],
        by_id: bool = False,
        by_name: bool = False,
        by_path: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Tuple[PcsFile, PcsSharedLinkInfo]]:
        query = PcsFileTable.select().join(
            PcsSharedLinkInfoTable,
            on=(PcsFileTable.shared_link_info_id == PcsSharedLinkInfoTable.id),
        )
        if share_ids:
            query = query.where(PcsSharedLinkInfoTable.share_id.in_(share_ids))
        if by_name:
            query = query.order_by(PcsFileTable.name)
        elif by_path:
            query = query.order_by(PcsFileTable.path)
        else:
            # Default by id
            query = query.order_by(PcsFileTable.id)

        # If limit is 0, selecting all items
        if limit > 0:
            query = query.limit(limit).offset(offset)

        return [(item.to_pcs(), item.shared_link_info_id.to_pcs()) for item in query]


class AliPCSApiMixWithSharedStore(AliPCSApiMix):
    """AliPCS API Mix with SharedStore

    Hooking the `AliPCSApiMix.list` to store the shared file infos
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._sharedstore: Optional[SharedStore] = None

    @property
    def sharedstore(self) -> Optional[SharedStore]:
        return self._sharedstore

    def get_share_token(self, share_id: str, share_password: str = "") -> str:
        token = super().get_share_token(share_id, share_password=share_password)
        if not self._sharedstore:
            return token

        if token:
            shared_link_info = self._sharedstore.get_shared_link_info(share_id)
            if not shared_link_info:
                shared_link_info = self.shared_info(share_id)
                self._sharedstore.add_shared_link_info(shared_link_info)

        return token

    def meta(self, *args, **kwargs):
        share_id = kwargs.get("share_id")

        pcs_files = super().meta(*args, **kwargs)
        if not self._sharedstore:
            return pcs_files

        if share_id:
            for pcs_file in pcs_files:
                self._sharedstore.add_shared_file(share_id, pcs_file)

        return pcs_files

    def list(self, *args, **kwargs):
        share_id = kwargs.get("share_id")

        pcs_files, next_marker = super().list(*args, **kwargs)
        if not self._sharedstore:
            return pcs_files, next_marker

        if share_id:
            for pcs_file in pcs_files:
                self._sharedstore.add_shared_file(share_id, pcs_file)

        return pcs_files, next_marker
