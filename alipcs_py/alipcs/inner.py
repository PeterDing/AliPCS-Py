from typing import Optional, List, Dict, Any, TYPE_CHECKING
from dataclasses import dataclass
from collections import namedtuple
import time
import re

from alipcs_py.common.date import iso_8601_to_timestamp

if TYPE_CHECKING:
    from alipcs_py.alipcs.api import AliPCSApi


@dataclass
class PcsRapidUploadInfo:
    """Rapid Upload Info"""

    content_hash: str
    content_length: int
    content_hash_name: str = "sha1"
    crc64_hash: Optional[str] = None
    name: Optional[str] = None

    def _filename(self) -> str:
        return (self.name or "").replace(" ", "%20")

    def ali(self) -> str:
        """ali://<filename>|<content_length>|<content_hash>"""

        filename = self._filename()
        return f"ali://{filename}|{self.content_length}|{self.content_hash}"

    def short(self) -> str:
        """<filename>|<content_length>|<content_hash>"""

        filename = self._filename()
        return f"{filename}|{self.content_length}|{self.content_hash}"

    def all_links(self) -> List[str]:
        return [self.ali(), self.short()]

    @staticmethod
    def hash_link_protocols() -> List[str]:
        return ["ali", "short"]

    @staticmethod
    def default_hash_link_protocol() -> str:
        return "ali"


@dataclass
class PcsFile:
    """
    A Ali PCS file

    path: str  # remote absolute path
    is_dir: Optional[bool] = None
    is_file: Optional[bool] = None
    fs_id: Optional[int] = None  # file id
    size: Optional[int] = None
    md5: Optional[str] = None
    block_list: Optional[List[str]] = None  # block md5 list
    category: Optional[int] = None
    user_id: Optional[int] = None
    created_at: Optional[int] = None  # server created time
    updated_at: Optional[int] = None  # server updated time
    shared: Optional[bool] = None  # this file is shared if True
    """

    file_id: str
    name: str
    parent_file_id: str
    type: str
    is_dir: bool
    is_file: bool
    size: int = 0
    path: str = ""  # remote absolute path

    created_at: Optional[int] = None  # server created time
    updated_at: Optional[int] = None  # server updated time (updated time)

    file_extension: Optional[str] = None
    content_type: Optional[str] = None
    mime_type: Optional[str] = None
    mime_extension: Optional[str] = None
    labels: Optional[List[str]] = None

    status: Optional[str] = None
    hidden: Optional[bool] = None
    starred: Optional[bool] = None
    category: Optional[str] = None
    punish_flag: Optional[int] = None
    encrypt_mode: Optional[str] = None

    drive_id: Optional[str] = None
    domain_id: Optional[str] = None
    upload_id: Optional[str] = None
    async_task_id: Optional[str] = None

    rapid_upload_info: Optional[PcsRapidUploadInfo] = None
    download_url: Optional[str] = None

    @staticmethod
    def from_(info) -> "PcsFile":
        created_at = None
        if "created_at" in info:
            created_at = iso_8601_to_timestamp(info["created_at"])
        updated_at = None
        if "updated_at" in info:
            updated_at = iso_8601_to_timestamp(info["updated_at"])

        rapid_upload_info: Optional[PcsRapidUploadInfo] = None
        if "content_hash" in info:
            rapid_upload_info = PcsRapidUploadInfo(
                content_hash=info.get("content_hash"),
                content_length=info.get("size"),
                content_hash_name=info.get("content_hash_name"),
                crc64_hash=info.get("crc64_hash"),
                name=info.get("name"),
            )

        return PcsFile(
            file_id=info.get("file_id", ""),
            name=info.get("name", ""),
            parent_file_id=info.get("parent_file_id", ""),
            type=info.get("type", ""),
            is_dir=info.get("type") == "folder",
            is_file=info.get("type") == "file",
            size=info.get("size"),
            created_at=created_at,
            updated_at=updated_at,
            file_extension=info.get("file_extension"),
            content_type=info.get("content_type"),
            mime_type=info.get("mime_type"),
            mime_extension=info.get("mime_extension"),
            labels=info.get("labels"),
            status=info.get("status"),
            hidden=info.get("hidden"),
            starred=info.get("starred"),
            category=info.get("category"),
            punish_flag=info.get("punish_flag"),
            encrypt_mode=info.get("encrypt_mode"),
            drive_id=info.get("drive_id"),
            domain_id=info.get("domain_id"),
            upload_id=info.get("upload_id"),
            async_task_id=info.get("async_task_id"),
            rapid_upload_info=rapid_upload_info,
            download_url=info.get("download_url"),
        )

    @staticmethod
    def root() -> "PcsFile":
        return PcsFile(
            file_id="root",
            name="",
            parent_file_id="root",
            type="folder",
            is_dir=True,
            is_file=False,
            size=0,
            path="/",
        )

    def is_root(self) -> bool:
        return self.file_id == "root"

    def download_url_expires(self) -> bool:
        """Check whether the `self.download_url` expires"""

        if self.download_url:
            mod = re.search(r"oss-expires=(\d+)", self.download_url)
            if mod:
                expire_time = float(mod.group(1))
                if time.time() < expire_time - 5:
                    return False
        return True

    def update_download_url(self, api: "AliPCSApi"):
        """Update the download url if it expires"""

        if self.download_url_expires():
            pcs_url = api.download_link(self.file_id)
            if not pcs_url:
                return
            self.download_url = pcs_url.url


@dataclass
class PcsUploadUrl:
    upload_url: Optional[str] = None
    internal_upload_url: Optional[str] = None
    content_type: Optional[str] = None
    part_number: Optional[int] = None

    @staticmethod
    def from_(info) -> "PcsUploadUrl":
        return PcsUploadUrl(
            upload_url=info.get("upload_url"),
            internal_upload_url=info.get("internal_upload_url"),
            content_type=info.get("content_type"),
            part_number=info.get("part_number"),
        )


@dataclass
class PcsPreparedFile:
    file_id: Optional[str] = None
    parent_file_id: Optional[str] = None
    file_name: Optional[str] = None
    type: Optional[str] = None
    part_info_list: Optional[List[PcsUploadUrl]] = None

    rapid_upload: Optional[bool] = None
    pre_hash: Optional[str] = None

    domain_id: Optional[str] = None
    drive_id: Optional[str] = None
    upload_id: Optional[str] = None

    encrypt_mode: Optional[str] = None
    location: Optional[str] = None

    @staticmethod
    def from_(info) -> "PcsPreparedFile":
        part_info_list = [
            PcsUploadUrl.from_(i) for i in info.get("part_info_list") or []
        ]
        return PcsPreparedFile(
            file_id=info.get("file_id"),
            parent_file_id=info.get("parent_file_id"),
            file_name=info.get("file_name"),
            type=info.get("type"),
            part_info_list=part_info_list,
            rapid_upload=info.get("rapid_upload"),
            pre_hash=info.get("pre_hash"),
            domain_id=info.get("domain_id"),
            drive_id=info.get("drive_id"),
            upload_id=info.get("upload_id"),
            encrypt_mode=info.get("encrypt_mode"),
            location=info.get("location"),
        )

    def can_rapid_upload(self) -> bool:
        return bool(self.pre_hash)

    def is_rapid_upload(self) -> bool:
        return bool(self.rapid_upload)

    def upload_urls(self, internal: bool = False) -> List[str]:
        if internal:
            return [
                p.internal_upload_url
                for p in self.part_info_list or []
                if p.internal_upload_url
            ]
        else:
            return [p.upload_url for p in self.part_info_list or [] if p.upload_url]


@dataclass
class PcsSharedLink:
    """The shared link by the signin user"""

    share_url: str
    file_id: str
    file_id_list: List[str]
    share_id: str
    share_name: Optional[str] = None
    share_policy: Optional[str] = None
    share_pwd: Optional[str] = None
    drive_id: Optional[str] = None

    # The expiration timestramp
    expiration: Optional[int] = None
    expired: Optional[bool] = None

    # created_at
    created_at: Optional[int] = None
    # updated_at
    updated_at: Optional[int] = None

    status: Optional[str] = None
    preview_count: Optional[int] = None
    save_count: Optional[int] = None
    download_count: Optional[int] = None

    description: Optional[str] = None
    creator: Optional[str] = None

    @staticmethod
    def from_(info) -> "PcsSharedLink":
        created_at = None
        if "created_at" in info:
            created_at = iso_8601_to_timestamp(info["created_at"])
        updated_at = None
        if "updated_at" in info:
            updated_at = iso_8601_to_timestamp(info["updated_at"])
        expiration = None
        if info.get("expiration"):
            expiration = iso_8601_to_timestamp(info["expiration"])

        return PcsSharedLink(
            share_url=info["share_url"],
            file_id=info["file_id"],
            file_id_list=info["file_id_list"],
            share_id=info["share_id"],
            share_name=info.get("share_name"),
            share_policy=info.get("share_policy"),
            share_pwd=info.get("share_pwd"),
            drive_id=info.get("drive_id"),
            expiration=expiration,
            expired=info.get("expired"),
            created_at=created_at,
            updated_at=updated_at,
            status=info.get("status"),
            preview_count=info.get("preview_count"),
            save_count=info.get("save_count"),
            download_count=info.get("download_count"),
            description=info.get("description"),
            creator=info.get("creator"),
        )

    def has_password(self) -> bool:
        return bool(self.share_pwd)


@dataclass
class PcsSharedLinkInfo:
    """The shared link by anyone"""

    share_id: str
    share_pwd: Optional[str] = None
    share_name: Optional[str] = None
    display_name: Optional[str] = None
    file_count: Optional[str] = None
    file_infos: Optional[str] = None
    expiration: Optional[int] = None
    updated_at: Optional[int] = None
    vip: Optional[str] = None
    avatar: Optional[str] = None
    is_following_creator: Optional[str] = None
    creator_id: Optional[str] = None
    creator_name: Optional[str] = None
    creator_phone: Optional[str] = None

    @staticmethod
    def from_(info) -> "PcsSharedLinkInfo":
        updated_at = None
        if "updated_at" in info:
            updated_at = iso_8601_to_timestamp(info["updated_at"])
        expiration = None
        if info.get("expiration"):
            expiration = iso_8601_to_timestamp(info["expiration"])

        return PcsSharedLinkInfo(
            share_id=info.get("share_id"),
            share_pwd=info.get("share_pwd"),
            share_name=info.get("share_name"),
            display_name=info.get("display_name"),
            file_count=info.get("file_count"),
            file_infos=info.get("file_infos"),
            expiration=expiration,
            updated_at=updated_at,
            vip=info.get("vip"),
            avatar=info.get("avatar"),
            is_following_creator=info.get("is_following_creator"),
            creator_id=info.get("creator_id"),
            creator_name=info.get("creator_name"),
            creator_phone=info.get("creator_phone"),
        )

    @property
    def share_url(self) -> str:
        return "https://www.aliyundrive.com/s/" + self.share_id


@dataclass
class SharedAuth:
    """The shared link auth by anyone"""

    share_id: str
    share_password: str
    share_token: str
    expire_time: float
    expires_in: int
    info: Any

    @staticmethod
    def from_(share_id: str, share_password: str, info: Any) -> "SharedAuth":
        return SharedAuth(
            share_id,
            share_password,
            share_token=info["share_token"],
            expire_time=iso_8601_to_timestamp(info["expire_time"]),
            expires_in=info["expires_in"],
            info=info,
        )

    def is_expired(self) -> bool:
        return time.time() >= self.expire_time


FromTo = namedtuple("FromTo", ["from_", "to_"])


@dataclass
class PcsSpace:
    used_size: int
    total_size: int

    @staticmethod
    def from_(info) -> "PcsSpace":
        return PcsSpace(
            used_size=info.get("used_size"),
            total_size=info.get("total_size"),
        )


@dataclass
class PcsAuth:
    refresh_token: str
    access_token: Optional[str] = None


@dataclass
class PcsUserPrivilege:
    feature_id: Optional[str] = None
    feature_attr_id: Optional[str] = None
    quota: Optional[int] = None

    @staticmethod
    def from_(info) -> "PcsUserPrivilege":
        return PcsUserPrivilege(
            feature_id=info.get("feature_id"),
            feature_attr_id=info.get("feature_attr_id"),
            quota=info.get("quota"),
        )


@dataclass
class PcsUserRights:
    spu_id: Optional[str] = None
    name: Optional[str] = None
    is_expires: Optional[str] = None
    privileges: Optional[List[PcsUserPrivilege]] = None

    @staticmethod
    def from_(info) -> "PcsUserRights":
        privileges = [PcsUserPrivilege.from_(i) for i in info.get("privileges") or []]
        return PcsUserRights(
            spu_id=info.get("spu_id"),
            name=info.get("name"),
            is_expires=info.get("is_expires"),
            privileges=privileges,
        )


@dataclass
class PcsUserVip:
    identity: str
    icon: str
    status: str
    vip_list: List[Dict]

    @staticmethod
    def from_(info) -> "PcsUserVip":
        return PcsUserVip(
            identity=info.get("identity"),
            icon=info.get("icon"),
            status=info.get("status"),
            vip_list=info.get("vipList"),
        )

    def is_vip(self) -> bool:
        return self.identity == "vip"


@dataclass
class PcsUser:
    user_id: str
    default_drive_id: Optional[str] = None
    device_id: Optional[str] = None
    domain_id: Optional[str] = None

    refresh_token: Optional[str] = None
    access_token: Optional[str] = None
    token_type: Optional[str] = None
    expire_time: Optional[int] = None

    user_name: Optional[str] = None
    nick_name: Optional[str] = None
    user_data: Optional[Dict[Any, Any]] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    avatar: Optional[str] = None
    personal_space_info: Optional[PcsSpace] = None
    personal_rights_info: Optional[PcsUserRights] = None
    user_vip_info: Optional[PcsUserVip] = None

    created_at: Optional[int] = None
    updated_at: Optional[int] = None

    role: Optional[str] = None
    status: Optional[str] = None
    description: Optional[str] = None

    deny_change_password_by_self: Optional[str] = None
    need_change_password_next_login: Optional[str] = None

    @staticmethod
    def from_(info) -> "PcsUser":
        created_at = None
        if "created_at" in info:
            created_at = info["created_at"] // 1000
        updated_at = None
        if "updated_at" in info:
            updated_at = info["updated_at"] // 1000

        return PcsUser(
            user_id=info["user_id"],
            default_drive_id=info.get("default_drive_id"),
            device_id=info.get("device_id"),
            domain_id=info.get("domain_id"),
            user_name=info.get("user_name"),
            nick_name=info.get("nick_name"),
            user_data=info.get("user_data"),
            phone=info.get("phone"),
            email=info.get("email"),
            avatar=info.get("avatar"),
            personal_space_info=PcsSpace.from_(info.get("personal_space_info") or {}),
            personal_rights_info=PcsUserRights.from_(
                info.get("personal_rights_info") or {}
            ),
            user_vip_info=PcsUserVip.from_(info.get("user_vip_info") or {}),
            created_at=created_at,
            updated_at=updated_at,
            role=info.get("role"),
            status=info.get("status"),
            description=info.get("description"),
            deny_change_password_by_self=info.get("deny_change_password_by_self"),
            need_change_password_next_login=info.get("need_change_password_next_login"),
        )


@dataclass
class PcsRateLimit:
    part_speed: Optional[int] = None
    part_size: Optional[int] = None

    @staticmethod
    def from_(info) -> "PcsRateLimit":
        return PcsRateLimit(
            part_speed=info.get("part_speed"),
            part_size=info.get("part_size"),
        )


@dataclass
class PcsDownloadUrl:
    url: Optional[str] = None
    internal_url: Optional[str] = None
    cdn_url: Optional[str] = None
    size: Optional[int] = None

    method: Optional[str] = None
    expiration: Optional[int] = None
    ratelimit: Optional[PcsRateLimit] = None

    @staticmethod
    def from_(info) -> "PcsDownloadUrl":
        expiration = None
        if info.get("expiration"):
            expiration = iso_8601_to_timestamp(info["expiration"])

        return PcsDownloadUrl(
            url=info.get("url"),
            internal_url=info.get("internal_url"),
            cdn_url=info.get("cdn_url"),
            size=info.get("size"),
            method=info.get("method"),
            expiration=expiration,
            ratelimit=PcsRateLimit.from_(info.get("ratelimit") or {}),
        )
