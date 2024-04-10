from typing import Optional, Dict, List, Union, Any, Callable, IO
from enum import Enum
import threading
from urllib.parse import urljoin
from typing_extensions import Literal

import requests  # type: ignore
from requests_toolbelt import MultipartEncoder, MultipartEncoderMonitor

from alipcs_py.common.date import now_timestamp, iso_8601_to_timestamp, timestamp_to_iso_8601
from alipcs_py.common import constant
from alipcs_py.common.io import RangeRequestIO, DEFAULT_MAX_CHUNK_SIZE, ChunkIO, total_len
from alipcs_py.common.cache import timeout_cache
from alipcs_py.common.crypto import generate_secp256k1_keys
from alipcs_py.alipcs.errors import AliPCSError, make_alipcs_error, handle_error
from alipcs_py.alipcs.inner import SharedAuth
from alipcs_py.common.net import make_http_session


UPLOAD_CHUNK_SIZE = 10 * constant.OneM

APP_ID = "5dde4e1bdf9e4966b387ba58f4b3fdc3"

ALIYUNDRIVE_COM = "https://www.aliyundrive.com"
ALIYUNDRIVE_COM_API = "https://api.aliyundrive.com"
ALIYUNDRIVE_OPENAPI_DOMAIN = "https://openapi.aliyundrive.com"

PCS_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
PCS_HEADERS = {"Origin": ALIYUNDRIVE_COM, "Referer": ALIYUNDRIVE_COM + "/", "User-Agent": PCS_UA}

CheckNameMode = Literal[
    "overwrite",  # 直接覆盖，以后多版本有用
    "auto_rename",  # 自动换一个随机名称
    "refuse",  # 不会创建，告诉你已经存在
    "ignore",  # 会创建重名的
]


class Method(Enum):
    Head = "HEAD"
    Get = "GET"
    Post = "POST"


class PcsNode(Enum):
    f"""Aliyundrive PCS Nodes which use {ALIYUNDRIVE_COM_API}"""

    Token = "v2/account/token"
    Refresh = "token/refresh"
    CreateSession = "users/v1/users/device/create_session"

    FileList = "adrive/v3/file/list"
    Walk = "v2/file/walk"
    GetPath = "adrive/v1/file/get_path"
    MetaByPath = "v2/file/get_by_path"
    Meta = "v2/file/get"
    Search = "adrive/v3/file/search"
    DownloadUrl = "v2/file/get_download_url"

    CreateFile = "v2/file/create"
    UploadUrl = "v2/file/get_upload_url"
    UploadComplete = "v2/file/complete"

    Batch = "v3/batch"

    CreateWithFolders = "adrive/v2/file/createWithFolders"
    Move = "adrive/v3/file/move"
    Update = "v3/file/update"
    Trash = "v2/recyclebin/trash"

    Available = "adrive/v2/share_link/check_avaliable"
    Share = "adrive/v2/share_link/create"
    ShareToken = "v2/share_link/get_share_token"
    SharedInfo = "adrive/v3/share_link/get_share_by_anonymous"
    SharedList = "adrive/v3/share_link/list"
    SharedFileDownloadUrl = "v2/file/get_share_link_download_url"

    PersonalInfo = "v2/databox/get_personal_info"
    User = "v2/user/get"
    UserVip = "business/v1.0/users/vip/info"

    def url(self) -> str:
        return f"{ALIYUNDRIVE_COM_API}/{self.value}"


# The Lock is used to refresh token
_LOCK = threading.Lock()


class AliPCS:
    """Alipan Drive Personal Cloud Service Raw API

    The core class is used to interact with Alipan Drive Personal Cloud Service.
    It provides the basic operations of the service and handles the raw requests and responses.

    An `AliPCSError` error will be raised if the code of the response occurs.

    Args:
        refresh_token (str): The refresh token of the user.
        access_token (str, optional): The access token of the user.
        token_type (str): The token type. Default is "Bearer".
        expire_time (int, optional): The expire time of the token.
        user_id (str, optional): The user id of the user.
        user_name (str, optional): The user name of the user.
        nick_name (str, optional): The nick name of the user.
        device_id (str, optional): The device id of the user.
        default_drive_id (str, optional): The default drive id of the user.
        role (str, optional): The role of the user.
        status (str, optional): The status of the user.
        error_max_retries (int): The max retries when a client request error occurs. Default is 2.
        max_keepalive_connections (int): The max keepalive connections. Default is 50.
        max_connections (int): The max number of connections in the pool. Default is 50.
        keepalive_expiry (float): The keepalive expiry. Default is 10 * 60 seconds.
        connection_max_retries (int): The max retries when a connection error occurs. Default is 2.
    """

    SHARE_AUTHS: Dict[str, SharedAuth] = {}

    def __init__(
        self,
        refresh_token: str,
        access_token: str = "",
        token_type: str = "Bearer",
        expire_time: int = 0,
        user_id: str = "",
        user_name: str = "",
        nick_name: str = "",
        device_id: str = "",
        default_drive_id: str = "",
        role: str = "",
        status: str = "",
        error_max_retries: int = 2,
        max_keepalive_connections: int = 50,
        max_connections: int = 50,
        keepalive_expiry: float = 10 * 60,
        connection_max_retries: int = 2,
    ):
        self._session = make_http_session(
            max_keepalive_connections=max_keepalive_connections,
            max_connections=max_connections,
            keepalive_expiry=keepalive_expiry,
            max_retries=connection_max_retries,
        )

        self._error_max_retries = error_max_retries

        self._refresh_token = refresh_token
        self._access_token = access_token
        self._token_type = token_type
        self._expire_time = expire_time

        self._user_id = user_id
        self._user_name = user_name
        self._nick_name = nick_name
        self._device_id = device_id
        self._signature = ""
        self._default_drive_id = default_drive_id
        self._role = role
        self._status = status
        self._nonce = 0

    def __str__(self) -> str:
        return f"""AliPCS

    user_id: {self.user_id}
    user_name: {self.user_name}
    nick_name: {self.nick_name}

    refresh_token: {self.refresh_token}
    access_token: {self.access_token}
    token_type: {self.token_type}
    expire_time: {self.expire_time}

    device_id: {self.device_id}
    signature: {self.signature}
    default_drive_id: {self.default_drive_id}
    role: {self.role}
    status: {self.status}
    nonce: {self._nonce}
    """

    def get_token(self):
        """Get authorization token"""

        url = PcsNode.Token.url()
        data = dict(refresh_token=self.refresh_token, grant_type="refresh_token")
        resp = self._request(Method.Post, url, json=data)
        return resp.json()

    @property
    def refresh_token(self) -> str:
        with _LOCK:
            if not self._access_token or (now_timestamp() - 1 * 60 * 60) >= self._expire_time or not self._device_id:
                self.refresh()
            return self._refresh_token

    @property
    def access_token(self) -> str:
        self.refresh_token
        return self._access_token

    @property
    def user_id(self) -> str:
        self.refresh_token
        return self._user_id

    @property
    def user_name(self) -> str:
        self.refresh_token
        return self._user_name

    @property
    def nick_name(self) -> str:
        self.refresh_token
        return self._nick_name

    @property
    def token_type(self) -> str:
        self.refresh_token
        return self._token_type

    @property
    def expire_time(self) -> int:
        self.refresh_token
        return self._expire_time

    @property
    def device_id(self) -> str:
        self.refresh_token
        return self._device_id

    @property
    def signature(self) -> str:
        if not self._signature:
            self.create_session()
        return self._signature

    @property
    def default_drive_id(self) -> str:
        self.refresh_token
        return self._default_drive_id

    @property
    def role(self) -> str:
        self.refresh_token
        return self._role

    @property
    def status(self) -> str:
        self.refresh_token
        return self._status

    def _request(
        self,
        method: Method,
        url: str,
        params: Optional[Dict[str, str]] = None,
        headers: Optional[Dict[str, str]] = None,
        data: Union[str, bytes, Dict[str, str], Any] = None,
        json: Any = None,
        files: Optional[Dict[str, Any]] = None,
        refresh: bool = False,
        **kwargs,
    ) -> requests.Response:
        if not headers:
            headers = dict(PCS_HEADERS)

        if not refresh and "Authorization" not in headers:
            headers["Authorization"] = f"{self.token_type} {self.access_token}"
            headers["x-device-id"] = self.device_id

        if json is not None:
            headers["Content-Type"] = "application/json;charset=UTF-8"

        if isinstance(data, (MultipartEncoder, MultipartEncoderMonitor)):
            headers["Content-Type"] = data.content_type

        headers["x-canary"] = "client=web,app=adrive,version=v4.1.0"

        try:
            resp = self._session.request(
                method.value, url, params=params, headers=headers, data=data, json=json, files=files, **kwargs
            )
            return resp
        except Exception as err:
            raise AliPCSError("AliPCS._request") from err

    def refresh(self):
        """Refresh token"""

        url = PcsNode.Refresh.url()
        data = dict(refresh_token=self._refresh_token)
        resp = self._request(Method.Post, url, json=data, refresh=True)
        info = resp.json()

        if "code" in info:
            err = make_alipcs_error(info["code"], info=info)
            raise err

        self._user_id = info["user_id"]
        self._user_name = info["user_name"]
        self._nick_name = info["nick_name"]

        self._refresh_token = info["refresh_token"]
        self._access_token = info["access_token"]
        self._token_type = info["token_type"]
        self._expire_time = iso_8601_to_timestamp(info["expire_time"])

        self._device_id = info["device_id"]
        self._default_drive_id = info["default_drive_id"]

        self._role = info["role"]
        self._status = info["status"]

        return info

    def create_session(self):
        """Create session"""

        assert self.device_id, "No AliPCS._device_id"
        assert self.user_id, "No AliPCS._user_id"

        device_id = self.device_id
        user_id = self.user_id
        nonce = self._nonce

        private_key, public_key = generate_secp256k1_keys()
        message = f"{APP_ID}:{device_id}:{user_id}:{nonce}".encode()
        signature: str = private_key.sign(message).hex()
        sign = signature + "00"

        url = PcsNode.CreateSession.url()
        data = dict(deviceName="Chrome浏览器", modelName="Mac OS网页版", pubKey=public_key.to_string().hex())

        headers = dict(**PCS_HEADERS)
        headers.update({"x-device-id": device_id, "x-signature": sign})

        @handle_error
        def do(self):
            resp = self._request(Method.Post, url, headers=headers, json=data)
            return resp.json()

        info = do(self)
        if info["result"] and info["success"]:
            self._signature = sign

        return info

    @handle_error
    def path_traceback(self, file_id: str, share_id: Optional[str] = None):
        """Traceback the path of the file by its file_id

        Return the list of all parent directories' info from the file to the top level directory.
        """

        url = PcsNode.GetPath.url()

        headers = dict(PCS_HEADERS)
        headers.update({"x-device-id": self.device_id, "x-signature": self.signature})

        if share_id:
            share_token = self.share_token(share_id)
            assert share_token, "Need share_token"

            headers["x-share-token"] = share_token

        data = dict(file_id=file_id, fields="*")

        if share_id:
            data["share_id"] = share_id
        else:
            data["drive_id"] = self.default_drive_id

        resp = self._request(Method.Post, url, headers=headers, json=data)
        info = resp.json()
        return info

    @handle_error
    def meta_by_path(self, remotepath: str):
        """Get meta info of the file by its path

        Can not get the shared files' meta info.
        """

        assert remotepath.startswith("/"), "Path should start with '/'"

        url = PcsNode.MetaByPath.url()
        headers = dict(PCS_HEADERS)
        headers.update({"x-device-id": self.device_id, "x-signature": self.signature})

        data = dict(
            file_path=remotepath,
            fields="*",
            drive_id=self.default_drive_id,
            image_thumbnail_process="image/resize,w_400/format,jpeg",
            image_url_process="image/resize,w_375/format,jpeg",
            video_thumbnail_process="video/snapshot,t_1000,f_jpg,ar_auto,w_375",
        )

        resp = self._request(Method.Post, url, headers=headers, json=data)
        info = resp.json()
        return info

    @handle_error
    def meta(self, file_id: str, share_id: Optional[str] = None):
        """Get meta info of the file by its file_id"""

        url = PcsNode.Meta.url()
        headers = dict(PCS_HEADERS)
        headers.update({"x-device-id": self.device_id, "x-signature": self.signature})

        if share_id:
            share_token = self.share_token(share_id)
            assert share_token, "Need share_token"

            headers["x-share-token"] = share_token

        data = dict(
            file_id=file_id,
            fields="*",
            image_thumbnail_process="image/resize,w_400/format,jpeg",
            image_url_process="image/resize,w_375/format,jpeg",
            video_thumbnail_process="video/snapshot,t_1000,f_jpg,ar_auto,w_375",
        )

        if share_id:
            data["share_id"] = share_id
        else:
            data["drive_id"] = self.default_drive_id

        resp = self._request(Method.Post, url, headers=headers, json=data)
        info = resp.json()
        return info

    def exists(self, file_id: str) -> bool:
        """Check whether the file exists

        Return True if the file exists and does not in the trash else False.
        """

        if file_id == "root":
            return True

        try:
            info = self.meta(file_id)
        except AliPCSError as err:
            if err.error_code and err.error_code.startswith("NotFound."):
                return False
            raise err

        if info.get("parent_file_id") == "recyclebin":
            # The file is not exists or in the trash
            return False
        else:
            return True

    def exists_in_trash(self, file_id: str) -> bool:
        """Check whether the file exists in the trash

        Return True if the file exists in the trash else False.
        """

        if file_id == "recyclebin":
            return True

        try:
            info = self.meta(file_id)
        except AliPCSError as err:
            if err.error_code and err.error_code.startswith("NotFound."):
                return False
            raise err

        if info.get("parent_file_id") == "recyclebin":
            return True
        else:
            return False

    def is_file(self, file_id: str) -> bool:
        if file_id == "root":
            return False

        try:
            info = self.meta(file_id)
        except AliPCSError as err:
            if err.error_code and err.error_code.startswith("NotFound."):
                return False
            raise err

        if info["type"] == "file":
            return True
        else:
            return False

    def is_dir(self, file_id: str) -> bool:
        if file_id == "root":
            return True

        try:
            info = self.meta(file_id)
        except AliPCSError as err:
            if err.error_code and err.error_code.startswith("NotFound."):
                return False
            raise err

        if info["type"] == "folder":
            return True
        else:
            return False

    @handle_error
    def list(
        self,
        file_id: str,
        share_id: Optional[str] = None,
        desc: bool = False,
        name: bool = False,
        time: bool = False,
        size: bool = False,
        all: bool = False,
        limit: int = 200,
        url_expire_sec: int = 14400,
        next_marker: str = "",
    ):
        """List files at the directory which has `file_id`

        Args:
            file_id (str):
                The file_id of the directory
        """

        assert limit <= 200, "`limit` should be less than 200"

        url = PcsNode.FileList.url()
        orderby = "name"
        if name:
            orderby = "name"
        elif time:
            orderby = "updated_at"  # "created_at"  # 服务器最后修改时间
        elif size:
            orderby = "size"

        data = dict(
            all=all,
            drive_id=self.default_drive_id,
            fields="*",
            limit=limit,
            order_by=orderby,
            order_direction="DESC" if desc else "ASC",
            parent_file_id=file_id,
            url_expire_sec=url_expire_sec,
            image_thumbnail_process="image/resize,w_256/format,jpeg",
            image_url_process="image/resize,w_1920/format,jpeg/interlace,1",
            video_thumbnail_process="video/snapshot,t_1000,f_jpg,ar_auto,w_256",
        )

        if next_marker:
            data["marker"] = next_marker

        headers = dict(PCS_HEADERS)
        headers.update({"x-device-id": self.device_id, "x-signature": self.signature})
        if share_id:
            share_token = self.share_token(share_id)
            assert share_token, "Need share_token"

            data["share_id"] = share_id
            data.pop("drive_id")
            headers["x-share-token"] = share_token

        resp = self._request(Method.Post, url, headers=headers, json=data)
        return resp.json()

    @handle_error
    def walk(
        self,
        file_id: str,
        share_id: Optional[str] = None,
        all: bool = False,
        limit: int = 200,
        url_expire_sec: int = 14400,
        next_marker: str = "",
    ):
        """Walk through the directory tree which has `file_id`"""

        url = PcsNode.Walk.url()

        headers = dict(PCS_HEADERS)
        headers.update({"x-device-id": self.device_id, "x-signature": self.signature})

        data = dict(
            all=all,
            drive_id=self.default_drive_id,
            fields="*",
            limit=limit,
            parent_file_id=file_id,
            url_expire_sec=url_expire_sec,
            image_thumbnail_process="image/resize,w_256/format,jpeg",
            image_url_process="image/resize,w_1920/format,jpeg/interlace,1",
            video_thumbnail_process="video/snapshot,t_1000,f_jpg,ar_auto,w_256",
        )
        if next_marker:
            data["marker"] = next_marker

        if share_id:
            share_token = self.share_token(share_id)
            assert share_token, "Need share_token"

            data["share_id"] = share_id
            data.pop("drive_id")
            headers["x-share-token"] = share_token

        resp = self._request(Method.Post, url, headers=headers, json=data)
        info = resp.json()
        return info

    @staticmethod
    def part_info_list(part_number: int) -> List[Dict[str, int]]:
        return [dict(part_number=i) for i in range(1, part_number + 1)]

    @handle_error
    def create_file(
        self,
        filename: str,
        dir_id: str,
        size: int,
        pre_hash: str = "",
        content_hash: str = "",
        part_number: int = 1,
        proof_code: str = "",
        check_name_mode: CheckNameMode = "auto_rename",
    ):
        """Create a prepared file for uploading

        filename (str):
            The name of file.
        dir_id (str):
            The directory id where the file is at.
        size (int):
            the length of total content.
        pre_hash (str):
            The sha1 of the IO first 1k bytes
        content_hash (str):
            the sha1 of total content.
        part_number (int):
            The number of one file's chunks to upload.
            The server will returns the number of urls to prepare to upload the file's chunks.
            `WARNNING`: this value MUST be set by caller.
        check_name_mode(str):
            'overwrite' (直接覆盖，以后多版本有用)
            'auto_rename' (自动换一个随机名称)
            'refuse' (不会创建，告诉你已经存在)
            'ignore' (会创建重名的)
        """

        url = PcsNode.CreateWithFolders.url()
        data = dict(
            drive_id=self.default_drive_id,
            part_info_list=self.part_info_list(part_number),
            parent_file_id=dir_id,
            name=filename,
            type="file",
            check_name_mode=check_name_mode,
            size=size,
            pre_hash=pre_hash,
            content_hash=content_hash,
            content_hash_name="sha1",
            proof_code=proof_code,
            proof_version="v1",
        )

        resp = self._request(Method.Post, url, json=data)
        return resp.json()

    def prepare_file(self, filename: str, dir_id: str, size: int, pre_hash: str = "", part_number: int = 1):
        return self.create_file(filename, dir_id, size, pre_hash=pre_hash, part_number=part_number)

    @handle_error
    def get_upload_url(self, upload_id: str, file_id: str, part_number: int):
        """Get upload slices' urls

        It is useful to get new upload slice url when these urls gotten from
        `AliPCS.prepare_file` or `AliPCS.create_file` are expired.
        """

        url = PcsNode.UploadUrl.url()
        data = dict(
            upload_id=upload_id,
            file_id=file_id,
            drive_id=self.default_drive_id,
            part_info_list=self.part_info_list(part_number),
        )

        resp = self._request(Method.Post, url, json=data)
        return resp.json()

    def rapid_upload_file(self, filename: str, dir_id: str, size: int, content_hash: str, proof_code: str):
        """Rapid Upload File

        size (int): the length of total content.
        content_hash (str): the sha1 of total content.
        """

        return self.create_file(filename, dir_id, size, content_hash=content_hash, proof_code=proof_code)

    def upload_slice(self, io: IO, url: str, callback_for_monitor: Optional[Callable[[int], Any]] = None) -> None:
        """Upload the content of io to remote url"""

        data = ChunkIO(io, total_len(io))
        if callback_for_monitor is not None:
            data = MultipartEncoderMonitor(data, callback=lambda monitor: callback_for_monitor(monitor.bytes_read))

        session = requests.Session()
        session.request(
            "PUT",
            url,
            headers=dict(PCS_HEADERS),
            data=data,
            timeout=(3, 9),  # (connect timeout, read timeout)
        )

    @handle_error
    def upload_complete(self, file_id: str, upload_id: str):
        url = PcsNode.UploadComplete.url()
        data = dict(drive_id=self.default_drive_id, file_id=file_id, upload_id=upload_id)
        resp = self._request(Method.Post, url, json=data)
        return resp.json()

    @handle_error
    def search(
        self,
        keyword: str,
        desc: bool = False,
        name: bool = False,
        time: bool = False,
        size: bool = False,
        all: bool = False,
        limit: int = 100,
        next_marker: str = "",
    ):
        assert limit <= 100, "`limit` should be less than 100"

        url = PcsNode.Search.url()
        orderby = "name"
        if name:
            orderby = "name"
        elif time:
            orderby = "updated_at"  # "created_at"  # 服务器最后修改时间
        elif size:
            orderby = "size"

        data = dict(
            all=all,
            drive_id=self.default_drive_id,
            limit=limit,
            order_by=orderby + " " + ("DESC" if desc else "ASC"),
            query=f'name match "{keyword}"',
            marker=next_marker,
            image_thumbnail_process="image/resize,w_160/format,jpeg",
            image_url_process="image/resize,w_1920/format,jpeg",
            video_thumbnail_process="video/snapshot,t_0,f_jpg,ar_auto,w_300",
        )
        resp = self._request(Method.Post, url, json=data)
        return resp.json()

    @handle_error
    def makedir(self, dir_id: str, name: str):
        url = PcsNode.CreateWithFolders.url()
        data = dict(
            check_name_mode="refuse",  # or "auto_rename"
            drive_id=self.default_drive_id,
            name=name,
            parent_file_id=dir_id,
            type="folder",
        )
        resp = self._request(Method.Post, url, json=data)
        return resp.json()

    @handle_error
    def batch_operate(
        self, requests_: List[Dict[str, Any]], resource: str = "file", headers: Optional[Dict[str, str]] = None
    ):
        url = PcsNode.Batch.url()
        data = dict(resource=resource, requests=requests_)
        resp = self._request(Method.Post, url, headers=headers, json=data)
        return resp.json()

    def move(self, *file_ids: str):
        """
        Move sources to destination

        source_ids, dest_id = file_ids[:-1], file_ids[-1]

        `dest_id` must be a directory
        """

        source_ids, dest_id = file_ids[:-1], file_ids[-1]

        if self.is_file(dest_id):
            raise AliPCSError("The remote `dest_id` is a file. It must be a directory.")

        requests_ = []
        for source_id in source_ids:
            req = dict(
                method="POST",
                url="/file/move",
                id=source_id,
                headers={"Content-Type": "application/json"},
                body=dict(
                    drive_id=self.default_drive_id,
                    to_drive_id=self.default_drive_id,
                    file_id=source_id,
                    to_parent_file_id=dest_id,
                ),
            )
            requests_.append(req)

        return self.batch_operate(requests_, resource="file")

    @handle_error
    def rename(self, file_id: str, name: str):
        """Rename the file to `name`"""

        url = PcsNode.Update.url()
        data = dict(
            check_name_mode="refuse",
            drive_id=self.default_drive_id,
            file_id=file_id,
            name=name,  # or "auto_rename"
        )
        resp = self._request(Method.Post, url, json=data)
        return resp.json()

    def copy(self, *file_ids: str):
        assert len(file_ids) > 1

        to_dir_id = file_ids[-1]
        requests_ = []
        for file_id in file_ids[:-1]:
            req = dict(
                method="POST",
                url="/file/copy",
                id=file_id,
                headers={"Content-Type": "application/json"},
                body=dict(
                    drive_id=self.default_drive_id,
                    file_id=file_id,
                    to_parent_file_id=to_dir_id,
                    overwrite=False,
                    auto_rename=True,
                ),
            )
            requests_.append(req)
        return self.batch_operate(requests_, resource="file")

    def remove(self, *file_ids: str):
        requests_ = []
        for file_id in file_ids:
            req = dict(
                method="POST",
                url="/recyclebin/trash",
                id=file_id,
                headers={"Content-Type": "application/json"},
                body=dict(drive_id=self.default_drive_id, file_id=file_id),
            )
            requests_.append(req)
        return self.batch_operate(requests_, resource="file")

    @handle_error
    def check_available(self, file_ids: str):
        """Check whether file_ids are available"""

        url = PcsNode.Available.url()
        data = dict(
            drive_id=self.default_drive_id,
            file_id_list=list(file_ids),
        )
        resp = self._request(Method.Post, url, json=data)
        return resp.json()

    @handle_error
    def share(self, *file_ids: str, password: str = "", period: int = 0, description: str = ""):
        """Share `remotepaths` to public

        period (int): The days for expiring. `0` means no expiring
        """

        expiration = ""  # Living forever
        if period > 0:
            expiration = timestamp_to_iso_8601(now_timestamp() + period * 24 * 60 * 60)

        url = PcsNode.Share.url()
        data = dict(
            drive_id=self.default_drive_id,
            expiration=expiration,
            file_id_list=list(file_ids),
            share_pwd=password,
            description=description,
        )
        resp = self._request(Method.Post, url, json=data)
        return resp.json()

    @handle_error
    def list_shared(self, next_marker: str = ""):
        """List shared links"""

        url = PcsNode.SharedList.url()
        data = dict(
            creator=self.user_id,
            include_canceled=False,
            order_by="created_at",
            order_direction="DESC",
            marker=next_marker,
        )
        resp = self._request(Method.Post, url, json=data)
        return resp.json()

    def cancel_shared(self, *share_ids: str):
        requests_ = []
        for share_id in share_ids:
            req = dict(
                method="POST",
                url="/share_link/cancel",
                id=share_id,
                headers={"Content-Type": "application/json"},
                body=dict(share_id=share_id),
            )
            requests_.append(req)

        return self.batch_operate(requests_, resource="file")

    @handle_error
    def get_share_token(self, share_id: str, share_password: str = ""):
        """Get share token"""

        shared_auth = self.__class__.SHARE_AUTHS.get(share_id)
        if shared_auth is not None:
            share_password = share_password or shared_auth.share_password

            if not shared_auth.is_expired():
                return shared_auth.info

        url = PcsNode.ShareToken.url()
        data = dict(share_id=share_id, share_pwd=share_password)
        resp = self._request(Method.Post, url, json=data)
        info = resp.json()
        if info.get("share_token"):
            # Store share password for refreshing share token
            self.__class__.SHARE_AUTHS[share_id] = SharedAuth.from_(share_id, share_password, info)

        return info

    def share_token(self, share_id: str) -> str:
        self.get_share_token(share_id)

        shared_auth = self.__class__.SHARE_AUTHS[share_id]
        return shared_auth.share_token

    @handle_error
    def shared_info(self, share_id: str):
        """Get shared items info"""

        url = PcsNode.SharedInfo.url()
        params = dict(share_id=share_id)
        data = dict(share_id=share_id)
        resp = self._request(Method.Post, url, params=params, json=data)
        info = resp.json()
        info["share_id"] = share_id
        return info

    # list_shared_files is an alias of list
    list_shared_files = list

    def transfer_shared_files(
        self, shared_file_ids: List[str], dest_id: str, share_id: str, auto_rename: bool = False
    ):
        """Transfer shared files to destination directory"""

        requests_ = []
        for i, file_id in enumerate(shared_file_ids):
            req = dict(
                method="POST",
                url="/file/copy",
                id=str(i),
                headers={"Content-Type": "application/json"},
                body=dict(
                    auto_rename=auto_rename,
                    file_id=file_id,
                    share_id=share_id,
                    to_drive_id=self.default_drive_id,
                    to_parent_file_id=dest_id,
                ),
            )
            requests_.append(req)

        share_token = self.share_token(share_id)
        assert share_token, "Need share_token"

        headers = dict(PCS_HEADERS)
        headers["x-share-token"] = share_token

        return self.batch_operate(requests_, resource="file", headers=headers)

    @handle_error
    def _get_shared_file_download_url(self, shared_file_id: str, share_id: str, expire_duration: int = 10 * 60):
        url = PcsNode.SharedFileDownloadUrl.url()
        data = dict(expire_sec=expire_duration, file_id=shared_file_id, share_id=share_id)

        share_token = self.share_token(share_id)
        assert share_token, "Need share_token"

        headers = dict(PCS_HEADERS)
        headers["x-share-token"] = share_token

        resp = self._request(Method.Post, url, headers=headers, json=data)
        return resp.json()

    def shared_file_download_url(self, shared_file_id: str, share_id: str, expire_duration: int = 10 * 60) -> str:
        info = self._get_shared_file_download_url(shared_file_id, share_id, expire_duration=expire_duration)
        url = info["url"]

        headers = dict(PCS_HEADERS)
        headers["Referer"] = "https://www.aliyundrive.com/"
        resp = requests.get(url, headers=headers, allow_redirects=False)
        return resp.headers["Location"]

    @handle_error
    def user_info(self):
        url = PcsNode.User.url()
        resp = self._request(Method.Post, url, json={})
        info1 = resp.json()

        headers = dict(PCS_HEADERS)
        headers.update({"x-device-id": self.device_id, "x-signature": self.signature})
        url = PcsNode.PersonalInfo.url()
        resp = self._request(Method.Post, url, headers=headers, json={})
        info2 = resp.json()

        url = PcsNode.UserVip.url()
        resp = self._request(Method.Post, url, headers=headers, json={})
        info3 = resp.json()

        return {**info1, **info2, "user_vip_info": info3}

    @timeout_cache(1 * 60 * 60)  # 1 hour timeout
    @handle_error
    def download_link(self, file_id: str):
        """Get download link of the file by its file_id

        First try to get the download link from the meta info of the file.
        If the download link is not in the meta info, then request the getting download url api.
        """

        info = self.meta(file_id)
        if info.get("url") or info.get("download_url"):
            return info

        url = PcsNode.DownloadUrl.url()
        data = dict(drive_id=self.default_drive_id, file_id=file_id)
        headers = dict(PCS_HEADERS)
        headers.update({"x-device-id": self.device_id, "x-signature": self.signature})
        resp = self._request(Method.Post, url, headers=headers, json=data)
        return resp.json()

    def file_stream(
        self,
        file_id: str,
        max_chunk_size: int = DEFAULT_MAX_CHUNK_SIZE,
        callback: Optional[Callable[..., None]] = None,
        encrypt_password: bytes = b"",
    ) -> Optional[RangeRequestIO]:
        info = self.download_link(file_id)
        url = info.get("url") or info.get("download_url")

        headers = {
            "User-Agent": PCS_UA,
            "Connection": "Keep-Alive",
            "Referer": "https://www.aliyundrive.com/",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7,zh-TW;q=0.6",
        }
        return RangeRequestIO(
            Method.Get.value,
            url,
            headers=headers,
            max_chunk_size=max_chunk_size,
            callback=callback,
            encrypt_password=encrypt_password,
        )

    def shared_file_stream(
        self,
        shared_file_id: str,
        share_id: str,
        expire_duration: int = 10 * 60,
        max_chunk_size: int = DEFAULT_MAX_CHUNK_SIZE,
        callback: Optional[Callable[..., None]] = None,
        encrypt_password: bytes = b"",
    ) -> Optional[RangeRequestIO]:
        url = self.shared_file_download_url(shared_file_id, share_id, expire_duration=expire_duration)

        headers = {
            "User-Agent": PCS_UA,
            "Connection": "Keep-Alive",
            "Referer": "https://www.aliyundrive.com/",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7,zh-TW;q=0.6",
        }
        return RangeRequestIO(
            Method.Get.value,
            url,
            headers=headers,
            max_chunk_size=max_chunk_size,
            callback=callback,
            encrypt_password=encrypt_password,
        )


class OpenPcsNode(Enum):
    f"""PCS Nodes which use {ALIYUNDRIVE_OPENAPI_DOMAIN}"""

    UpdateRefreshToken = "oauth/access_token"
    QrcodeLink = "oauth/authorize/qrcode"
    QrcodeUrl = "o/oauth/authorize?sid={sid}"
    QrcodeStatus = "oauth/qrcode/{sid}/status"

    Token = "v2/account/token"
    CreateSession = "users/v1/users/device/create_session"
    DriveInfo = "adrive/v1.0/user/getDriveInfo"

    Meta = "adrive/v1.0/openFile/get"
    FileList = "adrive/v1.0/openFile/list"
    Search = "adrive/v3/file/search"
    DownloadUrl = "adrive/v1.0/openFile/getDownloadUrl"

    CreateFile = "v2/file/create"
    UploadComplete = "v2/file/complete"

    Batch = "v3/batch"

    CreateWithFolders = "adrive/v2/file/createWithFolders"
    Move = "adrive/v3/file/move"
    Update = "v3/file/update"
    Trash = "v2/recyclebin/trash"

    Available = "adrive/v2/share_link/check_avaliable"
    Share = "adrive/v2/share_link/create"
    ShareToken = "v2/share_link/get_share_token"
    SharedInfo = "adrive/v3/share_link/get_share_by_anonymous"
    SharedList = "adrive/v3/share_link/list"
    SharedFileDownloadUrl = "v2/file/get_share_link_download_url"

    SpaceInfo = "adrive/v1.0/user/getSpaceInfo"
    # PersonalInfo = "v2/databox/get_personal_info"
    PersonalInfo = "adrive/v1.0/user/get"
    User = "v2/user/get"
    UserVip = "adrive/v1.0/user/getVipInfo"

    def url(self) -> str:
        return f"{ALIYUNDRIVE_OPENAPI_DOMAIN}/{self.value}"


class AliOpenPCS:
    """Alipan Drive Personal Cloud Service Raw Open API

    The core class is used to interact with Alipan Drive Personal Cloud Service with open apis.
    It provides the basic operations of the service and handles the raw requests and responses.

    An `AliPCSError` error will be raised if the code of the response occurs.

    Args:
        refresh_token (str): The refresh token of the user.
        access_token (str, optional): The access token of the user.
        token_type (str): The token type. Default is "Bearer".
        expire_time (int, optional): The expire time of the token.
        user_id (str, optional): The user id of the user.
        user_name (str, optional): The user name of the user.
        nick_name (str, optional): The nick name of the user.
        device_id (str, optional): The device id of the user.
        default_drive_id (str, optional): The default drive id of the user.
        role (str, optional): The role of the user.
        status (str, optional): The status of the user.
        error_max_retries (int): The max retries when a client request error occurs. Default is 2.
        max_keepalive_connections (int): The max keepalive connections. Default is 50.
        max_connections (int): The max number of connections in the pool. Default is 50.
        keepalive_expiry (float): The keepalive expiry. Default is 10 * 60 seconds.
        connection_max_retries (int): The max retries when a connection error occurs. Default is 2.
    """

    SHARE_AUTHS: Dict[str, SharedAuth] = {}

    def __init__(
        self,
        refresh_token: str,
        access_token: str = "",
        token_type: str = "Bearer",
        expire_time: int = 0,
        client_id: str = "",
        client_secret: str = "",
        client_server: str = "",
        user_id: str = "",
        user_name: str = "",
        nick_name: str = "",
        default_drive_id: str = "",
        role: str = "",
        status: str = "",
        error_max_retries: int = 2,
        max_keepalive_connections: int = 50,
        max_connections: int = 50,
        keepalive_expiry: float = 10 * 60,
        connection_max_retries: int = 2,
    ):
        assert (
            client_id and client_secret
        ) or client_server, "(client_id and client_secret) or client_server must be set"

        self._session = make_http_session(
            max_keepalive_connections=max_keepalive_connections,
            max_connections=max_connections,
            keepalive_expiry=keepalive_expiry,
            max_retries=connection_max_retries,
        )

        self._error_max_retries = error_max_retries

        self._refresh_token = refresh_token
        self._access_token = access_token
        self._token_type = token_type
        self._expire_time = expire_time

        self._client_id = client_id
        self._client_secret = client_secret
        self._client_server = client_server

        self._user_id = user_id
        self._user_name = user_name
        self._nick_name = nick_name
        self._default_drive_id = default_drive_id
        self._role = role
        self._status = status
        self._nonce = 0

    def __str__(self) -> str:
        return f"""AliPCS

    user_id: {self.user_id}
    user_name: {self.user_name}
    nick_name: {self.nick_name}

    refresh_token: {self.refresh_token}
    access_token: {self.access_token}
    token_type: {self.token_type}
    expire_time: {self.expire_time}

    client_id: {self._client_id}
    client_secret: {self._client_secret}
    client_server: {self._client_server}

    default_drive_id: {self.default_drive_id}
    role: {self.role}
    status: {self.status}
    nonce: {self._nonce}
    """

    @property
    def refresh_token(self) -> str:
        now = now_timestamp()
        if self._expire_time - now < 200 or not self._access_token:
            self._update_refresh_token()

        if not self._default_drive_id:
            self._get_drive_info()

        return self._refresh_token

    @property
    def access_token(self) -> str:
        self.refresh_token
        return self._access_token

    @property
    def client_id(self) -> str:
        return self._client_id

    @property
    def client_secret(self) -> str:
        return self._client_secret

    @property
    def client_server(self) -> str:
        return self._client_server

    @property
    def user_id(self) -> str:
        self.refresh_token
        return self._user_id

    @property
    def user_name(self) -> str:
        self.refresh_token
        return self._user_name

    @property
    def nick_name(self) -> str:
        self.refresh_token
        return self._nick_name

    @property
    def token_type(self) -> str:
        self.refresh_token
        return self._token_type

    @property
    def expire_time(self) -> int:
        self.refresh_token
        return self._expire_time

    @property
    def default_drive_id(self) -> str:
        self.refresh_token
        return self._default_drive_id

    @property
    def role(self) -> str:
        self.refresh_token
        return self._role

    @property
    def status(self) -> str:
        self.refresh_token
        return self._status

    def _request(
        self,
        method: Method,
        url: str,
        params: Optional[Dict[str, str]] = None,
        headers: Optional[Dict[str, str]] = None,
        data: Union[str, bytes, Dict[str, str], Any] = None,
        json: Any = None,
        files: Optional[Dict[str, Any]] = None,
        refresh: bool = False,
        **kwargs,
    ) -> requests.Response:
        if not headers:
            headers = dict(PCS_HEADERS)

        if not refresh and "Authorization" not in headers:
            headers["Authorization"] = f"{self._token_type} {self._access_token}"

        if json is not None:
            headers["Content-Type"] = "application/json;charset=UTF-8"

        if isinstance(data, (MultipartEncoder, MultipartEncoderMonitor)):
            headers["Content-Type"] = data.content_type

        try:
            resp = self._session.request(
                method.value, url, params=params, headers=headers, data=data, json=json, files=files, **kwargs
            )
            return resp
        except Exception as err:
            raise AliPCSError("AliPCS._request") from err

    def _update_refresh_token(self):
        """Update refresh token"""

        data = dict(refresh_token=self._refresh_token, grant_type="refresh_token")
        if self._client_id and self._client_secret:
            url = OpenPcsNode.UpdateRefreshToken.url()
            data["client_id"] = self._client_id
            data["client_secret"] = self._client_secret
        else:
            url = urljoin(self._client_server, OpenPcsNode.UpdateRefreshToken.value)

        resp = self._request(Method.Post, url, json=data, refresh=True)
        info = resp.json()

        if "code" in info:
            err = make_alipcs_error(info["code"], info=info)
            raise err

        self._refresh_token = info["refresh_token"]
        self._access_token = info["access_token"]
        self._token_type = info["token_type"]
        self._expire_time = now_timestamp() + info["expires_in"]

        return info

    def _get_drive_info(self):
        """Get drive info"""

        url = OpenPcsNode.DriveInfo.url()
        data = dict()

        resp = self._request(Method.Post, url, json=data)
        info = resp.json()

        if "code" in info:
            err = make_alipcs_error(info["code"], info=info)
            raise err

        self._user_id = info["user_id"]
        self._user_name = info["user_name"]
        self._nick_name = info["nick_name"]
        self._default_drive_id = info["default_drive_id"]
        self._domain_id = info.get("domain_id")
        self._role = info.get("role")
        self._status = info.get("status")

        return info

    @handle_error
    def meta(self, file_id: str, share_id: Optional[str] = None):
        data = dict(file_id=file_id, fields="*", drive_id=self.default_drive_id)
        url = OpenPcsNode.Meta.url()
        resp = self._request(Method.Post, url, json=data)
        info = resp.json()
        return info

    def exists(self, file_id: str) -> bool:
        if file_id == "root":
            return True

        try:
            info = self.meta(file_id)
        except AliPCSError as err:
            if err.error_code and err.error_code.startswith("NotFound."):
                return False
            raise err

        return True

    def is_file(self, file_id: str) -> bool:
        if file_id == "root":
            return False

        try:
            info = self.meta(file_id)
        except AliPCSError as err:
            if err.error_code and err.error_code.startswith("NotFound."):
                return False
            raise err

        if info["type"] == "file":
            return True
        else:
            return False

    def is_dir(self, file_id: str) -> bool:
        if file_id == "root":
            return True

        try:
            info = self.meta(file_id)
        except AliPCSError as err:
            if err.error_code and err.error_code.startswith("NotFound."):
                return False
            raise err

        if info["type"] == "folder":
            return True
        else:
            return False

    @handle_error
    def list(
        self,
        file_id: str,
        share_id: Optional[str] = None,
        desc: bool = False,
        name: bool = False,
        time: bool = False,
        size: bool = False,
        all: bool = False,
        limit: int = 200,
        url_expire_sec: int = 14200,
        next_marker: str = "",
    ):
        """List files at the directory which has `file_id`

        Args:
            file_id (str):
                The file_id of the directory
        """

        assert limit <= 200, "`limit` should be less than 200"

        url = OpenPcsNode.FileList.url()
        orderby = "name"
        if name:
            orderby = "name"
        elif time:
            orderby = "updated_at"  # "created_at"  # 服务器最后修改时间
        elif size:
            orderby = "size"

        data = dict(
            all=all,
            drive_id=self.default_drive_id,
            fields="*",
            limit=limit,
            order_by=orderby,
            order_direction="DESC" if desc else "ASC",
            parent_file_id=file_id,
            url_expire_sec=url_expire_sec,
            marker=next_marker,
        )

        headers = dict(PCS_HEADERS)
        resp = self._request(Method.Post, url, headers=headers, json=data)
        return resp.json()

    @staticmethod
    def part_info_list(part_number: int) -> List[Dict[str, int]]:
        return [dict(part_number=i) for i in range(1, part_number + 1)]

    @timeout_cache(4 * 60 * 60)  # 4 hour timeout
    @handle_error
    def download_link(self, file_id: str):
        url = OpenPcsNode.DownloadUrl.url()
        data = dict(drive_id=self.default_drive_id, file_id=file_id, expire_sec=14400)
        resp = self._request(Method.Post, url, json=data)
        return resp.json()

    def file_stream(
        self,
        file_id: str,
        max_chunk_size: int = DEFAULT_MAX_CHUNK_SIZE,
        callback: Optional[Callable[..., None]] = None,
        encrypt_password: bytes = b"",
    ) -> Optional[RangeRequestIO]:
        info = self.download_link(file_id)
        url = info["url"]

        headers = {
            "User-Agent": PCS_UA,
            "Connection": "Keep-Alive",
            "Referer": "https://www.aliyundrive.com/",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7,zh-TW;q=0.6",
        }
        return RangeRequestIO(
            Method.Get.value,
            url,
            headers=headers,
            max_chunk_size=max_chunk_size,
            callback=callback,
            encrypt_password=encrypt_password,
        )


class AliOpenAuth:
    def __init__(self, client_id: str = "", client_secret: str = "", client_server: str = ""):
        assert (
            client_id and client_secret
        ) or client_server, "(client_id and client_secret) or client_server must be set"

        self._session = requests.Session()

        self._client_id = client_id
        self._client_secret = client_secret
        self._client_server = client_server

    def _request(
        self,
        method: Method,
        url: str,
        params: Optional[Dict[str, str]] = None,
        headers: Optional[Dict[str, str]] = None,
        data: Union[str, bytes, Dict[str, str], Any] = None,
        json: Any = None,
        files: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> requests.Response:
        if not headers:
            headers = dict(PCS_HEADERS)

        try:
            resp = self._session.request(
                method.value, url, params=params, headers=headers, data=data, json=json, files=files, **kwargs
            )
            return resp
        except Exception as err:
            raise AliPCSError("AliOpenAuth._request") from err

    @staticmethod
    def qrcode_url(sid: str) -> str:
        return OpenPcsNode.QrcodeUrl.url().format(sid)

    @handle_error
    def get_qrcode_info(self):
        data: Dict[str, Any] = dict(
            scopes=[
                "user:base",
                "file:all:read",
                "file:all:write",
            ],
            width=None,
            height=None,
        )

        if self._client_server:
            url = self._client_server.strip("/") + "/" + OpenPcsNode.QrcodeLink.value
        else:
            data["client_id"] = self._client_id
            data["client_secret"] = self._client_secret
            url = OpenPcsNode.QrcodeLink.url()

        resp = self._request(Method.Post, url, json=data)
        return resp.json()

    @handle_error
    def scan_status(self, sid: str):
        url = OpenPcsNode.QrcodeStatus.url().format(sid=sid)
        resp = self._request(Method.Get, url)
        return resp.json()

    @handle_error
    def get_refresh_token(self, auth_code: str):
        data = dict(grant_type="authorization_code", code=auth_code)

        if self._client_server:
            url = self._client_server.strip("/") + "/" + OpenPcsNode.UpdateRefreshToken.value
        else:
            data["client_id"] = self._client_id
            data["client_secret"] = self._client_secret
            url = OpenPcsNode.UpdateRefreshToken.url()

        resp = self._request(Method.Post, url, json=data)
        return resp.json()
