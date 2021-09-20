from typing import Optional, Dict, List, Union, Any, Callable, IO
from enum import Enum

import math
import time
import threading

import requests
from requests_toolbelt import MultipartEncoder, MultipartEncoderMonitor

from alipcs_py.common.date import (
    now_timestamp,
    iso_8601_to_timestamp,
    timestamp_to_iso_8601,
)
from alipcs_py.common import constant
from alipcs_py.common.io import (
    RangeRequestIO,
    DEFAULT_MAX_CHUNK_SIZE,
    ChunkIO,
    total_len,
)
from alipcs_py.common.cache import timeout_cache
from alipcs_py.alipcs.errors import AliPCSError, parse_error, to_refresh_token
from alipcs_py.alipcs.errors import assert_ok


UPLOAD_CHUNK_SIZE = 10 * constant.OneM

PCS_BAIDU_COM = "https://api.aliyundrive.com"
# PCS_BAIDU_COM = 'http://127.0.0.1:8888'

PCS_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.75 Safari/537.36"
PCS_HEADERS = {"User-Agent": PCS_UA}


class Method(Enum):
    Head = "HEAD"
    Get = "GET"
    Post = "POST"


class PcsNode(Enum):
    f"""PCS Nodes which use {PCS_BAIDU_COM}"""

    Refresh = "token/refresh"

    FileList = "adrive/v3/file/list"
    Meta = "v2/file/get"
    Search = "adrive/v3/file/search"
    DownloadUrl = "v2/file/get_download_url"

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

    PersonalInfo = "v2/databox/get_personal_info"
    User = "v2/user/get"

    def url(self) -> str:
        return f"{PCS_BAIDU_COM}/{self.value}"


# The Lock is used to refresh token
_LOCK = threading.Lock()


class AliPCS:
    """`AliPCS` provides pcs's apis which return raw json"""

    def __init__(
        self,
        refresh_token: str,
        access_token: str = "",
        expire_time: int = 0,
        user_id: str = "",
        user_name: str = "",
        nick_name: str = "",
        token_type: str = "",
        device_id: str = "",
        default_drive_id: str = "",
        role: str = "",
        status: str = "",
    ):
        self._session = requests.Session()
        self._refresh_token = refresh_token
        self._access_token = access_token
        self._expire_time = expire_time

        self._user_id = user_id
        self._user_name = user_name
        self._nick_name = nick_name
        self._token_type = token_type
        self._device_id = device_id
        self._default_drive_id = default_drive_id
        self._role = role
        self._status = status

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
    default_drive_id: {self.default_drive_id}
    role: {self.role}
    status: {self.status}
    """

    @property
    def refresh_token(self) -> str:
        with _LOCK:
            if (
                not self._access_token
                or (now_timestamp() - 1 * 60 * 60) >= self._expire_time
            ):
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

        if json is not None:
            headers["Content-Type"] = "application/json;charset=UTF-8"

        if isinstance(data, (MultipartEncoder, MultipartEncoderMonitor)):
            headers["Content-Type"] = data.content_type

        try:
            resp = self._session.request(
                method.value,
                url,
                params=params,
                headers=headers,
                data=data,
                json=json,
                files=files,
                **kwargs,
            )
            return resp
        except Exception as err:
            raise AliPCSError("AliPCS._request", cause=err)

    def refresh(self):
        """Refresh token"""

        url = PcsNode.Refresh.url()
        data = dict(refresh_token=self._refresh_token)
        resp = self._request(Method.Post, url, json=data, refresh=True)
        info = resp.json()

        if "code" in info:
            err = parse_error(info["code"], info=info)
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

    @assert_ok
    @to_refresh_token
    def meta(self, *file_ids: str, share_id: str = None, share_token: str = None):
        assert "root" not in file_ids, '"root" has NOT meta info'

        headers = dict(PCS_HEADERS)
        if share_id:
            assert share_token, "Need share_token"

            headers["x-share-token"] = share_token

        requests = []
        for file_id in file_ids:
            body = dict(
                file_id=file_id,
            )
            if share_id:
                body["share_id"] = share_id
            else:
                body["drive_id"] = self.default_drive_id

            req = dict(
                method="POST",
                url="/file/get",
                id=file_id,
                headers={"Content-Type": "application/json"},
                body=body,
            )
            requests.append(req)

        return self.batch_operate(requests, resource="file", headers=headers)

    def exists(self, file_id: str) -> bool:
        if file_id == "root":
            return True

        r = self.meta(file_id)
        info = r["responses"][0]["body"]
        if info.get("code") == "NotFound.File":
            return False
        else:
            return True

    def is_file(self, file_id: str) -> bool:
        if file_id == "root":
            return False

        r = self.meta(file_id)
        info = r["responses"][0]["body"]
        if info.get("code") == "NotFound.File":
            return False
        if info["type"] == "file":
            return True
        else:
            return False

    def is_dir(self, file_id: str) -> bool:
        if file_id == "root":
            return True

        r = self.meta(file_id)
        info = r["responses"][0]["body"]
        if info.get("code") == "NotFound.File":
            return False
        if info["type"] == "folder":
            return True
        else:
            return False

    @assert_ok
    @to_refresh_token
    def list(
        self,
        file_id: str,
        share_id: str = None,
        share_token: str = None,
        desc: bool = False,
        name: bool = False,
        time: bool = False,
        size: bool = False,
        all: bool = False,
        limit: int = 100,
        url_expire_sec: int = 7200,
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
            marker=next_marker,
            # image_thumbnail_process="image/resize,w_400/format,jpeg",
            # image_url_process="image/resize,w_1920/format,jpeg",
            # video_thumbnail_process="video/snapshot,t_0,f_jpg,ar_auto,w_300",
        )

        headers = dict(PCS_HEADERS)
        if share_id:
            assert share_token, "Need share_token"

            data["share_id"] = share_id
            data.pop("drive_id")
            headers["x-share-token"] = share_token

        resp = self._request(Method.Post, url, headers=headers, json=data)
        return resp.json()

    @staticmethod
    def part_info_list(size: int) -> List[Dict[str, int]]:
        return [
            dict(part_number=i)
            for i in range(1, math.ceil(size / UPLOAD_CHUNK_SIZE) + 1)
        ]

    @to_refresh_token
    def create_file(
        self,
        filename: str,
        dir_id: str,
        size: int,
        pre_hash: str = "",
        content_hash: str = "",
        proof_code: str = "",
        check_name_mode: str = "auto_rename",
    ):
        """
        Args:
            size (int):
                the length of total content.
            pre_hash (str):
                The sha1 of the IO first 1k bytes
            content_hash (str):
                the sha1 of total content.
        """

        url = PcsNode.CreateWithFolders.url()
        data = dict(
            drive_id=self.default_drive_id,
            part_info_list=self.part_info_list(size),
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

    def prepare_file(self, filename: str, dir_id: str, size: int, pre_hash: str = ""):
        return self.create_file(filename, dir_id, size, pre_hash=pre_hash)

    @assert_ok
    @to_refresh_token
    def rapid_upload_file(
        self, filename: str, dir_id: str, size: int, content_hash: str, proof_code: str
    ):
        """Rapid Upload File

        size (int): the length of total content.
        content_hash (str): the sha1 of total content.
        """

        return self.create_file(
            filename, dir_id, size, content_hash=content_hash, proof_code=proof_code
        )

    def upload_slice(
        self,
        io: IO,
        url: str,
        callback: Callable[[MultipartEncoderMonitor], None] = None,
    ) -> None:
        """Upload the content of io to remote url"""

        cio = ChunkIO(io, total_len(io))
        monitor = MultipartEncoderMonitor(cio, callback=callback)

        session = requests.Session()
        session.request(
            "PUT",
            url,
            headers=dict(PCS_HEADERS),
            data=monitor,
            # timeout=(3, 9),  # (connect timeout, read timeout)
        )

    @assert_ok
    @to_refresh_token
    def upload_complete(self, file_id: str, upload_id: str):
        url = PcsNode.UploadComplete.url()
        data = dict(
            drive_id=self.default_drive_id,
            file_id=file_id,
            upload_id=upload_id,
        )
        resp = self._request(Method.Post, url, json=data)
        return resp.json()

    @assert_ok
    @to_refresh_token
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
            # image_thumbnail_process="image/resize,w_160/format,jpeg",
            # image_url_process="image/resize,w_1920/format,jpeg",
            # video_thumbnail_process="video/snapshot,t_0,f_jpg,ar_auto,w_300",
        )
        resp = self._request(Method.Post, url, json=data)
        return resp.json()

    @assert_ok
    @to_refresh_token
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

    @assert_ok
    @to_refresh_token
    def batch_operate(
        self,
        requests: List[Dict[str, Any]],
        resource: str = "file",
        headers: Optional[Dict[str, str]] = None,
    ):
        url = PcsNode.Batch.url()
        data = dict(resource=resource, requests=requests)
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

        requests = []
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
            requests.append(req)

        return self.batch_operate(requests, resource="file")

    @assert_ok
    @to_refresh_token
    def rename(self, file_id: str, name: str):
        """Rename the file to `name`"""

        url = PcsNode.Update.url()
        data = dict(
            check_name_mode="refuse",  # or "auto_rename"
            drive_id=self.default_drive_id,
            file_id=file_id,
            name=name,
        )
        resp = self._request(Method.Post, url, json=data)
        return resp.json()

    def copy(self, *file_ids: str):
        assert len(file_ids) > 1

        to_dir_id = file_ids[-1]
        requests = []
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
            requests.append(req)
        return self.batch_operate(requests, resource="file")

    def remove(self, *file_ids: str):
        requests = []
        for file_id in file_ids:
            req = dict(
                method="POST",
                url="/recyclebin/trash",
                id=file_id,
                headers={"Content-Type": "application/json"},
                body=dict(
                    drive_id=self.default_drive_id,
                    file_id=file_id,
                ),
            )
            requests.append(req)
        return self.batch_operate(requests, resource="file")

    @assert_ok
    @to_refresh_token
    def check_available(self, file_ids: str):
        """Check whether file_ids are available"""

        url = PcsNode.Available.url()
        data = dict(
            drive_id=self.default_drive_id,
            file_id_list=list(file_ids),
        )
        resp = self._request(Method.Post, url, json=data)
        return resp.json()

    @assert_ok
    @to_refresh_token
    def share(
        self, *file_ids: str, password: str = "", period: int = 0, description: str = ""
    ):
        """Share `remotepaths` to public

        period (int): The days for expiring. `0` means no expiring
        """

        expiration = ""  # Living forever
        if period > 0:
            expiration = timestamp_to_iso_8601(int(time.time()) + period * 24 * 60 * 60)

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

    @assert_ok
    @to_refresh_token
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
        requests = []
        for share_id in share_ids:
            req = dict(
                method="POST",
                url="/share_link/cancel",
                id=share_id,
                headers={"Content-Type": "application/json"},
                body=dict(
                    share_id=share_id,
                ),
            )
            requests.append(req)

        return self.batch_operate(requests, resource="file")

    @assert_ok
    @to_refresh_token
    def get_share_token(self, share_id: str, share_password: str = ""):
        """Get share token"""

        url = PcsNode.ShareToken.url()
        data = dict(share_id=share_id, share_pwd=share_password)
        resp = self._request(Method.Post, url, json=data)
        return resp.json()

    @assert_ok
    @to_refresh_token
    def shared_info(self, share_id: str):
        url = PcsNode.SharedInfo.url()
        params = dict(share_id=share_id)
        data = dict(share_id=share_id)
        resp = self._request(Method.Post, url, params=params, json=data)
        return resp.json()

    # list_shared_files is an alias of list
    list_shared_files = list

    def transfer_shared_files(
        self,
        shared_file_ids: List[str],
        dest_id: str,
        share_id: str,
        share_token: str,
        auto_rename: bool = False,
    ):
        """Transfer shared files to destination directory"""

        requests = []
        for file_id in shared_file_ids:
            req = dict(
                method="POST",
                url="/file/copy",
                id="0",
                headers={"Content-Type": "application/json"},
                body=dict(
                    auto_rename=auto_rename,
                    file_id=file_id,
                    share_id=share_id,
                    to_drive_id=self.default_drive_id,
                    to_parent_file_id=dest_id,
                ),
            )
            requests.append(req)

        headers = dict(PCS_HEADERS)
        headers["x-share-token"] = share_token

        return self.batch_operate(requests, resource="file", headers=headers)

    @assert_ok
    @to_refresh_token
    def _get_shared_file_download_url(
        self,
        shared_file_id: str,
        share_id: str,
        share_token: str,
        expire_duration: int = 10 * 60,
    ):
        url = PcsNode.SharedFileDownloadUrl.url()
        data = dict(
            expire_sec=expire_duration,
            file_id=shared_file_id,
            share_id=share_id,
        )
        headers = dict(PCS_HEADERS)
        headers["x-share-token"] = share_token

        resp = self._request(Method.Post, url, headers=headers, json=data)
        return resp.json()

    def shared_file_download_url(
        self,
        shared_file_id: str,
        share_id: str,
        share_token: str,
        expire_duration: int = 10 * 60,
    ) -> str:
        info = self._get_shared_file_download_url(
            shared_file_id,
            share_id,
            share_token,
            expire_duration=expire_duration,
        )
        url = info["url"]

        headers = dict(PCS_HEADERS)
        headers["Referer"] = "https://www.aliyundrive.com/"
        resp = requests.get(url, headers=headers, allow_redirects=False)
        return resp.headers["Location"]

    @assert_ok
    @to_refresh_token
    def user_info(self):
        url = PcsNode.PersonalInfo.url()
        data = dict()
        resp = self._request(Method.Post, url, json=data)
        info1 = resp.json()

        url = PcsNode.User.url()
        data = dict()
        resp = self._request(Method.Post, url, json=data)
        info2 = resp.json()

        return {**info1, **info2}

    @timeout_cache(1 * 60 * 60)  # 1 hour timeout
    @assert_ok
    @to_refresh_token
    def download_link(self, file_id: str):
        url = PcsNode.DownloadUrl.url()
        data = dict(drive_id=self.default_drive_id, file_id=file_id)
        resp = self._request(Method.Post, url, json=data)
        return resp.json()

    def file_stream(
        self,
        file_id: str,
        max_chunk_size: int = DEFAULT_MAX_CHUNK_SIZE,
        callback: Callable[..., None] = None,
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

    def shared_file_stream(
        self,
        shared_file_id: str,
        share_id: str,
        share_token: str,
        expire_duration: int = 10 * 60,
        max_chunk_size: int = DEFAULT_MAX_CHUNK_SIZE,
        callback: Callable[..., None] = None,
        encrypt_password: bytes = b"",
    ) -> Optional[RangeRequestIO]:
        url = self.shared_file_download_url(
            shared_file_id,
            share_id,
            share_token,
            expire_duration=expire_duration,
        )

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
