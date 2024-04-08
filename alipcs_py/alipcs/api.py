from typing import Optional, List, Tuple, Dict, Union, DefaultDict, Iterable, Callable, IO
from threading import Lock
from collections import defaultdict
from copy import deepcopy
from functools import partial
import logging
import warnings

from alipcs_py.alipcs.errors import AliPCSError
from alipcs_py.common.io import RangeRequestIO, DEFAULT_MAX_CHUNK_SIZE
from alipcs_py.alipcs.pcs import AliPCS, AliOpenPCS, CheckNameMode
from alipcs_py.alipcs.inner import PcsFile, PcsPreparedFile, PcsSharedLink, PcsSharedLinkInfo, PcsUser, PcsDownloadUrl
from alipcs_py.common.path import split_posix_path, join_path, posix_path_dirname

from requests_toolbelt import MultipartEncoderMonitor

logger = logging.getLogger(__name__)


_ALI_PCS_API_LOCK = Lock()


class AliPCSApi:
    """Alipan Drive Personal Cloud Service API

    This is the wrapper of `AliPCS` class. It parses the raw content of response of
    AliPCS request into the inner data structions.

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

    def __init__(
        self,
        refresh_token: str,
        access_token: str = "",
        token_type: str = "",
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
        self._alipcs = AliPCS(
            refresh_token,
            access_token=access_token,
            token_type=token_type,
            expire_time=expire_time,
            user_id=user_id,
            user_name=user_name,
            nick_name=nick_name,
            device_id=device_id,
            default_drive_id=default_drive_id,
            role=role,
            status=status,
            error_max_retries=error_max_retries,
            max_keepalive_connections=max_keepalive_connections,
            max_connections=max_connections,
            keepalive_expiry=keepalive_expiry,
            connection_max_retries=connection_max_retries,
        )

        # The path tree is for user's own files
        self._path_tree = PathTree(self)

        # Map `share_id` to a `PathTree` for that shared link
        self._shared_path_tree: DefaultDict[str, PathTree] = defaultdict(partial(PathTree, self))

    @property
    def refresh_token(self) -> str:
        return self._alipcs.refresh_token

    @property
    def access_token(self) -> str:
        return self._alipcs.access_token

    @property
    def token_type(self) -> str:
        return self._alipcs.token_type

    @property
    def expire_time(self) -> int:
        return self._alipcs.expire_time

    @property
    def user_id(self) -> str:
        return self._alipcs.user_id

    @property
    def user_name(self) -> str:
        return self._alipcs.user_name

    @property
    def nick_name(self) -> str:
        return self._alipcs.nick_name

    @property
    def device_id(self) -> str:
        return self._alipcs.device_id

    @property
    def default_drive_id(self) -> str:
        return self._alipcs.default_drive_id

    @property
    def role(self) -> str:
        return self._alipcs.role

    @property
    def status(self) -> str:
        return self._alipcs.status

    def path_traceback(self, file_id: str, share_id: Optional[str] = None) -> List[PcsFile]:
        """Traceback the path of the file

        Return the list of all `PcsFile`s from the file to the top level directory.

        Important:
            The `path` property of the returned `PcsFile` has absolute path.
        """

        try:
            info = self._alipcs.path_traceback(file_id, share_id=share_id)
        except AliPCSError as err:
            if err.error_code == "NotFound.File":
                return []
            raise

        pcs_files = []
        for item_info in info["items"][::-1]:
            pcs_file = PcsFile.from_(item_info)
            pcs_file.path = join_path(pcs_files[-1].path if pcs_files else "/", pcs_file.name)
            pcs_files.append(pcs_file)

        pcs_files.reverse()
        return pcs_files

    def meta_by_path(self, remotepath: str) -> Optional[PcsFile]:
        """Get the meta of the the path

        Can not get the shared files' meta info by their paths.

        Important:
            The `path` property of the returned `PcsFile` is the argument `remotepath`.
        """

        assert remotepath.startswith("/"), "Path should start with '/'"

        if remotepath == "/":
            return PcsFile.root()

        try:
            info = self._alipcs.meta_by_path(remotepath)
        except AliPCSError as err:
            if err.error_code == "NotFound.File":
                return None
            raise

        pcs_file = PcsFile.from_(info)
        pcs_file.path = remotepath
        return pcs_file

    def meta(self, file_id: str, share_id: Optional[str] = None) -> Optional[PcsFile]:
        """Get meta info of the file

        Important:
            The `path` property of the returned `PcsFile` is only the name of the file.
        """

        try:
            info = self._alipcs.meta(file_id, share_id=share_id)
        except AliPCSError as err:
            if err.error_code == "NotFound.File":
                return None
            raise

        return PcsFile.from_(info)

    def get_file(
        self, *, remotepath: str = "", file_id: str = "", share_id: Optional[str] = None
    ) -> Optional[PcsFile]:
        """Get the file's info by the given `remotepath` or `file_id`

        If the `remotepath` is given, the `file_id` will be ignored.

        Important:
            If the `remotepath` is given, the `path` property of the returned `PcsFile` is the `remotepath`.
            If the `file_id` is given, the `path` property of the returned `PcsFile` is only the name of the file.
        """

        if remotepath:
            if share_id:
                return self.path(remotepath, share_id=share_id)
            else:
                return self.meta_by_path(remotepath)
        elif file_id:
            return self.meta(file_id, share_id=share_id)
        else:
            raise ValueError("One of `remotepath` and `file_id` must be given")

    def exists(self, file_id: str) -> bool:
        """Check whether the file exists

        Return True if the file exists and does not in the trash else False.
        """

        return self._alipcs.exists(file_id)

    def exists_in_trash(self, file_id: str) -> bool:
        """Check whether the file exists in the trash

        Return True if the file exists in the trash else False.
        """

        return self._alipcs.exists_in_trash(file_id)

    def is_file(self, file_id: str) -> bool:
        """Check whether `file_id` is a file"""

        return self._alipcs.is_file(file_id)

    def is_dir(self, file_id: str) -> bool:
        """Check whether `file_id` is a directory"""

        return self._alipcs.is_dir(file_id)

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
    ) -> Tuple[List[PcsFile], str]:
        """List the directory's contents

        List files and directories in the given directory (which has the `file_id`).
        The return items size is limited by the `limit` parameter. If you want to list
        more, using the returned `next_marker` parameter for next `list` call.

        Args:
            file_id (str): The directory's file id.
            share_id (str): The share id if the file_id is in shared link.
            desc (bool): Descending order by time.
            name (bool): Order by name.
            time (bool): Order by time.
            size (bool): Order by size.
            all (bool): Unknown, just for the request.
            limit (int): The number of items to return.
            url_expire_sec (int): The download url's expire time.
            next_marker (str): The next marker for next list call.

        Returns:
            Tuple[List[PcsFile], str]: The list of `PcsFile` and the next marker.

        Important:
            These PcsFile instances' path property is only the name of the file.
        """

        info = self._alipcs.list(
            file_id=file_id,
            share_id=share_id,
            desc=desc,
            name=name,
            time=time,
            size=size,
            all=all,
            limit=limit,
            url_expire_sec=url_expire_sec,
            next_marker=next_marker,
        )
        next_marker = info["next_marker"]
        pcs_files = []
        for v in info.get("items", []):
            pcs_files.append(PcsFile.from_(v))
        return pcs_files, next_marker

    def list_iter(
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
        recursive: bool = False,
        include_dir: bool = True,
    ) -> Iterable[PcsFile]:
        """Iterate the directory by its `file_id`

        Iterate all files and directories at the directory (which has the `file_id`).

        Args:
            file_id (str): The directory's file id.
            share_id (str): The share id if the file_id is in shared link.
            desc (bool): Descending order by time.
            name (bool): Order by name.
            time (bool): Order by time.
            size (bool): Order by size.
            all (bool): Unknown, just for the request.
            limit (int): The number of one request queries.
            url_expire_sec (int): The download url's expire time.
            recursive (bool): Recursively iterate the directory.
            include_dir (bool): Include directory in the result.

        Returns:
            Iterable[PcsFile]: The iterator of `PcsFile`.

        Important:
            These PcsFile instances' path property is the path from the first sub-directory of the `file_id` to the file name.
            e.g.
                If the directory (owned `file_id`) has path `level0/`, a sub-directory which of path is
                `level0/level1/level2` then its corresponding PcsFile.path is `level1/level2`.
        """

        next_marker = ""
        while True:
            pcs_files, next_marker = self.list(
                file_id,
                share_id=share_id,
                desc=desc,
                name=name,
                time=time,
                size=size,
                all=all,
                limit=limit,
                url_expire_sec=url_expire_sec,
                next_marker=next_marker,
            )
            for pf in pcs_files:
                if pf.is_file:
                    yield pf
                else:
                    if include_dir:
                        # The upper recursive call will change the `pf.path`.
                        # So, we need to deepcopy it.
                        yield deepcopy(pf)
                    if recursive:
                        for sub_pf in self.list_iter(
                            pf.file_id,
                            share_id=share_id,
                            desc=desc,
                            name=name,
                            time=time,
                            size=size,
                            all=all,
                            limit=limit,
                            url_expire_sec=url_expire_sec,
                            recursive=recursive,
                            include_dir=include_dir,
                        ):
                            sub_pf.path = join_path(pf.path, sub_pf.path)
                            if sub_pf.is_file:
                                yield sub_pf
                            else:
                                # The upper recursive call will change the `pf.path`.
                                # So, we need to deepcopy it.
                                yield deepcopy(sub_pf)

            if not next_marker:
                return

    def path(self, remotepath: str, share_id: Optional[str] = None) -> Optional[PcsFile]:
        """Get the pcs file's info by the given absolute `remotepath`

        Important:
            The `path` property of the returned `PcsFile` is the argument `remotepath`.
        """

        assert remotepath.startswith("/"), "`remotepath` should start with '/'"

        if share_id:
            return self._shared_path_tree[share_id].search(remotepath=remotepath, share_id=share_id)
        else:
            return self._path_tree.search(remotepath=remotepath)

    def paths(self, *remotepaths: str, share_id: Optional[str] = None) -> List[Optional[PcsFile]]:
        """Get the pcs files' info by the given absolute `remotepaths`

        Important:
            The `path` property of the returned `PcsFile` is the argument `remotepath`.
        """

        return [self.path(remote_path, share_id=share_id) for remote_path in remotepaths]

    def walk(
        self,
        file_id: str,
        share_id: str = "",
        all: bool = False,
        limit: int = 200,
        url_expire_sec: int = 14400,
    ) -> Iterable[PcsFile]:
        """Recursively Walk through the directory tree which has `file_id`

        Args:
            file_id (str): The directory's file id.
            share_id (str): The share id if the file_id is in shared link.
            all (bool): Unknown, just for the request.
            limit (int): The number of one request queries.
            url_expire_sec (int): The download url's expire time.
            include_dir (bool): Include directory in the result.

        Returns:
            Iterable[PcsFile]: The iterator of `PcsFile`.

        Important:
            These PcsFile instances' path property is the path from the first sub-directory of the `file_id` to the file.
            e.g.
                If the directory (owned `file_id`) has path `level0/`, a sub-directory which of path is
                `level0/level1/level2` then its corresponding PcsFile.path is `level1/level2`.

        """

        file_id_to_path = {file_id: ""}
        next_marker = ""
        while True:
            info = self._alipcs.walk(
                file_id,
                share_id=share_id,
                all=all,
                limit=limit,
                url_expire_sec=url_expire_sec,
                next_marker=next_marker,
            )
            for v in info["items"]:
                pcs_file = PcsFile.from_(v)
                pcs_file.path = join_path(file_id_to_path[pcs_file.parent_file_id], pcs_file.name)
                file_id_to_path[pcs_file.file_id] = pcs_file.path
                yield pcs_file

            next_marker = info["next_marker"]
            if not next_marker:
                return

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
    ) -> PcsPreparedFile:
        """Create a prepared file for uploading

        filename (str):
            The name of file.
        dir_id (str):
            The directory id where the file is at.
        size (int):
            the length of total content.
        pre_hash (str):
            The sha1 of the IO first 1k bytes.
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

        info = self._alipcs.create_file(
            filename,
            dir_id,
            size,
            pre_hash=pre_hash,
            content_hash=content_hash,
            part_number=part_number,
            proof_code=proof_code,
            check_name_mode=check_name_mode,
        )
        return PcsPreparedFile.from_(info)

    def prepare_file(
        self,
        filename: str,
        dir_id: str,
        size: int,
        pre_hash: str,
        part_number: int = 1,
        check_name_mode: CheckNameMode = "auto_rename",
    ) -> PcsPreparedFile:
        """Create a prepared file with `pre_hash` for uploading"""

        return self.create_file(
            filename, dir_id, size, pre_hash=pre_hash, part_number=part_number, check_name_mode=check_name_mode
        )

    def get_upload_url(self, upload_id: str, file_id: str, part_number: int = 1) -> PcsPreparedFile:
        """Get upload slices' urls

        It is useful to get new upload slice url when these urls gotten from
        `AliPCS.prepare_file` or `AliPCS.create_file` are expired.
        """

        info = self._alipcs.get_upload_url(upload_id, file_id, part_number)
        return PcsPreparedFile.from_(info)

    def rapid_upload_file(
        self,
        filename: str,
        dir_id: str,
        size: int,
        content_hash: str,
        proof_code: str,
        check_name_mode: CheckNameMode = "auto_rename",
    ) -> PcsPreparedFile:
        return self.create_file(
            filename, dir_id, size, content_hash=content_hash, proof_code=proof_code, check_name_mode=check_name_mode
        )

    def upload_slice(
        self, io: IO, url: str, callback: Optional[Callable[[MultipartEncoderMonitor], None]] = None
    ) -> None:
        """Upload an io as a slice

        callable: the callback for monitoring uploading progress
        """

        self._alipcs.upload_slice(io, url, callback=callback)

    def upload_complete(self, file_id: str, upload_id: str) -> PcsFile:
        """Tell server that all slices have been uploaded

        After uploading all slices using `AliPCSApi.upload_slice`, we need to
        tell server that all slice have been uploaded. Then, the server will
        return the final file info back.
        """

        info = self._alipcs.upload_complete(file_id, upload_id)
        return PcsFile.from_(info)

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
    ) -> Tuple[List[PcsFile], str]:
        """Search with `keyword`"""

        info = self._alipcs.search(
            keyword, desc=desc, name=name, time=time, size=size, all=all, limit=limit, next_marker=next_marker
        )
        next_marker = info["next_marker"]
        return [PcsFile.from_(v) for v in info.get("items", [])], next_marker

    def search_all(
        self,
        keyword: str,
        desc: bool = False,
        name: bool = False,
        time: bool = False,
        size: bool = False,
        all: bool = False,
        limit: int = 100,
    ) -> List[PcsFile]:
        """Search with `keyword`"""

        next_marker = ""
        pcs_files = []
        while True:
            pfs, next_marker = self.search(
                keyword=keyword,
                desc=desc,
                name=name,
                time=time,
                size=size,
                all=all,
                limit=limit,
                next_marker=next_marker,
            )
            pcs_files.extend(pfs)

            if not next_marker:
                break

        return pcs_files

    def makedir(self, dir_id: str, name: str) -> PcsFile:
        """Make a directory in the `dir_id` directory

        Important:
            The `path` property of the returned `PcsFile` is only the name of the directory.
        """

        info = self._alipcs.makedir(dir_id, name)
        return PcsFile.from_(info)

    def makedir_path(self, remotedir: str) -> List[PcsFile]:
        """Make a directory by the absolute `remotedir` path

        Return the list of all `PcsFile`s from the directory to the top level directory.

        Important:
            The `path` property of the returned `PcsFile` has absolute path.
        """

        # Use lock to ignore make a directory twice
        with _ALI_PCS_API_LOCK:
            paths = []
            parts = split_posix_path(remotedir)
            parent = PcsFile.root()
            for i, part in enumerate(parts):
                if part == "/":  # ignore root
                    continue

                # Find the sub directory which has the name of `part`
                pf = PcsFile.root()
                for pf in self.list_iter(parent.file_id):
                    if pf.name == part:
                        break

                now_dir = join_path(*parts[: i + 1])
                if pf.name == part:
                    assert pf.is_dir, f"{now_dir} is a file"
                else:
                    pf = self.makedir(parent.file_id, part)

                pf.path = now_dir
                parent = pf
                paths.append(pf)

            paths.reverse()
            return paths

    def move(self, *file_ids: str) -> List[bool]:
        """Move `file_ids[:-1]` to `file_ids[-1]`"""

        info = self._alipcs.move(*file_ids)

        # Remove nodes from self._path_tree
        for file_id in file_ids[:-1]:
            self._path_tree.pop_by_file_id(file_id)

        return ["code" not in v["body"] for v in info["responses"]]

    def rename(self, file_id: str, name: str) -> PcsFile:
        """Rename the file with `file_id` to `name`

        Important:
            The `path` property of the returned `PcsFile` is only the name of the file.
        """

        info = self._alipcs.rename(file_id, name)

        # Remove node from self._path_tree
        self._path_tree.pop_by_file_id(file_id)

        return PcsFile.from_(info)

    def copy(self, *file_ids: str) -> List[PcsFile]:
        """Copy `file_ids[:-1]` to `file_ids[-1]`

        Important:
            The `path` property of the returned `PcsFile` is only the name of the file.
        """

        info = self._alipcs.copy(*file_ids)
        return [PcsFile.from_(v["body"]) for v in info["responses"]]

    def remove(self, *file_ids: str) -> List[bool]:
        """Remove all `file_ids`"""

        info = self._alipcs.remove(*file_ids)

        # Remove nodes from self._path_tree
        for file_id in file_ids:
            self._path_tree.pop_by_file_id(file_id)

        return ["code" not in v for v in info["responses"]]

    def share(self, *file_ids: str, password: str = "", period: int = 0, description: str = "") -> PcsSharedLink:
        """Share `file_ids` to public with a optional password

        Args:
            period (int): The days for expiring. `0` means no expiring
        """

        info = self._alipcs.share(*file_ids, password=password, period=period, description=description)
        return PcsSharedLink.from_(info)

    def is_shared_valid(self, share_id: str) -> bool:
        try:
            self.shared_info(share_id)
            return True
        except AliPCSError as err:
            if err.error_code in ("ShareLink.Forbidden", "ShareLink.Cancelled", "ShareLink.Expired"):
                return False
            raise

    def list_shared(self, next_marker: str = "") -> Tuple[List[PcsSharedLink], str]:
        """List shared link on a page"""

        info = self._alipcs.list_shared(next_marker=next_marker)
        return [PcsSharedLink.from_(v) for v in info["items"]], info["next_marker"]

    def list_shared_all(self) -> List[PcsSharedLink]:
        """List all shared links"""

        next_marker = ""
        pcs_shared_links = []
        while True:
            pslinks, next_marker = self.list_shared(next_marker=next_marker)
            pcs_shared_links.extend(pslinks)
            if not next_marker:
                return pcs_shared_links

    def cancel_shared(self, *share_ids: str) -> List[bool]:
        """Cancel shared links with their `share_ids`"""

        info = self._alipcs.cancel_shared(*share_ids)
        return ["code" not in v for v in info["responses"]]

    def get_share_token(self, share_id: str, share_password: str = "") -> str:
        """Get share token"""

        info = self._alipcs.get_share_token(share_id, share_password=share_password)
        return info["share_token"]

    def shared_info(self, share_id: str) -> PcsSharedLinkInfo:
        """Get shared link info by anyone"""

        while True:
            try:
                info = self._alipcs.shared_info(share_id)
                return PcsSharedLinkInfo.from_(info)
            except AliPCSError as err:
                # XXX: What is the error?
                if err.error_code == "ParamFlowException":
                    logger.debug("AliPCSApi.shared_info gets error: `ParamFlowException`")
                    continue
                else:
                    raise

    # list_shared_files is an alias of list
    list_shared_files = list

    def transfer_shared_files(
        self, shared_file_ids: List[str], dest_id: str, share_id: str, auto_rename: bool = False
    ) -> List[PcsFile]:
        """Save the `shared_file_ids` to `dest_id`"""

        info = self._alipcs.transfer_shared_files(shared_file_ids, dest_id, share_id, auto_rename=auto_rename)
        return [PcsFile.from_(v["body"]) for v in info.get("responses", [])]

    def shared_file_download_url(self, shared_file_id: str, share_id: str, expire_duration: int = 10 * 60) -> str:
        """Get shared file download link"""

        return self._alipcs.shared_file_download_url(shared_file_id, share_id, expire_duration=expire_duration)

    def user_info(self) -> PcsUser:
        """User's information"""

        info = self._alipcs.user_info()

        user_info = PcsUser.from_(info)
        user_info.web_refresh_token = self.refresh_token
        user_info.web_access_token = self.access_token
        user_info.web_token_type = self.token_type
        user_info.web_expire_time = self.expire_time
        user_info.device_id = self.device_id
        return user_info

    def download_link(self, file_id: str) -> Optional[PcsDownloadUrl]:
        """Get the download link of the `file_id`"""

        info = self._alipcs.download_link(file_id)
        return PcsDownloadUrl.from_(info)

    def update_download_url(self, pcs_file: PcsFile) -> PcsFile:
        """Update the download url of the `pcs_file` if it is expired

        Return a new `PcsFile` with the updated download url.
        """

        assert pcs_file.is_file, f"{pcs_file} is not a file"

        pcs_file = deepcopy(pcs_file)
        if pcs_file.download_url_expires():
            pcs_url = self.download_link(pcs_file.file_id)
            if pcs_url:
                pcs_file.download_url = pcs_url.url
        return pcs_file

    def file_stream(
        self,
        file_id: str,
        max_chunk_size: int = DEFAULT_MAX_CHUNK_SIZE,
        callback: Optional[Callable[..., None]] = None,
        encrypt_password: bytes = b"",
    ) -> Optional[RangeRequestIO]:
        """File stream as a normal io"""

        return self._alipcs.file_stream(
            file_id, max_chunk_size=max_chunk_size, callback=callback, encrypt_password=encrypt_password
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
        """Shared file stream as a normal io"""

        return self._alipcs.shared_file_stream(
            shared_file_id,
            share_id,
            expire_duration=expire_duration,
            max_chunk_size=max_chunk_size,
            callback=callback,
            encrypt_password=encrypt_password,
        )


class AliOpenPCSApi:
    """Alipan drive PCS Open Api

    This is the wrapper of `AliPCS`. It parses the content of response of raw
    AliPCS requests to some inner data structions.
    """

    def __init__(
        self,
        refresh_token: str,
        access_token: str = "",
        expire_time: int = 0,
        client_id: str = "",
        client_secret: str = "",
        client_server: str = "",
        user_id: str = "",
        user_name: str = "",
        nick_name: str = "",
        token_type: str = "",
        default_drive_id: str = "",
        role: str = "",
        status: str = "",
        error_max_retries: int = 2,
        max_keepalive_connections: int = 50,
        max_connections: int = 50,
        keepalive_expiry: float = 10 * 60,
        connection_max_retries: int = 2,
    ):
        self._aliopenpcs = AliOpenPCS(
            refresh_token,
            access_token=access_token,
            expire_time=expire_time,
            client_id=client_id,
            client_secret=client_secret,
            client_server=client_server,
            user_id=user_id,
            user_name=user_name,
            nick_name=nick_name,
            token_type=token_type,
            default_drive_id=default_drive_id,
            role=role,
            status=status,
            error_max_retries=error_max_retries,
            max_keepalive_connections=max_keepalive_connections,
            max_connections=max_connections,
            keepalive_expiry=keepalive_expiry,
            connection_max_retries=connection_max_retries,
        )

        # The path tree is for user's own files
        self._path_tree = PathTree(self)

        # Map `share_id` to a `PathTree` for that shared link
        self._shared_path_tree: DefaultDict[str, PathTree] = defaultdict(partial(PathTree, self))

    @property
    def refresh_token(self) -> str:
        return self._aliopenpcs.refresh_token

    @property
    def access_token(self) -> str:
        return self._aliopenpcs.access_token

    @property
    def token_type(self) -> str:
        return self._aliopenpcs.token_type

    @property
    def client_id(self) -> str:
        return self._aliopenpcs.client_id

    @property
    def client_secret(self) -> str:
        return self._aliopenpcs.client_secret

    @property
    def client_server(self) -> str:
        return self._aliopenpcs.client_server

    @property
    def user_id(self) -> str:
        return self._aliopenpcs.user_id

    @property
    def user_name(self) -> str:
        return self._aliopenpcs.user_name

    @property
    def nick_name(self) -> str:
        return self._aliopenpcs.nick_name

    @property
    def expire_time(self) -> int:
        return self._aliopenpcs.expire_time

    @property
    def default_drive_id(self) -> str:
        return self._aliopenpcs.default_drive_id

    @property
    def role(self) -> str:
        return self._aliopenpcs.role

    @property
    def status(self) -> str:
        return self._aliopenpcs.status

    def meta(self, file_id: str, share_id: Optional[str] = None) -> Optional[PcsFile]:
        """Get meta info of the file"""

        info = self._aliopenpcs.meta(file_id, share_id=share_id)
        if info.get("code") == "NotFound.File":
            return None
        return PcsFile.from_(info)

    def exists(self, file_id: str) -> bool:
        """Check whether `remotepath` exists"""

        return self._aliopenpcs.exists(file_id)

    def is_file(self, file_id: str) -> bool:
        """Check whether `remotepath` is a file"""

        return self._aliopenpcs.is_file(file_id)

    def is_dir(self, file_id: str) -> bool:
        """Check whether `remotepath` is a directory"""

        return self._aliopenpcs.is_dir(file_id)

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
    ) -> Tuple[List[PcsFile], str]:
        """List the directory's contents

        List files and directories in the given directory (which has the `file_id`).
        The return items size is limited by the `limit` parameter. If you want to list
        more, using the returned `next_marker` parameter for next `list` call.
        """

        info = self._aliopenpcs.list(
            file_id=file_id,
            share_id=share_id,
            desc=desc,
            name=name,
            time=time,
            size=size,
            all=all,
            limit=limit,
            url_expire_sec=url_expire_sec,
            next_marker=next_marker,
        )
        next_marker = info["next_marker"]
        return [PcsFile.from_(v) for v in info.get("items", [])], next_marker

    def list_iter(
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
        recursive: bool = False,
        include_dir: bool = True,
    ) -> Iterable[PcsFile]:
        """Iterate the directory by its `file_id`

        Iterate all files and directories at the directory (which has the `file_id`).
        """

        next_marker = ""
        pcs_file = self.meta(file_id, share_id=share_id)
        if pcs_file is None:
            return
        dirname = pcs_file.name
        while True:
            pcs_files, next_marker = self.list(
                file_id,
                share_id=share_id,
                desc=desc,
                name=name,
                time=time,
                size=size,
                all=all,
                limit=limit,
                url_expire_sec=url_expire_sec,
                next_marker=next_marker,
            )
            for pf in pcs_files:
                pf.path = join_path(dirname, pf.name)

                if pf.is_dir:
                    if include_dir:
                        yield pf
                    if recursive:
                        for sub_pf in self.list_iter(
                            pf.file_id,
                            share_id=share_id,
                            desc=desc,
                            name=name,
                            time=time,
                            size=size,
                            all=all,
                            limit=limit,
                            url_expire_sec=url_expire_sec,
                            recursive=recursive,
                            include_dir=include_dir,
                        ):
                            sub_pf.path = join_path(dirname, sub_pf.path)
                            yield sub_pf
                else:
                    yield pf

            if not next_marker:
                return

    def path(self, remotepath: str, share_id: Optional[str] = None) -> Optional[PcsFile]:
        """Get the pcs file's info by the given absolute `remotepath`"""

        if share_id:
            return self._shared_path_tree[share_id].search(remotepath=remotepath, share_id=share_id)
        else:
            return self._path_tree.search(remotepath=remotepath)

    def paths(self, *remotepaths: str, share_id: Optional[str] = None) -> List[Optional[PcsFile]]:
        """Get the pcs files' info by the given absolute `remotepaths`"""

        return [self.path(remote_path, share_id=share_id) for remote_path in remotepaths]

    def list_path_iter(
        self,
        remotepath: str,
        file_id: Optional[str] = None,
        share_id: Optional[str] = None,
        desc: bool = False,
        name: bool = False,
        time: bool = False,
        size: bool = False,
        all: bool = False,
        limit: int = 200,
        url_expire_sec: int = 14400,
        recursive: bool = False,
        include_dir: bool = True,
    ) -> Iterable[PcsFile]:
        """Iterate the `remotepath`"""

        if not file_id:
            pf = self.path(remotepath, share_id=share_id)
            if not pf:
                return
            file_id = pf.file_id

        dirname = posix_path_dirname(remotepath)

        for p in self.list_iter(
            file_id,
            share_id=share_id,
            desc=desc,
            name=name,
            time=time,
            size=size,
            all=all,
            limit=limit,
            url_expire_sec=url_expire_sec,
            recursive=recursive,
            include_dir=include_dir,
        ):
            p.path = join_path(dirname, p.path)
            yield p

    def list_path(
        self,
        remotepath: str,
        file_id: Optional[str] = None,
        share_id: Optional[str] = None,
        desc: bool = False,
        name: bool = False,
        time: bool = False,
        size: bool = False,
        all: bool = False,
        limit: int = 200,
        url_expire_sec: int = 14400,
    ) -> List[PcsFile]:
        return list(
            self.list_path_iter(
                remotepath,
                file_id=file_id,
                share_id=share_id,
                desc=desc,
                name=name,
                time=time,
                size=size,
                all=all,
                limit=limit,
                url_expire_sec=url_expire_sec,
            )
        )

    def download_link(self, file_id: str) -> Optional[PcsDownloadUrl]:
        """Get the download link of the `file_id`"""

        info = self._aliopenpcs.download_link(file_id)
        return PcsDownloadUrl.from_(info)

    def update_download_url(self, pcs_file: PcsFile) -> PcsFile:
        """Update the download url of the `pcs_file` if it is expired

        Return a new `PcsFile` with the updated download url.
        """

        assert pcs_file.is_file, f"{pcs_file} is not a file"

        pcs_file = deepcopy(pcs_file)
        if pcs_file.download_url_expires():
            pcs_url = self.download_link(pcs_file.file_id)
            if pcs_url:
                pcs_file.download_url = pcs_url.url
        return pcs_file

    def file_stream(
        self,
        file_id: str,
        max_chunk_size: int = DEFAULT_MAX_CHUNK_SIZE,
        callback: Optional[Callable[..., None]] = None,
        encrypt_password: bytes = b"",
    ) -> Optional[RangeRequestIO]:
        """File stream as a normal io"""

        return self._aliopenpcs.file_stream(
            file_id, max_chunk_size=max_chunk_size, callback=callback, encrypt_password=encrypt_password
        )


class AliPCSApiMix(AliPCSApi):
    """The class mixed with `AliPCSApi` and `AliOpenPCSApi`

    Only following methods are used from AliOpenPCSApi:
        - download_link
        - update_download_url
        - file_stream
    Other methods are used from AliPCSApi.

    Args:
        web_refresh_token (str): The refresh token from browser.
        web_access_token (str, optional): The access token from browser.
        web_token_type (str): The token type. Default is "Bearer".
        web_expire_time (int, optional): The expire time of the token.

        openapi_refresh_token (str): The refresh token from alipan openapi.
        openapi_access_token (str, optional): The access token from alipan openai.
        openapi_token_type (str): The token type. Default is "Bearer".
        openapi_expire_time (int, optional): The expire time of the token.
        client_id (str, optional): The client id of the app for openapi.
        client_secret (str, optional): The client secret of the app for openapi.
        client_server (str, optional): The client server of the app for openapi to access token.
            If `client_id` and `client_secret` are provided, the `client_server` is not needed, vice versa.

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

    def __init__(
        self,
        web_refresh_token: str,
        web_access_token: str = "",
        web_token_type: str = "",
        web_expire_time: int = 0,
        openapi_refresh_token: str = "",
        openapi_access_token: str = "",
        openapi_token_type: str = "",
        openapi_expire_time: int = 0,
        client_id: str = "",
        client_secret: str = "",
        client_server: str = "",
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
        super().__init__(
            refresh_token=web_refresh_token,
            access_token=web_access_token,
            token_type=web_token_type,
            expire_time=web_expire_time,
            user_id=user_id,
            user_name=user_name,
            nick_name=nick_name,
            device_id=device_id,
            default_drive_id=default_drive_id,
            role=role,
            status=status,
            error_max_retries=error_max_retries,
            max_keepalive_connections=max_keepalive_connections,
            max_connections=max_connections,
            keepalive_expiry=keepalive_expiry,
            connection_max_retries=connection_max_retries,
        )

        self._aliopenpcsapi: Optional[AliOpenPCSApi] = None
        if openapi_refresh_token and ((client_id and client_secret) or client_server):
            self._aliopenpcsapi = AliOpenPCSApi(
                refresh_token=openapi_refresh_token,
                access_token=openapi_access_token,
                token_type=openapi_token_type,
                expire_time=openapi_expire_time,
                client_id=client_id,
                client_secret=client_secret,
                client_server=client_server,
                user_id=user_id,
                user_name=user_name,
                nick_name=nick_name,
                default_drive_id=default_drive_id,
                role=role,
                status=status,
                error_max_retries=error_max_retries,
                max_keepalive_connections=max_keepalive_connections,
                max_connections=max_connections,
                keepalive_expiry=keepalive_expiry,
                connection_max_retries=connection_max_retries,
            )

    def download_link(self, file_id: str) -> Optional[PcsDownloadUrl]:
        """Get the download link of the `file_id`"""

        if self._aliopenpcsapi is not None:
            return self._aliopenpcsapi.download_link(file_id)
        else:
            return super().download_link(file_id)

    def update_download_url(self, pcs_file: PcsFile) -> PcsFile:
        """Update the download url of the `pcs_file` if it is expired

        Return a new `PcsFile` with the updated download url.
        """

        assert pcs_file.is_file, f"{pcs_file} is not a file"

        if self._aliopenpcsapi is not None:
            return self._aliopenpcsapi.update_download_url(pcs_file)
        else:
            return super().update_download_url(pcs_file)

    def file_stream(
        self,
        file_id: str,
        max_chunk_size: int = DEFAULT_MAX_CHUNK_SIZE,
        callback: Optional[Callable[..., None]] = None,
        encrypt_password: bytes = b"",
    ) -> Optional[RangeRequestIO]:
        """File stream as a normal io"""

        if self._aliopenpcsapi is not None:
            return self._aliopenpcsapi.file_stream(
                file_id, max_chunk_size=max_chunk_size, callback=callback, encrypt_password=encrypt_password
            )
        else:
            return super().file_stream(
                file_id, max_chunk_size=max_chunk_size, callback=callback, encrypt_password=encrypt_password
            )

    def user_info(self) -> PcsUser:
        pcs_user = super().user_info()

        if self._aliopenpcsapi:
            pcs_user.openapi_refresh_token = self._aliopenpcsapi.refresh_token
            pcs_user.openapi_access_token = self._aliopenpcsapi.access_token
            pcs_user.openapi_token_type = self._aliopenpcsapi.token_type
            pcs_user.openapi_expire_time = self._aliopenpcsapi.expire_time

            pcs_user.client_id = self._aliopenpcsapi.client_id
            pcs_user.client_secret = self._aliopenpcsapi.client_secret
            pcs_user.client_server = self._aliopenpcsapi.client_server

        return pcs_user


class _Node:
    def __init__(self, file_id: str, pcs_file: PcsFile):
        self.file_id = file_id
        self.pcs_file = pcs_file

        # Map path basename to node
        self.sub_nodes: Dict[str, _Node] = {}

    def _str(self, level: int) -> str:
        s = f"<_Node {self.pcs_file.name}>\n"
        for n in self.sub_nodes.values():
            s = s + " " * ((level + 1) * 2) + n._str(level + 1)
            n._str(level + 1)
        return s

    def __str__(self) -> str:
        return self._str(0)

    def add(self, node: "_Node") -> None:
        """Add a sub node"""

        self.sub_nodes[node.pcs_file.name] = node

    def pop(self, key: str) -> Optional["_Node"]:
        """Pop a sub node which has basename as `key`"""

        try:
            return self.sub_nodes.pop(key)
        except KeyError:
            return None


_PATH_TREE_LOCK = Lock()


class PathTree:
    """Path Tree

    Invite a file or directory by its path is not supported by aliyundrive.
    This class is aim to support to invite a file or directory by its path.
    """

    def __init__(self, api: Union[AliPCSApi, AliOpenPCSApi]):
        self._api = api
        self.root = _Node("root", PcsFile.root())
        self._file_id_to_node: Dict[str, _Node] = {}
        self._file_id_to_node["root"] = self.root

    def __str__(self) -> str:
        return f"<PathTree {self.root} >"

    def _add(self, node: _Node):
        """Add a node to self._file_id_to_node"""

        self._file_id_to_node[node.file_id] = node

    def _pop(self, file_id: str) -> Optional[_Node]:
        """Pop a node from self._file_id_to_node"""

        return self._file_id_to_node.pop(file_id, None)

    def search(
        self, remotepath: str = "", topdown: Iterable[str] = [], share_id: Optional[str] = None
    ) -> Optional[PcsFile]:
        """Search the PcsFile which has remote path as `remotepath`
        or has the tree path `topdown`
        """

        if not topdown:
            assert remotepath.startswith("/")
            topdown = split_posix_path(remotepath)

        node = self._dfs(list(topdown), self.root, share_id=share_id)
        if node:
            return deepcopy(node.pcs_file)
        else:
            return None

    def _dfs(
        self, topdown: List[str], root: _Node, pull: bool = True, share_id: Optional[str] = None
    ) -> Optional[_Node]:
        """Search a node with the path `topdown` using depth first search"""

        if not topdown:
            return root

        next_key = topdown[0]
        if next_key == "/":
            return self._dfs(topdown[1:], self.root, share_id=share_id)

        root_pcs_file = root.pcs_file

        # Add lock
        with _PATH_TREE_LOCK:
            if pull and next_key not in root.sub_nodes:
                for pf in self._api.list_iter(root_pcs_file.file_id, share_id=share_id):
                    if pf.name not in root.sub_nodes:
                        pf.path = join_path(root_pcs_file.path, pf.name)
                        new_node = _Node(pf.file_id, pf)
                        root.add(new_node)
                        self._add(new_node)
                    if pf.name == next_key:
                        break

            if next_key not in root.sub_nodes:
                return None

        return self._dfs(topdown[1:], root.sub_nodes[next_key], share_id=share_id)

    def pop(self, remotepath: str = "", topdown: Iterable[str] = []) -> Optional[PcsFile]:
        """Pop a node which is at the path of `remotepath` or `topdown`"""

        if not topdown:
            assert remotepath.startswith("/")
            topdown = split_posix_path(remotepath)

        parts = list(topdown)
        dest = parts[-1]
        parent = parts[:-1]
        assert len(parent) > 0, "Can not pop root"

        node = self._dfs(list(parent), self.root, pull=False)
        if node:
            if dest in node.sub_nodes:
                sub_node = node.pop(dest)
                if sub_node:
                    self._pop(sub_node.file_id)
                    return deepcopy(sub_node.pcs_file)
        return None

    def pop_by_file_id(self, file_id: str) -> Optional[PcsFile]:
        node = self._file_id_to_node.get(file_id)
        if not node:
            return None

        return self.pop(node.pcs_file.path)
