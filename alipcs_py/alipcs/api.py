from typing import (
    Optional,
    List,
    Tuple,
    Dict,
    DefaultDict,
    Iterable,
    Iterator,
    Callable,
    IO,
)
from threading import Lock
from collections import defaultdict
from copy import deepcopy
from functools import partial
from alipcs_py.alipcs.errors import AliPCSError

from alipcs_py.common.io import RangeRequestIO, DEFAULT_MAX_CHUNK_SIZE
from alipcs_py.alipcs.pcs import AliPCS
from alipcs_py.alipcs.inner import (
    PcsFile,
    PcsPreparedFile,
    PcsSharedLink,
    PcsSharedLinkInfo,
    PcsUser,
    PcsDownloadUrl,
)
from alipcs_py.common.path import split_posix_path, join_path, posix_path_dirname

from requests_toolbelt import MultipartEncoderMonitor

_LOCK = Lock()


class AliPCSApi:
    """Ali PCS Api

    This is the wrapper of `AliPCS`. It parses the content of response of raw
    AliPCS requests to some inner data structions.
    """

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
        self._alipcs = AliPCS(
            refresh_token,
            access_token=access_token,
            expire_time=expire_time,
            user_id=user_id,
            user_name=user_name,
            nick_name=nick_name,
            token_type=token_type,
            device_id=device_id,
            default_drive_id=default_drive_id,
            role=role,
            status=status,
        )

        # The path tree is for user's own files
        self._path_tree = PathTree(self)

        # Map `share_id` to a `PathTree` for that shared link
        self._shared_path_tree: DefaultDict[str, PathTree] = defaultdict(
            partial(PathTree, self)
        )

    @property
    def refresh_token(self) -> str:
        return self._alipcs.refresh_token

    @property
    def access_token(self) -> str:
        return self._alipcs.access_token

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
    def token_type(self) -> str:
        return self._alipcs.token_type

    @property
    def expire_time(self) -> int:
        return self._alipcs.expire_time

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

    def meta(self, *file_ids: str, share_id: str = None) -> List[PcsFile]:
        """Meta data of `remotepaths`"""

        pcs_files = [PcsFile.root() if fid == "root" else None for fid in file_ids]
        fids = [fid for fid in file_ids if fid != "root"]

        if fids:
            info = self._alipcs.meta(*fids, share_id=share_id)
            pfs = [PcsFile.from_(v.get("body")) for v in info["responses"]]
            j = 0
            for i in range(len(pcs_files)):
                if pcs_files[i] is None:
                    pcs_files[i] = pfs[j]
                    j += 1

        return [pf for pf in pcs_files if pf is not None]

    def exists(self, file_id: str) -> bool:
        """Check whether `remotepath` exists"""

        return self._alipcs.exists(file_id)

    def is_file(self, file_id: str) -> bool:
        """Check whether `remotepath` is a file"""

        return self._alipcs.is_file(file_id)

    def is_dir(self, file_id: str) -> bool:
        """Check whether `remotepath` is a directory"""

        return self._alipcs.is_dir(file_id)

    def list(
        self,
        file_id: str,
        share_id: str = None,
        desc: bool = False,
        name: bool = False,
        time: bool = False,
        size: bool = False,
        all: bool = False,
        limit: int = 100,
        url_expire_sec: int = 7200,
        next_marker: str = "",
    ) -> Tuple[List[PcsFile], str]:
        """List the directory's contents

        List files and directories in the given directory (which has the `file_id`).
        The return items size is limited by the `limit` parameter. If you want to list
        more, using the returned `next_marker` parameter for next `list` call.
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
        return [PcsFile.from_(v) for v in info.get("items", [])], next_marker

    def list_iter(
        self,
        file_id: str,
        share_id: str = None,
        desc: bool = False,
        name: bool = False,
        time: bool = False,
        size: bool = False,
        all: bool = False,
        limit: int = 100,
        url_expire_sec: int = 7200,
        recursive: bool = False,
        include_dir: bool = True,
    ) -> Iterator[PcsFile]:
        """Iterate the directory by its `file_id`

        Iterate all files and directories at the directory (which has the `file_id`).
        """

        next_marker = ""
        pcs_file = self.meta(file_id, share_id=share_id)[0]
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

    def path(self, remotepath: str, share_id: str = None) -> Optional[PcsFile]:
        """Get the pcs file's info by the given absolute `remotepath`"""

        if share_id:
            return self._shared_path_tree[share_id].search(
                remotepath=remotepath, share_id=share_id
            )
        else:
            return self._path_tree.search(remotepath=remotepath)

    def paths(self, *remotepaths: str, share_id: str = None) -> List[Optional[PcsFile]]:
        """Get the pcs files' info by the given absolute `remotepaths`"""

        return [self.path(rp, share_id=share_id) for rp in remotepaths]

    def list_path_iter(
        self,
        remotepath: str,
        file_id: str = None,
        share_id: str = None,
        desc: bool = False,
        name: bool = False,
        time: bool = False,
        size: bool = False,
        all: bool = False,
        limit: int = 100,
        url_expire_sec: int = 7200,
        recursive: bool = False,
        include_dir: bool = True,
    ) -> Iterator[PcsFile]:
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
        file_id: str = None,
        share_id: str = None,
        desc: bool = False,
        name: bool = False,
        time: bool = False,
        size: bool = False,
        all: bool = False,
        limit: int = 100,
        url_expire_sec: int = 7200,
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

    def create_file(
        self,
        filename: str,
        dir_id: str,
        size: int,
        pre_hash: str = "",
        content_hash: str = "",
        proof_code: str = "",
        check_name_mode: str = "auto_rename",
    ) -> PcsPreparedFile:
        info = self._alipcs.create_file(
            filename,
            dir_id,
            size,
            pre_hash=pre_hash,
            content_hash=content_hash,
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
        check_name_mode: str = "auto_rename",
    ) -> PcsPreparedFile:
        return self.create_file(
            filename, dir_id, size, pre_hash=pre_hash, check_name_mode=check_name_mode
        )

    def rapid_upload_file(
        self,
        filename: str,
        dir_id: str,
        size: int,
        content_hash: str,
        proof_code: str,
        check_name_mode: str = "auto_rename",
    ) -> PcsPreparedFile:
        return self.create_file(
            filename,
            dir_id,
            size,
            content_hash=content_hash,
            proof_code=proof_code,
            check_name_mode=check_name_mode,
        )

    def upload_slice(
        self,
        io: IO,
        url: str,
        callback: Callable[[MultipartEncoderMonitor], None] = None,
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
            keyword,
            desc=desc,
            name=name,
            time=time,
            size=size,
            all=all,
            limit=limit,
            next_marker=next_marker,
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
        info = self._alipcs.makedir(dir_id, name)
        return PcsFile.from_(info)

    def makedir_path(self, remotedir: str) -> PcsFile:
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
        return parent

    def move(self, *file_ids: str) -> List[bool]:
        """Move `file_ids[:-1]` to `file_ids[-1]`"""

        info = self._alipcs.move(*file_ids)

        # Remove nodes from self._path_tree
        for file_id in file_ids[:-1]:
            self._path_tree.pop_by_file_id(file_id)

        return ["code" not in v["body"] for v in info["responses"]]

    def rename(self, file_id: str, name: str) -> PcsFile:
        info = self._alipcs.rename(file_id, name)

        # Remove node from self._path_tree
        self._path_tree.pop_by_file_id(file_id)

        return PcsFile.from_(info)

    def copy(self, *file_ids: str) -> List[PcsFile]:
        """Copy `remotepaths[:-1]` to `remotepaths[-1]`"""

        info = self._alipcs.copy(*file_ids)
        return [PcsFile.from_(v["body"]) for v in info["responses"]]

    def remove(self, *file_ids: str) -> List[bool]:
        """Remove all `remotepaths`"""

        info = self._alipcs.remove(*file_ids)

        # Remove nodes from self._path_tree
        for file_id in file_ids[:-1]:
            self._path_tree.pop_by_file_id(file_id)

        return ["code" not in v for v in info["responses"]]

    def share(
        self, *file_ids: str, password: str = "", period: int = 0, description: str = ""
    ) -> PcsSharedLink:
        """Share `remotepaths` to public with a optional password

        Args:
            period (int): The days for expiring. `0` means no expiring
        """

        info = self._alipcs.share(
            *file_ids, password=password, period=period, description=description
        )
        return PcsSharedLink.from_(info)

    def is_shared_valid(self, share_id: str) -> bool:
        try:
            self.shared_info(share_id)
            return True
        except AliPCSError as err:
            if err.error_code in (
                "ShareLink.Forbidden",
                "ShareLink.Cancelled",
                "ShareLink.Expired",
            ):
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

        info = self._alipcs.shared_info(share_id)
        return PcsSharedLinkInfo.from_(info)

    # list_shared_files is an alias of list
    list_shared_files = list

    def transfer_shared_files(
        self,
        shared_file_ids: List[str],
        dest_id: str,
        share_id: str,
        auto_rename: bool = False,
    ) -> List[PcsFile]:
        """Save the `shared_file_ids` to `dest_id`"""

        info = self._alipcs.transfer_shared_files(
            shared_file_ids, dest_id, share_id, auto_rename=auto_rename
        )
        return [PcsFile.from_(v["body"]) for v in info.get("responses", [])]

    def shared_file_download_url(
        self, shared_file_id: str, share_id: str, expire_duration: int = 10 * 60
    ) -> str:
        """Get shared file download link"""

        return self._alipcs.shared_file_download_url(
            shared_file_id, share_id, expire_duration=expire_duration
        )

    def user_info(self) -> PcsUser:
        """User's information"""

        info = self._alipcs.user_info()

        user_info = PcsUser.from_(info)
        user_info.refresh_token = self.refresh_token
        user_info.access_token = self.access_token
        user_info.token_type = self.token_type
        user_info.expire_time = self.expire_time
        return user_info

    def download_link(self, file_id: str) -> Optional[PcsDownloadUrl]:
        """Get the download link of the `file_id`"""

        info = self._alipcs.download_link(file_id)
        return PcsDownloadUrl.from_(info)

    def file_stream(
        self,
        file_id: str,
        max_chunk_size: int = DEFAULT_MAX_CHUNK_SIZE,
        callback: Callable[..., None] = None,
        encrypt_password: bytes = b"",
    ) -> Optional[RangeRequestIO]:
        """File stream as a normal io"""

        return self._alipcs.file_stream(
            file_id,
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
        callback: Callable[..., None] = None,
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


class PathTree:
    def __init__(self, api: AliPCSApi):
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

        try:
            return self._file_id_to_node.pop(file_id)
        except KeyError:
            return None

    def search(
        self, remotepath: str = "", topdown: Iterable[str] = [], share_id: str = None
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
        self, topdown: List[str], root: _Node, pull: bool = True, share_id: str = None
    ) -> Optional[_Node]:
        """Search a node with the path `topdown` using depth first search"""

        if not topdown:
            return root

        next_key = topdown[0]
        if next_key == "/":
            return self._dfs(topdown[1:], self.root, share_id=share_id)

        root_pcs_file = root.pcs_file

        # Add lock
        with _LOCK:
            # sub_node = root.sub_nodes.get(next_key)
            # if sub_node:
            #     if not self._api.exists(sub_node.pcs_file.file_id):
            #         root.sub_nodes.pop(next_key)

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

    def pop(
        self, remotepath: str = "", topdown: Iterable[str] = []
    ) -> Optional[PcsFile]:
        """Pop a node which is at the path of `remotepath` or `topdown`"""

        if not topdown:
            assert remotepath.startswith("/")
            topdown = split_posix_path(remotepath)

        parts = list(topdown)
        dest = parts[-1]
        parent = parts[:-1]
        assert len(parent) > 0, "NO pop root"

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
