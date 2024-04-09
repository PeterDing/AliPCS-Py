from typing import List, Dict, Set, Union
from pathlib import PurePosixPath
from collections import deque
import re

from alipcs_py.alipcs import AliPCSApi, PcsFile
from alipcs_py.commands.list_files import list_files
from alipcs_py.commands.sifter import Sifter
from alipcs_py.commands.display import (
    display_invalid_shared_link_infos,
    display_shared_files,
    display_shared_link_infos,
    display_shared_links,
)
from alipcs_py.commands.download import (
    DEFAULT_CHUNK_SIZE,
    DEFAULT_CONCURRENCY,
    download,
    Downloader,
    DEFAULT_DOWNLOADER,
)
from alipcs_py.commands.play import play, Player, DEFAULT_PLAYER

import requests  # type: ignore

from rich import print


def share_files(api: AliPCSApi, *remotepaths: str, password: str = "", period: int = 0):
    pcs_files = api.paths(*remotepaths)
    assert all(pcs_files)

    file_ids = [pf.file_id for pf in pcs_files if pf]

    shared_link = api.share(*file_ids, password=password, period=period)
    display_shared_links(shared_link)


def list_shared(api: AliPCSApi, show_all=True):
    pcs_shared_links = api.list_shared_all()
    if not pcs_shared_links:
        return

    display_shared_links(*pcs_shared_links)


def cancel_shared(api: AliPCSApi, *share_ids: str):
    api.cancel_shared(*share_ids)


def _redirect(url: str) -> str:
    if not url or "alywp.net" not in url:
        return url
    resp = requests.get(url, allow_redirects=False)
    return resp.headers.get("Location") or ""


def _extract_share_id(share_url: str) -> str:
    m = re.search(r"/s/(\w+)", share_url)
    return m.group(1) if m else ""


def _extract_file_id(share_url: str) -> str:
    m = re.search(r"/folder/(\w+)", share_url)
    return m.group(1) if m else ""


def save_shared_by_url(api: AliPCSApi, remotedir: str, share_url: str, password: str = ""):
    share_url = _redirect(share_url)
    share_id = _extract_share_id(share_url)
    file_id = _extract_file_id(share_url)
    file_ids = [file_id] if file_id else []

    assert share_id

    save_shared_by_file_ids(api, remotedir, share_id, file_ids, password=password)


def save_shared_by_file_ids(
    api: AliPCSApi,
    remotedir: str,
    share_id: str,
    file_ids: List[str],
    password: str = "",
):
    """Save shared files to the remote directory `remotedir`. Ignore existed files."""

    assert share_id

    api.get_share_token(share_id, share_password=password)

    file_ids = file_ids or ["root"]

    shared_pcs_files = deque()
    for file_id in file_ids:
        pf = api.get_file(file_id=file_id, share_id=share_id)
        if pf is not None:
            shared_pcs_files.append(pf)

    # Record the remote directory of each shared_file
    shared_file_id_to_remotedir: Dict[str, str] = {}
    for sp in shared_pcs_files:
        shared_file_id_to_remotedir[sp.file_id] = remotedir

    # Map the remote directory to its pcs_file
    remotedir_to_its_pcs_file: Dict[str, PcsFile] = {}

    while shared_pcs_files:
        shared_file = shared_pcs_files.popleft()
        remote_dir = shared_file_id_to_remotedir[shared_file.file_id]

        # Make sure remote directory exists
        if remote_dir not in remotedir_to_its_pcs_file:
            remotedir_to_its_pcs_file[remote_dir] = api.makedir_path(remote_dir)[0]
        dest_pcs_file = remotedir_to_its_pcs_file[remote_dir]

        if not shared_file.is_root() and not remotepath_exists(api, shared_file.name, dest_pcs_file.file_id):
            api.transfer_shared_files(
                [shared_file.file_id],
                dest_pcs_file.file_id,
                share_id,
                auto_rename=False,
            )
            print(f"save: `{shared_file.path}` to `{remote_dir}`")
        else:
            # Ignore existed file
            if shared_file.is_file:
                print(f"[yellow]WARNING[/]: `{shared_file.path}` has be in `{remote_dir}`")
                continue
            else:  # shared_file.is_dir
                sub_files = list(api.list_iter(shared_file.file_id, share_id=share_id))

                remote_dir = (PurePosixPath(remote_dir) / shared_file.name).as_posix()
                for sp in sub_files:
                    shared_file_id_to_remotedir[sp.file_id] = remote_dir
                shared_pcs_files.extendleft(sub_files[::-1])


def save_shared(
    api: AliPCSApi,
    remotedir: str,
    share_id: str = "",
    share_url: str = "",
    file_ids: List[str] = [],
    password: str = "",
):
    """Save shared files of the shared url to the remote directory `remotedir`."""

    assert remotedir.startswith("/"), "`remotedir` must be an absolute path"

    assert int(bool(share_id)) ^ int(bool(share_url)), "`share_id` and `share_url` only can be given one"

    if share_url:
        save_shared_by_url(api, remotedir, share_url, password=password)
    else:
        save_shared_by_file_ids(api, remotedir, share_id, file_ids, password=password)


def list_shared_files(
    api: AliPCSApi,
    *remotepaths: str,
    share_id: str = "",
    share_url: str = "",
    password: str = "",
    file_ids: List[str] = [],
    desc: bool = False,
    name: bool = False,
    time: bool = False,
    size: bool = False,
    all: bool = True,
    limit: int = 200,
    recursive: bool = False,
    sifters: List[Sifter] = [],
    highlight: bool = False,
    show_size: bool = False,
    show_date: bool = False,
    show_file_id: bool = False,
    show_hash: bool = False,
    show_absolute_path: bool = False,
    csv: bool = False,
):
    """List shared files in the shared url or shared id."""

    assert int(bool(share_id)) ^ int(bool(share_url)), "`share_id` and `share_url` only can be given one"

    share_url = _redirect(share_url)

    if share_url:
        share_id = _extract_share_id(share_url)
        if not file_ids:
            file_id = _extract_file_id(share_url)
            file_ids = [file_id] if file_id else []

    if not remotepaths and not file_ids:
        return

    assert share_id

    api.get_share_token(share_id, share_password=password)

    list_files(
        api,
        *remotepaths,
        file_ids=file_ids,
        share_id=share_id,
        desc=desc,
        name=name,
        time=time,
        size=size,
        all=all,
        limit=limit,
        recursive=recursive,
        sifters=sifters,
        highlight=highlight,
        show_size=show_size,
        show_date=show_date,
        show_file_id=show_file_id,
        show_hash=show_hash,
        show_absolute_path=show_absolute_path,
        csv=csv,
    )


def remotepath_exists(api: AliPCSApi, name: str, remote_file_id: str, _cache: Dict[str, Set[str]] = {}) -> bool:
    """Check if the `name` exists in the remote directory `remote_file_id`."""

    names = _cache.get(remote_file_id)
    if not names:
        names = set(sp.name for sp in api.list_iter(remote_file_id))
        _cache[remote_file_id] = names
    return name in names


def download_shared(
    api: AliPCSApi,
    remotepaths: List[Union[str, PcsFile]],
    file_ids: List[str],
    localdir: str,
    share_id: str = "",
    share_url: str = "",
    password: str = "",
    sifters: List[Sifter] = [],
    recursive: bool = False,
    from_index: int = 0,
    downloader: Downloader = DEFAULT_DOWNLOADER,
    concurrency: int = DEFAULT_CONCURRENCY,
    chunk_size: Union[str, int] = DEFAULT_CHUNK_SIZE,
    show_progress: bool = False,
    max_retries: int = 2,
    out_cmd: bool = False,
    encrypt_password: bytes = b"",
):
    """Download shared files in the shared url or shared id."""

    assert int(bool(share_id)) ^ int(bool(share_url)), "`share_id` and `share_url` only can be given one"

    share_url = _redirect(share_url)

    if share_url:
        share_id = _extract_share_id(share_url)
        if not file_ids:
            file_id = _extract_file_id(share_url)
            file_ids = [file_id] if file_id else []

    if not remotepaths and not file_ids:
        return

    assert share_id

    api.get_share_token(share_id, share_password=password)

    download(
        api,
        remotepaths,
        file_ids=file_ids,
        localdir=localdir,
        share_id=share_id,
        sifters=sifters,
        recursive=recursive,
        from_index=from_index,
        downloader=downloader,
        concurrency=concurrency,
        chunk_size=chunk_size,
        show_progress=show_progress,
        max_retries=max_retries,
        out_cmd=out_cmd,
        encrypt_password=encrypt_password,
    )


def play_shared(
    api: AliPCSApi,
    remotepaths: List[str],
    file_ids: List[str],
    share_id: str,
    share_url: str = "",
    password: str = "",
    sifters: List[Sifter] = [],
    recursive: bool = False,
    from_index: int = 0,
    player: Player = DEFAULT_PLAYER,
    player_params: List[str] = [],
    quiet: bool = False,
    shuffle: bool = False,
    ignore_ext: bool = False,
    out_cmd: bool = False,
    local_server: str = "",
):
    """Play shared files in the shared url or shared id."""

    assert int(bool(share_id)) ^ int(bool(share_url)), "`share_id` and `share_url` only can be given one"

    share_url = _redirect(share_url)

    if share_url:
        share_id = _extract_share_id(share_url)
        if not file_ids:
            file_id = _extract_file_id(share_url)
            file_ids = [file_id] if file_id else []

    if not remotepaths and not file_ids:
        return

    assert share_id

    api.get_share_token(share_id, share_password=password)

    play(
        api,
        remotepaths,
        file_ids=file_ids,
        share_id=share_id,
        sifters=sifters,
        recursive=recursive,
        from_index=from_index,
        player=player,
        player_params=player_params,
        quiet=quiet,
        shuffle=shuffle,
        ignore_ext=ignore_ext,
        out_cmd=out_cmd,
        local_server=local_server,
    )


def get_share_token(api: AliPCSApi, share_id: str, share_url: str = "", password: str = "") -> str:
    """Initiate a shared link (or id) and get the share token."""

    assert int(bool(share_id)) ^ int(bool(share_url)), "`share_id` and `share_url` only can be given one"

    share_url = _redirect(share_url)

    if share_url:
        share_id = _extract_share_id(share_url)

    assert share_id

    return api.get_share_token(share_id, share_password=password)
