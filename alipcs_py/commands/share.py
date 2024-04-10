from typing import List, Dict, Set, Union
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


def extract_shared_info_from_url(share_url: str):
    share_url = _redirect(share_url)
    share_id = _extract_share_id(share_url)
    file_id = _extract_file_id(share_url)

    return share_id, file_id


def save_shared_files_to_remotedir(
    api: AliPCSApi, shared_pcs_files: List[PcsFile], share_id: str, remote_pcs_file: PcsFile
):
    """Save shared files to the remote directory `remotedir`. Ignore existed files."""

    wanted_pcs_files = []
    remain_pcs_files = []
    for sp in shared_pcs_files:
        if not remotepath_exists(api, sp.name, remote_pcs_file.file_id):
            wanted_pcs_files.append(sp)
        else:
            remain_pcs_files.append(sp)

    if wanted_pcs_files:
        api.transfer_shared_files(
            [sp.file_id for sp in wanted_pcs_files],
            remote_pcs_file.file_id,
            share_id,
            auto_rename=False,
        )
        for sp in wanted_pcs_files:
            print(f"save: `{sp.path}` to `{remote_pcs_file.path}`")

    for sp in remain_pcs_files:
        if sp.is_file:
            print(f"[yellow]WARNING[/]: `{sp.path}` has be in `{remote_pcs_file.path}`")
        else:
            remote_dir_pcs_file = api.get_file(remotepath=f"{remote_pcs_file.path}/{sp.name}")
            if remote_dir_pcs_file is None:
                remote_dir_pcs_file = api.makedir(remote_pcs_file.file_id, sp.name)

            sub_files = list(api.list_iter(sp.file_id, share_id=share_id, recursive=False, include_dir=True))
            save_shared_files_to_remotedir(api, sub_files, share_id, remote_dir_pcs_file)


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

    # 1. Get shared info
    if share_url:
        share_id, file_id = extract_shared_info_from_url(share_url)
        if file_id:
            file_ids.append(file_id)
        file_ids = list(set(file_ids))

        # Default save the sub files in root directory
        if not file_ids:
            file_ids = ["root"]
    assert share_id

    # 2. Get shared link token
    api.get_share_token(share_id, share_password=password)

    # 3. Make sure the remote directory exists
    remote_pcs_file = api.get_file(remotepath=remotedir)
    if remote_pcs_file is not None:
        if not remote_pcs_file.is_dir:
            print(f"[yellow]WARNING[/]: `{remotedir}` is not a directory")
            return
    else:
        remote_pcs_file = api.makedir_path(remotedir)[0]

    # 4. Get shared files and save them to the remote directory
    shared_pcs_files = []
    for file_id in file_ids:
        shared_pcs_file = api.get_file(file_id=file_id, share_id=share_id)
        if shared_pcs_file is not None:
            if shared_pcs_file.is_root():
                # No need to save root directory, save its sub files/directories
                shared_pcs_files.extend(api.list_iter(file_id, share_id=share_id, recursive=False, include_dir=True))
            else:
                shared_pcs_files.append(shared_pcs_file)

    save_shared_files_to_remotedir(api, shared_pcs_files, share_id, remote_pcs_file)


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

    if share_url:
        share_id, file_id = extract_shared_info_from_url(share_url)
        if file_id:
            file_ids.append(file_id)
            file_ids = list(set(file_ids))
    assert share_id

    if not remotepaths and not file_ids:
        return

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

    if share_url:
        share_id, file_id = extract_shared_info_from_url(share_url)
        if file_id:
            file_ids.append(file_id)
            file_ids = list(set(file_ids))
    assert share_id

    if not remotepaths and not file_ids:
        return

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

    if share_url:
        share_id, file_id = extract_shared_info_from_url(share_url)
        if file_id:
            file_ids.append(file_id)
            file_ids = list(set(file_ids))
    assert share_id

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

    share_id, _ = extract_shared_info_from_url(share_url)
    assert share_id

    return api.get_share_token(share_id, share_password=password)
