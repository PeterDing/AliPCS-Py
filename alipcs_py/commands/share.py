from typing import List, Dict, Set
from pathlib import PurePosixPath
from collections import deque
import re

from alipcs_py.alipcs import AliPCSApi, PcsFile
from alipcs_py.commands.list_files import list_files
from alipcs_py.commands.sifter import Sifter
from alipcs_py.commands.display import display_shared_links
from alipcs_py.commands.download import (
    download,
    Downloader,
    DEFAULT_DOWNLOADER,
    DownloadParams,
    DEFAULT_DOWNLOADPARAMS,
)
from alipcs_py.commands.play import play, Player, DEFAULT_PLAYER

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


def _extract_share_id(share_url: str) -> str:
    m = re.search(r"/s/(\w+)", share_url)
    return m.group(1) if m else ""


def _extract_file_id(share_url: str) -> str:
    m = re.search(r"/folder/(\w+)", share_url)
    return m.group(1) if m else ""


def save_shared_by_url(
    api: AliPCSApi, remotedir: str, share_url: str, password: str = ""
):
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
    assert share_id

    share_token = api.get_share_token(share_id, share_password=password)

    file_ids = file_ids or ["root"]

    sfs = api.meta(*file_ids, share_id=share_id, share_token=share_token)
    for sf in sfs:
        if not sf.path:
            sf.path = sf.name
    shared_files = deque(sfs)

    # Record the remotedir of each shared_file
    _remotedirs: Dict[str, str] = {}
    for sp in shared_files:
        _remotedirs[sp.file_id] = remotedir

    # Map the remotedir to its pcs_file
    dest_pcs_files: Dict[str, PcsFile] = {}

    while shared_files:
        shared_file = shared_files.popleft()
        rd = _remotedirs[shared_file.file_id]

        # Make sure remote dir exists
        if rd not in dest_pcs_files:
            dest_pcs_files[rd] = api.makedir_path(rd)
        dest_pcs_file = dest_pcs_files[rd]

        if not shared_file.is_root() and not remotepath_exists(
            api, shared_file.name, rd
        ):
            api.transfer_shared_files(
                [shared_file.file_id],
                dest_pcs_file.file_id,
                share_id,
                share_token,
                auto_rename=False,
            )
            print(f"save: `{shared_file.path}` to `{rd}`")
        else:
            # Ignore existed file
            if shared_file.is_file:
                print(f"[yellow]WARNING[/]: `{shared_file.path}` has be in `{rd}`")
                continue
            else:  # shared_file.is_dir
                sub_files = list(
                    api.list_path_iter(
                        shared_file.path,
                        file_id=shared_file.file_id,
                        share_id=share_id,
                        share_token=share_token,
                    )
                )

                rd = (PurePosixPath(rd) / shared_file.name).as_posix()
                for sp in sub_files:
                    _remotedirs[sp.file_id] = rd
                shared_files.extendleft(sub_files[::-1])


def save_shared(
    api: AliPCSApi,
    remotedir: str,
    share_id: str = "",
    share_url: str = "",
    file_ids: List[str] = [],
    password: str = "",
):
    assert remotedir.startswith("/"), "`remotedir` must be an absolute path"

    assert int(bool(share_id)) ^ int(
        bool(share_url)
    ), "`share_id` and `share_url` only can be given one"

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
    limit: int = 100,
    recursive: bool = False,
    sifters: List[Sifter] = [],
    highlight: bool = False,
    show_size: bool = False,
    show_date: bool = False,
    show_file_id: bool = False,
    show_absolute_path: bool = False,
    csv: bool = False,
):
    assert int(bool(share_id)) ^ int(
        bool(share_url)
    ), "`share_id` and `share_url` only can be given one"

    if share_url:
        share_id = _extract_share_id(share_url)
        if not file_ids:
            file_id = _extract_file_id(share_url)
            file_ids = [file_id] if file_id else []

    assert share_id

    share_token = api.get_share_token(share_id, share_password=password)

    list_files(
        api,
        *remotepaths,
        file_ids=file_ids,
        share_id=share_id,
        share_token=share_token,
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
        show_absolute_path=show_absolute_path,
        csv=csv,
    )


def remotepath_exists(
    api: AliPCSApi, name: str, rd: str, _cache: Dict[str, Set[str]] = {}
) -> bool:
    names = _cache.get(rd)
    if not names:
        names = set([sp.name for sp in api.list_path_iter(rd)])
        _cache[rd] = names
    return name in names


def download_shared(
    api: AliPCSApi,
    remotepaths: List[str],
    file_ids: List[str],
    localdir: str,
    share_id: str = "",
    share_url: str = "",
    password: str = "",
    sifters: List[Sifter] = [],
    recursive: bool = False,
    from_index: int = 0,
    downloader: Downloader = DEFAULT_DOWNLOADER,
    downloadparams: DownloadParams = DEFAULT_DOWNLOADPARAMS,
    out_cmd: bool = False,
    encrypt_password: bytes = b"",
):
    assert int(bool(share_id)) ^ int(
        bool(share_url)
    ), "`share_id` and `share_url` only can be given one"

    if share_url:
        share_id = _extract_share_id(share_url)
        if not file_ids:
            file_id = _extract_file_id(share_url)
            file_ids = [file_id] if file_id else ["root"]

    assert share_id

    share_token = api.get_share_token(share_id, share_password=password)

    download(
        api,
        remotepaths,
        file_ids=file_ids,
        localdir=localdir,
        share_id=share_id,
        share_token=share_token,
        sifters=sifters,
        recursive=recursive,
        from_index=from_index,
        downloader=downloader,
        downloadparams=downloadparams,
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
    assert int(bool(share_id)) ^ int(
        bool(share_url)
    ), "`share_id` and `share_url` only can be given one"

    if share_url:
        share_id = _extract_share_id(share_url)
        if not file_ids:
            file_id = _extract_file_id(share_url)
            file_ids = [file_id]

    assert share_id

    share_token = api.get_share_token(share_id, share_password=password)

    play(
        api,
        remotepaths,
        file_ids=file_ids,
        share_id=share_id,
        share_token=share_token,
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
