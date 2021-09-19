from typing import List, Dict, Set
from pathlib import PurePosixPath
from collections import deque
import re

from alipcs_py.alipcs import AliPCSApi, PcsFile
from alipcs_py.commands.display import display_shared_links, display_shared_paths

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


def save_shared(
    api: AliPCSApi,
    remotedir: str,
    shared_url: str = "",
    share_id: str = "",
    password: str = "",
):
    assert remotedir.startswith("/"), "`remotedir` must be an absolute path"
    assert shared_url or share_id

    if not share_id:
        share_id = _extract_share_id(shared_url)

    share_token = api.get_share_token(share_id, share_password=password)

    shared_files_iter = api.list_path_iter(
        "/", share_id=share_id, share_token=share_token
    )

    shared_files = deque(shared_files_iter)

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

        if not remotepath_exists(api, shared_file.name, rd):
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


def list_shared_files(api: AliPCSApi, shared_url: str, password: str = ""):
    share_id = _extract_share_id(shared_url)
    share_token = api.get_share_token(share_id, share_password=password)

    shared_files_iter = api.list_path_iter(
        "/", share_id=share_id, share_token=share_token
    )

    all_shared_files: List[PcsFile] = []

    shared_files = deque(shared_files_iter)
    all_shared_files += shared_files

    while shared_files:
        shared_file = shared_files.popleft()

        if shared_file.is_dir:
            # Take all sub paths
            sub_files = list(
                api.list_path_iter(
                    shared_file.path,
                    file_id=shared_file.file_id,
                    share_id=share_id,
                    share_token=share_token,
                )
            )
            all_shared_files += sub_files
            shared_files.extendleft(sub_files[::-1])

    display_shared_paths(*all_shared_files)


def remotepath_exists(
    api: AliPCSApi, name: str, rd: str, _cache: Dict[str, Set[str]] = {}
) -> bool:
    names = _cache.get(rd)
    if not names:
        names = set([sp.name for sp in api.list_path_iter(rd)])
        _cache[rd] = names
    return name in names
