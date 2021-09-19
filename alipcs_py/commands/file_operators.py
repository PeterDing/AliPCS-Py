from typing import Optional, List

from alipcs_py.alipcs import AliPCSApi, AliPCSError
from alipcs_py.alipcs.inner import PcsFile, FromTo
from alipcs_py.common.path import (
    posix_path_dirname,
    split_posix_path,
    posix_path_basename,
    join_path,
)
from alipcs_py.commands.display import display_files, display_from_to


def makedir(api: AliPCSApi, *remotedirs: str, show: bool = False) -> List[PcsFile]:
    pcs_files = []
    for d in remotedirs:
        pf = api.makedir_path(d)
        pcs_files.append(pf)

    if show:
        display_files(pcs_files, "", show_file_id=True, show_absolute_path=True)

    return pcs_files


def move(api: AliPCSApi, *remotepaths: str, show: bool = False):
    assert len(remotepaths) > 1

    dest_remotepath = remotepaths[-1]
    pcs_files = api.paths(*remotepaths)

    # Make sure destination directory exists
    if not pcs_files[-1]:
        dest_pcs_file = api.makedir_path(dest_remotepath)
        pcs_files[-1] = dest_pcs_file

    file_ids = [pf.file_id for pf in pcs_files if pf]
    oks = api.move(*file_ids)

    from_to_list = []
    j = 0
    for i, pf in enumerate(pcs_files[:-1]):
        if not pf:
            continue

        if oks[j]:
            from_to = FromTo(
                from_=remotepaths[i],
                to_=join_path(dest_remotepath, posix_path_basename(remotepaths[i])),
            )
            from_to_list.append(from_to)
        j += 1

    if show:
        display_from_to(*from_to_list)


def rename(api: AliPCSApi, remotepath: str, new_name: str, show: bool = False):
    pcs_file = api.path(remotepath)

    if not pcs_file:
        return

    api.rename(pcs_file.file_id, new_name)
    pcs_file.path = join_path(*split_posix_path(pcs_file.path)[:-1], new_name)

    if show:
        display_files([pcs_file], "", show_file_id=True, show_absolute_path=True)


def copy(api: AliPCSApi, *remotepaths: str, show: bool = False):
    assert len(remotepaths) > 1

    dest_remotepath = remotepaths[-1]
    pcs_files = api.paths(*remotepaths)

    # Make sure destination directory exists
    if not pcs_files[-1]:
        dest_pcs_file = api.makedir_path(dest_remotepath)
        pcs_files[-1] = dest_pcs_file

    file_ids = [pf.file_id for pf in pcs_files if pf]

    new_pfs = api.copy(*file_ids)

    if show:
        display_files(new_pfs, "", show_file_id=True, show_absolute_path=True)


def remove(api: AliPCSApi, *remotepaths: str):
    pcs_files = api.paths(*remotepaths)
    file_ids = [pf.file_id for pf in pcs_files if pf]
    api.remove(*file_ids)
