from typing import List

from alipcs_py.alipcs import AliPCSApi
from alipcs_py.alipcs.inner import PcsFile, FromTo
from alipcs_py.common.path import (
    split_posix_path,
    posix_path_basename,
    join_path,
)
from alipcs_py.commands.display import display_files, display_from_to


def makedir(api: AliPCSApi, *remotedirs: str, show: bool = False) -> List[PcsFile]:
    """Make directories (`remotepaths`)."""

    pcs_files = []
    for d in remotedirs:
        pf = api.makedir_path(d)[0]
        pcs_files.append(pf)

    if show:
        display_files(pcs_files, "", show_file_id=True, show_absolute_path=True)

    return pcs_files


def move(api: AliPCSApi, *remotepaths: str, show: bool = False):
    """Move files or directories to a destination directory.

    Move the paths(`remotepaths[:-1]`) to the directory(`remotepaths[-1]`).
    """

    assert len(remotepaths) > 1

    dest_remotepath = remotepaths[-1]
    pcs_files = api.paths(*remotepaths)

    # Make sure destination directory exists
    if not pcs_files[-1]:
        dest_pcs_file = api.makedir_path(dest_remotepath)[0]
        pcs_files[-1] = dest_pcs_file

    file_ids = [pf.file_id for pf in pcs_files if pf]
    oks = api.move(*file_ids)

    from_to_list: List[FromTo[str, str]] = []
    j = 0
    for i, pf in enumerate(pcs_files[:-1]):
        if not pf:
            continue

        if oks[j]:
            from_to = (
                remotepaths[i],
                join_path(dest_remotepath, posix_path_basename(remotepaths[i])),
            )
            from_to_list.append(from_to)
        j += 1

    if show:
        display_from_to(*from_to_list)


def rename(api: AliPCSApi, remotepath: str, new_name: str, show: bool = False):
    """Rename a file or directory.

    e.g.
        rename(api, remotepath="/some/file", new_name="new_file") is equal to
        move(api, "/some/file", "/some/new_file")
    """

    pcs_file = api.path(remotepath)

    if not pcs_file:
        return

    api.rename(pcs_file.file_id, new_name)
    pcs_file.path = join_path(*split_posix_path(pcs_file.path)[:-1], new_name)

    if show:
        display_files([pcs_file], "", show_file_id=True, show_absolute_path=True)


def copy(api: AliPCSApi, *remotepaths: str, show: bool = False):
    """Copy files or directories to a destination directory.

    Copy the paths(`remotepaths[:-1]`) to the directory(`remotepaths[-1]`).
    """

    assert len(remotepaths) > 1

    dest_remotepath = remotepaths[-1]
    pcs_files = api.paths(*remotepaths)

    non_exist = [(rp, pf) for rp, pf in zip(remotepaths[:-1], pcs_files[:-1]) if pf is None]
    assert len(non_exist) == 0, f"Paths not exist: {non_exist}"

    # Make sure destination directory exists
    if not pcs_files[-1]:
        dest_pcs_file = api.makedir_path(dest_remotepath)[0]
        pcs_files[-1] = dest_pcs_file

    file_ids = [pf.file_id for pf in pcs_files if pf]
    new_pfs = api.copy(*file_ids)
    if show:
        display_files(new_pfs, "", show_file_id=True, show_absolute_path=True)


def remove(api: AliPCSApi, *remotepaths: str):
    """Remove files or directories to the trash."""

    pcs_files = api.paths(*remotepaths)
    file_ids = [pf.file_id for pf in pcs_files if pf]
    api.remove(*file_ids)
