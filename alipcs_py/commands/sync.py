from typing import List, Tuple
from pathlib import Path
import os

from alipcs_py.alipcs import AliPCSApi, PcsFile, FromTo
from alipcs_py.common.path import PathType, join_path
from alipcs_py.common.crypto import calc_sha1
from alipcs_py.common.constant import CPU_NUM
from alipcs_py.common.io import EncryptType
from alipcs_py.commands.upload import upload, DEFAULT_SLICE_SIZE
from alipcs_py.commands.log import get_logger

from rich import print

logger = get_logger(__name__)


def sync(
    api: AliPCSApi,
    localdir: str,
    remotedir: str,
    encrypt_password: bytes = b"",
    encrypt_type: EncryptType = EncryptType.No,
    max_workers: int = CPU_NUM,
    slice_size: int = DEFAULT_SLICE_SIZE,
    show_progress: bool = True,
):
    """Sync local directory to remote directory."""

    localdir = Path(localdir).as_posix()
    remotedir = Path(remotedir).as_posix()

    remote_pcs_file = api.makedir_path(remotedir)[0]
    assert remote_pcs_file and remote_pcs_file.is_dir, "remotedir must be a directory"

    sub_path_to_its_pcs_file = {
        pcs_file.path: pcs_file
        for pcs_file in api.list_iter(remote_pcs_file.file_id, recursive=True, include_dir=False)
    }

    needed_uploads: List[FromTo[PathType, str]] = []
    needed_checks: List[Tuple[Path, PcsFile]] = []
    all_localpaths = set()
    for root, _, filenames in os.walk(localdir):
        for filename in filenames:
            localpath = Path(root[len(localdir) + 1 :]) / filename
            localpath_posix = localpath.as_posix()
            all_localpaths.add(localpath_posix)

            if localpath_posix not in sub_path_to_its_pcs_file:
                needed_uploads.append((root / localpath, join_path(remotedir, localpath)))
            else:
                needed_checks.append((root / localpath, sub_path_to_its_pcs_file[localpath_posix]))

    for lp, pf in needed_checks:
        sha1 = calc_sha1(lp.open("rb"))

        if pf.rapid_upload_info and sha1.lower() != pf.rapid_upload_info.content_hash.lower():
            needed_uploads.append((lp, pf.path))

    need_deleted_file_ids = []
    for rp in sub_path_to_its_pcs_file.keys():
        if rp not in all_localpaths:
            need_deleted_file_ids.append(sub_path_to_its_pcs_file[rp].file_id)

    logger.debug(
        "`sync`: all localpaths: %s, localpaths needed to upload: %s, remotepaths needed to delete: %s",
        len(all_localpaths),
        len(needed_uploads),
        len(need_deleted_file_ids),
    )

    upload(
        api,
        needed_uploads,
        check_name_mode="overwrite",
        encrypt_password=encrypt_password,
        encrypt_type=encrypt_type,
        max_workers=max_workers,
        slice_size=slice_size,
        show_progress=show_progress,
    )

    if need_deleted_file_ids:
        api.remove(*need_deleted_file_ids)
        print(f"Delete: [i]{len(need_deleted_file_ids)}[/i] remote paths")
