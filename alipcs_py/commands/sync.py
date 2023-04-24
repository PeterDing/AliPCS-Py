from typing import Optional, List, Tuple
from pathlib import Path

from alipcs_py.alipcs import AliPCSApi, PcsFile, FromTo
from alipcs_py.common.path import walk, join_path
from alipcs_py.common.crypto import calc_sha1
from alipcs_py.common.constant import CPU_NUM
from alipcs_py.common.io import EncryptType
from alipcs_py.commands.upload import UploadType, upload as _upload, DEFAULT_SLICE_SIZE
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
    rapiduploadinfo_file: Optional[str] = None,
    user_id: Optional[str] = None,
    user_name: Optional[str] = None,
):
    localdir = Path(localdir).as_posix()
    remotedir = Path(remotedir).as_posix()

    rpf = api.makedir_path(remotedir)
    assert rpf and rpf.is_dir, "remotedir must be a directory"

    if remotedir == "/":
        remotedir_len = 0
    else:
        remotedir_len = len(remotedir)

    all_pcs_files = {
        pcs_file.path[remotedir_len + 1 :]: pcs_file
        for pcs_file in api.list_path_iter(remotedir, recursive=True, include_dir=False)
    }

    fts: List[FromTo] = []
    check_list: List[Tuple[str, PcsFile]] = []
    all_localpaths = set()
    for localpath in walk(localdir):
        path = localpath[len(localdir) + 1 :]
        all_localpaths.add(path)

        if path not in all_pcs_files:
            fts.append(FromTo(localpath, join_path(remotedir, path)))
        else:
            check_list.append((localpath, all_pcs_files[path]))

    for lp, pf in check_list:
        sha1 = calc_sha1(Path(lp).open("rb"))

        if pf.rapid_upload_info and sha1.lower() != pf.rapid_upload_info.content_hash.lower():
            fts.append(FromTo(lp, pf.path))

    need_deleted_file_ids = []
    for rp in all_pcs_files.keys():
        if rp not in all_localpaths:
            need_deleted_file_ids.append(all_pcs_files[rp].file_id)

    logger.debug(
        "`sync`: all localpaths: %s, localpaths needed to upload: %s, remotepaths needed to delete: %s",
        len(all_localpaths),
        len(fts),
        len(need_deleted_file_ids),
    )

    _upload(
        api,
        fts,
        upload_type=UploadType.Many,
        check_name_mode="overwrite",
        encrypt_password=encrypt_password,
        encrypt_type=encrypt_type,
        max_workers=max_workers,
        slice_size=slice_size,
        show_progress=show_progress,
        rapiduploadinfo_file=rapiduploadinfo_file,
        user_id=user_id,
        user_name=user_name,
    )

    if need_deleted_file_ids:
        api.remove(*need_deleted_file_ids)
        print(f"Delete: [i]{len(need_deleted_file_ids)}[/i] remote paths")
