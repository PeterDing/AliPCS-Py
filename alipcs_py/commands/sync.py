from typing import Optional, List, Tuple
from pathlib import Path

from alipcs_py.alipcs import AliPCSApi, PcsFile, FromTo
from alipcs_py.common.path import walk, join_path
from alipcs_py.common.crypto import calc_sha1
from alipcs_py.common.constant import CPU_NUM
from alipcs_py.common.io import EncryptType
from alipcs_py.commands.upload import upload as _upload, DEFAULT_SLICE_SIZE
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
        for pcs_file in api.list_path_iter(remotedir)
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

        if pf.rapid_upload_info and sha1 != pf.rapid_upload_info.content_hash:
            fts.append(FromTo(lp, pf.path))

    to_deletes = []
    for rp in all_pcs_files.keys():
        if rp not in all_localpaths:
            to_deletes.append(all_pcs_files[rp].path)

    logger.debug(
        "`sync`: all localpaths: %s, "
        "localpaths needed to upload: %s, "
        "remotepaths needed to delete: %s",
        len(all_localpaths),
        len(fts),
        len(to_deletes),
    )

    # The md5 of remote file is incorrect at most time, so we don't compare md5
    #
    # # Compare localpath content md5 with remotepath content md5
    # semaphore = Semaphore(max_workers)
    # with ThreadPoolExecutor(max_workers=CPU_NUM) as executor:
    #     tasks = {}
    #     for lp, pf in check_list:
    #         semaphore.acquire()
    #         fut = executor.submit(sure_release, semaphore, check_file_md5, lp, pf)
    #         tasks[fut] = (lp, pf)
    #
    #     for fut in as_completed(tasks):
    #         is_equal = fut.result()
    #         lp, pf = tasks[fut]
    #         if not is_equal:
    #             fts.append(FromTo(lp, pf.path))

    _upload(
        api,
        fts,
        encrypt_password=encrypt_password,
        encrypt_type=encrypt_type,
        max_workers=max_workers,
        slice_size=slice_size,
        ignore_existing=False,
        show_progress=show_progress,
        rapiduploadinfo_file=rapiduploadinfo_file,
        user_id=user_id,
        user_name=user_name,
    )

    if to_deletes:
        api.remove(*to_deletes)
        print(f"Delete: [i]{len(to_deletes)}[/i] remote paths")
