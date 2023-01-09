from typing import Optional, List, Tuple, IO

import os
import time
import functools
import math
from io import BytesIO
from enum import Enum
from pathlib import Path
from threading import Semaphore
from concurrent.futures import ThreadPoolExecutor, as_completed

from alipcs_py.alipcs.errors import AliPCSError
from alipcs_py.alipcs import AliPCSApi, FromTo
from alipcs_py.alipcs.pcs import CheckNameMode
from alipcs_py.common import constant
from alipcs_py.common.path import (
    is_file,
    exists,
    posix_path_basename,
    posix_path_dirname,
    walk,
)
from alipcs_py.common.event import KeyHandler, KeyboardMonitor
from alipcs_py.common.constant import CPU_NUM
from alipcs_py.common.concurrent import sure_release, retry
from alipcs_py.common.progress_bar import (
    _progress,
    progress_task_exists,
    remove_progress_task,
    reset_progress_task,
)
from alipcs_py.common.crypto import calc_sha1, calc_proof_code
from alipcs_py.common.io import (
    total_len,
    EncryptType,
    reset_encrypt_io,
)
from alipcs_py.commands.log import get_logger

from requests_toolbelt import MultipartEncoderMonitor

from rich.progress import TaskID
from rich.table import Table
from rich.box import SIMPLE
from rich.text import Text
from rich import print

logger = get_logger(__name__)

# If slice size >= 100M, the rate of uploading will be much lower.
DEFAULT_SLICE_SIZE = 80 * constant.OneM

UPLOAD_STOP = False

_rapiduploadinfo_file: Optional[str] = None


def _wait_start():
    while True:
        if UPLOAD_STOP:
            time.sleep(1)
        else:
            break


def _toggle_stop(*args, **kwargs):
    global UPLOAD_STOP
    UPLOAD_STOP = not UPLOAD_STOP
    if UPLOAD_STOP:
        print("[i yellow]Uploading stop[/i yellow]")
    else:
        print("[i yellow]Uploading continue[/i yellow]")


# Pass "p" to toggle uploading start/stop
KeyboardMonitor.register(KeyHandler("p", callback=_toggle_stop))


def adjust_slice_size(slice_size: int, io_len: int) -> int:
    """Adjust slice_size to not let part_number of the file to be larger than the 10_000"""

    part_number = math.ceil(io_len / slice_size)
    if part_number > 10_000:
        return math.ceil(io_len / 10_000)
    else:
        return slice_size


def to_remotepath(sub_path: str, remotedir: str) -> str:
    return (Path(remotedir) / sub_path).as_posix()


def from_tos(localpaths: List[str], remotedir: str) -> List[FromTo]:
    """Find all localpaths and their corresponded remotepath"""

    ft: List[FromTo] = []
    for localpath in localpaths:
        if not exists(localpath):
            continue

        if is_file(localpath):
            remotepath = to_remotepath(os.path.basename(localpath), remotedir)
            ft.append(FromTo(localpath, remotepath))
        else:
            parents_num = max(len(Path(localpath).parts) - 1, 0)
            for sub_path in walk(localpath):
                relative_path = Path(*Path(sub_path).parts[parents_num:]).as_posix()
                remotepath = to_remotepath(relative_path, remotedir)
                ft.append(FromTo(sub_path, remotepath))
    return ft


class UploadType(Enum):
    """Upload Type

    One: Upload the slices of one file concurrently
    Many: Upload files concurrently
    """

    One = 1
    Many = 2


# remotedir must be a directory
def upload(
    api: AliPCSApi,
    from_to_list: List[FromTo],
    upload_type: UploadType = UploadType.One,
    check_name_mode: CheckNameMode = "auto_rename",
    encrypt_password: bytes = b"",
    encrypt_type: EncryptType = EncryptType.No,
    max_workers: int = CPU_NUM,
    slice_size: int = DEFAULT_SLICE_SIZE,
    show_progress: bool = True,
    rapiduploadinfo_file: Optional[str] = None,
    user_id: Optional[str] = None,
    user_name: Optional[str] = None,
):
    """Upload from_tos

    Args:
        upload_type (UploadType): the way of uploading.
        max_workers (int): The number of concurrent workers.
        slice_size (int): The size of slice for uploading slices.
        ignore_existing (bool): Ignoring these localpath which of remotepath exist.
        show_progress (bool): Show uploading progress.
        check_name_mode(str):
          'overwrite' (直接覆盖，以后多版本有用)
          'auto_rename' (自动换一个随机名称)
          'refuse' (不会创建，告诉你已经存在)
          'ignore' (会创建重名的)
    """

    logger.debug(
        "======== Uploading start ========\n-> UploadType: %s\n-> Size of from_to_list: %s",
        upload_type,
        len(from_to_list),
    )

    global _rapiduploadinfo_file
    if _rapiduploadinfo_file is None:
        _rapiduploadinfo_file = rapiduploadinfo_file

    if upload_type == UploadType.One:
        upload_one_by_one(
            api,
            from_to_list,
            check_name_mode,
            max_workers=max_workers,
            encrypt_password=encrypt_password,
            encrypt_type=encrypt_type,
            slice_size=slice_size,
            show_progress=show_progress,
            user_id=user_id,
            user_name=user_name,
        )
    elif upload_type == UploadType.Many:
        upload_many(
            api,
            from_to_list,
            check_name_mode,
            max_workers=max_workers,
            encrypt_password=encrypt_password,
            encrypt_type=encrypt_type,
            slice_size=slice_size,
            show_progress=show_progress,
            user_id=user_id,
            user_name=user_name,
        )


def _need_to_upload(
    api: AliPCSApi, remotepath: str, check_name_mode: CheckNameMode
) -> bool:
    """Check wether the `remotepath` needs to be uploaded

    If `check_name_mode` is `refuse` and the `remotepath` exists, then it does not need to be uploaded.
    """

    try:
        pcs_file = api.path(remotepath)
        if pcs_file and check_name_mode == "refuse":
            print(f"`{remotepath}` already exists.")
            logger.debug("`_init_encrypt_io`: remote file already exists")
            return False
        return True
    except Exception as err:
        raise err


def _init_encrypt_io(
    localpath: str,
    encrypt_password: bytes = b"",
    encrypt_type: EncryptType = EncryptType.No,
) -> Tuple[IO, int, int, int]:
    assert exists(Path(localpath)), f"`{localpath}` does not exist"

    stat = Path(localpath).stat()
    local_ctime, local_mtime = int(stat.st_ctime), int(stat.st_mtime)

    encrypt_io = encrypt_type.encrypt_io(open(localpath, "rb"), encrypt_password)
    # IO Length
    encrypt_io_len = total_len(encrypt_io)

    logger.debug(
        "`_init_encrypt_io`: encrypt_type: %s, localpath: %s, encrypt_io_len: %s",
        encrypt_type,
        localpath,
        encrypt_io_len,
    )

    return (encrypt_io, encrypt_io_len, local_ctime, local_mtime)


def _rapid_upload(
    api: AliPCSApi,
    localpath: str,
    filename: str,
    dest_file_id: str,
    content_hash: str,
    proof_code: str,
    io_len: int,
    check_name_mode: CheckNameMode = "auto_rename",
    task_id: Optional[TaskID] = None,
) -> bool:
    logger.debug("`_rapid_upload`: rapid_upload starts")
    try:
        api.rapid_upload_file(
            filename,
            dest_file_id,
            io_len,
            content_hash,
            proof_code,
            check_name_mode=check_name_mode,
        )

        remove_progress_task(task_id)

        logger.debug("`_can_rapid_upload`: rapid_upload success, task_id: %s", task_id)
        return True
    except AliPCSError as err:
        logger.warning("`_can_rapid_upload`: rapid_upload fails")

        if err.error_code != 31079:  # 31079: '未找到文件MD5，请使用上传API上传整个文件。'
            remove_progress_task(task_id)
            logger.warning("`_can_rapid_upload`: unknown error: %s", err)
            raise err
        else:
            reset_progress_task(task_id)
            logger.debug("`_can_rapid_upload`: %s, no exist in remote", localpath)

        return False


def upload_one_by_one(
    api: AliPCSApi,
    from_to_list: List[FromTo],
    check_name_mode: CheckNameMode,
    max_workers: int = CPU_NUM,
    encrypt_password: bytes = b"",
    encrypt_type: EncryptType = EncryptType.No,
    slice_size: int = DEFAULT_SLICE_SIZE,
    show_progress: bool = True,
    user_id: Optional[str] = None,
    user_name: Optional[str] = None,
):
    """Upload files one by one with uploading the slices concurrently"""

    for from_to in from_to_list:
        task_id = None
        if show_progress:
            task_id = _progress.add_task("upload", start=False, title=from_to.from_)
        upload_file_concurrently(
            api,
            from_to,
            check_name_mode,
            max_workers=max_workers,
            encrypt_password=encrypt_password,
            encrypt_type=encrypt_type,
            slice_size=slice_size,
            task_id=task_id,
            user_id=user_id,
            user_name=user_name,
        )

    logger.debug("======== Uploading end ========")


@retry(
    -1,
    except_callback=lambda err, fail_count: logger.warning(
        "`upload_file_concurrently`: fails: error: %s, fail_count: %s",
        err,
        fail_count,
        exc_info=err,
    ),
)
def upload_file_concurrently(
    api: AliPCSApi,
    from_to: FromTo,
    check_name_mode: CheckNameMode,
    max_workers: int = CPU_NUM,
    encrypt_password: bytes = b"",
    encrypt_type: EncryptType = EncryptType.No,
    slice_size: int = DEFAULT_SLICE_SIZE,
    task_id: Optional[TaskID] = None,
    user_id: Optional[str] = None,
    user_name: Optional[str] = None,
):
    """Uploading one file by uploading it's slices concurrently"""

    localpath, remotepath = from_to

    remotedir = posix_path_dirname(remotepath)
    dest_dir = api.makedir_path(remotedir)
    dest_file_id = dest_dir.file_id

    filename = posix_path_basename(remotepath)

    if not _need_to_upload(api, remotepath, check_name_mode):
        remove_progress_task(task_id)
        return

    info = _init_encrypt_io(
        localpath, encrypt_password=encrypt_password, encrypt_type=encrypt_type
    )
    encrypt_io, encrypt_io_len, local_ctime, local_mtime = info
    slice_size = adjust_slice_size(slice_size, encrypt_io_len)
    part_number = math.ceil(encrypt_io_len / slice_size)

    # Progress bar
    if task_id is not None and progress_task_exists(task_id):
        _progress.update(task_id, total=encrypt_io_len)
        _progress.start_task(task_id)

    slice_completed = 0
    slice_completeds = {}  # current i-th index slice completed size

    def callback_for_slice(upload_url: str, monitor: MultipartEncoderMonitor):
        if task_id is not None and progress_task_exists(task_id):
            slice_completeds[upload_url] = monitor.bytes_read
            current_compledted: int = sum(list(slice_completeds.values()))
            _progress.update(task_id, completed=slice_completed + current_compledted)

    slice1k_hash = ""
    content_hash = ""

    pcs_prepared_file = None
    if encrypt_type == EncryptType.No and encrypt_io_len >= 1 * constant.OneK:
        # Rapid Upload

        slice1k_bytes = encrypt_io.read(constant.OneK)
        reset_encrypt_io(encrypt_io)
        slice1k_hash = calc_sha1(slice1k_bytes)

        pcs_prepared_file = api.prepare_file(
            filename,
            dest_file_id,
            encrypt_io_len,
            slice1k_hash,
            part_number=part_number,
            check_name_mode=check_name_mode,
        )
        if pcs_prepared_file.can_rapid_upload():
            content_hash = calc_sha1(encrypt_io)
            proof_code = calc_proof_code(encrypt_io, encrypt_io_len, api.access_token)

            # Rapid upload
            _rapid_upload(
                api,
                localpath,
                filename,
                dest_file_id,
                content_hash,
                proof_code,
                encrypt_io_len,
                check_name_mode=check_name_mode,
                task_id=task_id,
            )
            return

    try:
        # Upload file slice
        logger.debug("`upload_file_concurrently`: upload_slice starts")

        if not pcs_prepared_file:
            pcs_prepared_file = api.create_file(
                filename,
                dest_file_id,
                encrypt_io_len,
                part_number=part_number,
                check_name_mode=check_name_mode,
            )

        reset_encrypt_io(encrypt_io)

        def _upload_slice(item):
            if not item:
                return

            io, upload_url = item

            # Retry upload until success
            retry(
                -1,
                except_callback=lambda err, fail_count: (
                    io.seek(0, 0),
                    logger.warning(
                        "`upload_file_concurrently`: error: %s, fail_count: %s",
                        err,
                        fail_count,
                        exc_info=err,
                    ),
                    _wait_start(),
                ),
            )(api.upload_slice)(
                io,
                upload_url,
                callback=functools.partial(callback_for_slice, upload_url),
            )

            slice_completeds.pop(upload_url)

            nonlocal slice_completed
            slice_completed += total_len(io)

        semaphore = Semaphore(max_workers)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futs = []
            i = 0
            for upload_url in pcs_prepared_file.upload_urls():
                semaphore.acquire()

                size = min(slice_size, encrypt_io_len - i)
                if size == 0:
                    break

                data = encrypt_io.read(size)
                io = BytesIO(data or b"")

                fut = executor.submit(
                    sure_release, semaphore, _upload_slice, (io, upload_url)
                )
                futs.append(fut)

                i += size

            as_completed(futs)

        file_id = pcs_prepared_file.file_id
        upload_id = pcs_prepared_file.upload_id
        assert file_id and upload_id
        api.upload_complete(file_id, upload_id)

        remove_progress_task(task_id)

        logger.debug(
            "`upload_file_concurrently`: upload_slice and combine_slices success, task_id: %s",
            task_id,
        )
    except Exception as err:
        logger.warning("`upload_file_concurrently`: error: %s", err)
        raise err
    finally:
        encrypt_io.close()
        reset_progress_task(task_id)


def upload_many(
    api: AliPCSApi,
    from_to_list: List[FromTo],
    check_name_mode: CheckNameMode = "overwrite",
    encrypt_password: bytes = b"",
    encrypt_type: EncryptType = EncryptType.No,
    max_workers: int = CPU_NUM,
    slice_size: int = DEFAULT_SLICE_SIZE,
    show_progress: bool = True,
    rapiduploadinfo_file: Optional[str] = None,
    user_id: Optional[str] = None,
    user_name: Optional[str] = None,
):
    """Upload files concurrently that one file is with one connection"""

    excepts = {}
    semaphore = Semaphore(max_workers)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futs = {}
        for idx, from_to in enumerate(from_to_list):
            semaphore.acquire()
            task_id = None
            if show_progress:
                task_id = _progress.add_task("upload", start=False, title=from_to.from_)

            logger.debug("`upload_many`: Upload: index: %s, task_id: %s", idx, task_id)

            fut = executor.submit(
                sure_release,
                semaphore,
                upload_file,
                api,
                from_to,
                check_name_mode,
                encrypt_password=encrypt_password,
                encrypt_type=encrypt_type,
                slice_size=slice_size,
                task_id=task_id,
                user_id=user_id,
                user_name=user_name,
            )
            futs[fut] = from_to

        for fut in as_completed(futs):
            e = fut.exception()
            if e is not None:
                from_to = futs[fut]
                excepts[from_to] = e

    logger.debug("======== Uploading end ========")

    # Summary
    if excepts:
        table = Table(title="Upload Error", box=SIMPLE, show_edge=False)
        table.add_column("From", justify="left", overflow="fold")
        table.add_column("To", justify="left", overflow="fold")
        table.add_column("Error", justify="left")

        for from_to, e in sorted(excepts.items()):
            table.add_row(from_to.from_, Text(str(e), style="red"))

        _progress.console.print(table)


@retry(
    -1,
    except_callback=lambda err, fail_count: logger.warning(
        "`upload_file`: fails: error: %s, fail_count: %s",
        err,
        fail_count,
        exc_info=err,
    ),
)
def upload_file(
    api: AliPCSApi,
    from_to: FromTo,
    check_name_mode: CheckNameMode,
    encrypt_password: bytes = b"",
    encrypt_type: EncryptType = EncryptType.No,
    slice_size: int = DEFAULT_SLICE_SIZE,
    task_id: Optional[TaskID] = None,
    user_id: Optional[str] = None,
    user_name: Optional[str] = None,
):
    """Upload one file with one connection"""

    _wait_start()

    localpath, remotepath = from_to

    remotedir = posix_path_dirname(remotepath)
    dest_dir = api.makedir_path(remotedir)
    dest_file_id = dest_dir.file_id

    filename = posix_path_basename(remotepath)

    if not _need_to_upload(api, remotepath, check_name_mode):
        remove_progress_task(task_id)
        return

    info = _init_encrypt_io(
        localpath, encrypt_password=encrypt_password, encrypt_type=encrypt_type
    )
    encrypt_io, encrypt_io_len, local_ctime, local_mtime = info
    slice_size = adjust_slice_size(slice_size, encrypt_io_len)
    part_number = math.ceil(encrypt_io_len / slice_size)

    # Progress bar
    if task_id is not None and progress_task_exists(task_id):
        _progress.update(task_id, total=encrypt_io_len)
        _progress.start_task(task_id)

    slice_completed = 0

    def callback_for_slice(monitor: MultipartEncoderMonitor):
        if task_id is not None and progress_task_exists(task_id):
            _progress.update(task_id, completed=slice_completed + monitor.bytes_read)

    slice1k_hash = ""
    content_hash = ""

    pcs_prepared_file = None
    if encrypt_type == EncryptType.No and encrypt_io_len >= 1 * constant.OneK:
        # Rapid Upload

        slice1k_bytes = encrypt_io.read(constant.OneK)
        reset_encrypt_io(encrypt_io)
        slice1k_hash = calc_sha1(slice1k_bytes)

        pcs_prepared_file = api.prepare_file(
            filename,
            dest_file_id,
            encrypt_io_len,
            slice1k_hash,
            part_number=part_number,
            check_name_mode=check_name_mode,
        )
        if pcs_prepared_file.can_rapid_upload():
            content_hash = calc_sha1(encrypt_io)
            proof_code = calc_proof_code(encrypt_io, encrypt_io_len, api.access_token)

            # Rapid upload
            _rapid_upload(
                api,
                localpath,
                filename,
                dest_file_id,
                content_hash,
                proof_code,
                encrypt_io_len,
                check_name_mode=check_name_mode,
                task_id=task_id,
            )
            return

    try:
        # Upload file slice
        logger.debug("`upload_file`: upload_slice starts")

        if not pcs_prepared_file:
            pcs_prepared_file = api.create_file(
                filename,
                dest_file_id,
                encrypt_io_len,
                part_number=part_number,
                check_name_mode=check_name_mode,
            )

        reset_encrypt_io(encrypt_io)

        for upload_url in pcs_prepared_file.upload_urls():
            _wait_start()

            logger.debug(
                "`upload_file`: upload_slice: slice_completed: %s", slice_completed
            )

            size = min(slice_size, encrypt_io_len - slice_completed)
            if size == 0:
                break

            data = encrypt_io.read(size) or b""
            io = BytesIO(data)

            logger.debug(
                "`upload_file`: upload_slice: size should be %s == %s", size, len(data)
            )

            # Retry upload until success
            retry(
                -1,
                except_callback=lambda err, fail_count: (
                    io.seek(0, 0),
                    logger.warning(
                        "`upload_file`: `upload_slice`: error: %s, fail_count: %s",
                        err,
                        fail_count,
                        exc_info=err,
                    ),
                    _wait_start(),
                ),
            )(api.upload_slice)(io, upload_url, callback=callback_for_slice)

            slice_completed += size

        file_id = pcs_prepared_file.file_id
        upload_id = pcs_prepared_file.upload_id
        assert file_id and upload_id
        api.upload_complete(file_id, upload_id)

        remove_progress_task(task_id)

        logger.debug(
            "`upload_file`: upload_slice and combine_slices success, task_id: %s",
            task_id,
        )
    except Exception as err:
        logger.warning("`upload_file`: error: %s", err)
        raise err
    finally:
        encrypt_io.close()
        reset_progress_task(task_id)
