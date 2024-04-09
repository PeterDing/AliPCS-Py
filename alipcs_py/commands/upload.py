from typing import Any, Callable, Optional, List, Sequence, Tuple, IO, Union
import os
import time
import math
from hashlib import sha1
from io import BytesIO
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from alipcs_py.alipcs.errors import AliPCSError, RapidUploadError, UploadError
from alipcs_py.alipcs import AliPCSApi, FromTo
from alipcs_py.alipcs.pcs import CheckNameMode
from alipcs_py.common import constant
from alipcs_py.common.path import PathType, exists, posix_path_basename, posix_path_dirname
from alipcs_py.common.event import KeyHandler, KeyboardMonitor
from alipcs_py.common.constant import CPU_NUM
from alipcs_py.common.concurrent import retry
from alipcs_py.common.progress_bar import (
    _progress,
    init_progress_bar,
    progress_task_exists,
    remove_progress_task,
    reset_progress_task,
)
from alipcs_py.common.crypto import calc_sha1, calc_proof_code
from alipcs_py.common.io import total_len, EncryptType, reset_encrypt_io
from alipcs_py.commands.log import get_logger

from rich.progress import TaskID
from rich import print

from alipcs_py.utils import human_size_to_int

logger = get_logger(__name__)

# If slice size >= 100M, the rate of uploading will be much lower.
DEFAULT_SLICE_SIZE = 80 * constant.OneM

UPLOAD_STOP = False


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


def from_tos(localpaths: Sequence[PathType], remotedir: str) -> List[FromTo]:
    """Recursively find all localpaths and their corresponded remotepath"""

    ft: List[FromTo[Path, str]] = []
    for localpath in localpaths:
        localpath = Path(localpath).resolve()
        if not localpath.exists():
            continue

        if localpath.is_file():
            remotepath = Path(remotedir, localpath.name).as_posix()
            ft.append((localpath, remotepath))
        else:
            parent_num = len(localpath.parent.parts)
            for root, _, filenames in os.walk(localpath):
                for filename in filenames:
                    sub_path = Path(root, filename)
                    relative_path = Path(*Path(root).parts[parent_num:], filename).as_posix()
                    remotepath = Path(remotedir, relative_path).as_posix()
                    ft.append((sub_path, remotepath))
    return ft


def upload(
    api: AliPCSApi,
    from_to_list: List[FromTo[PathType, str]],
    check_name_mode: CheckNameMode = "overwrite",
    encrypt_password: bytes = b"",
    encrypt_type: EncryptType = EncryptType.No,
    max_workers: int = CPU_NUM,
    max_retries: int = 3,
    slice_size: Union[str, int] = DEFAULT_SLICE_SIZE,
    only_use_rapid_upload: bool = False,
    show_progress: bool = False,
) -> None:
    r"""Upload files in `from_to_list` to Alipan Drive

    Use a `ThreadPoolExecutor` to upload files concurrently.

    Raise exception if any error occurs.

    Args:
        api (AliPCSApi): AliPCSApi instance.
        from_to_list (List[FromTo[PathType, str]]): List of FromTo instances which decide the local path needed to upload and the remote path to upload to.
        check_name_mode (CheckNameMode, optional): CheckNameMode. Defaults to "overwrite".
        encrypt_password (bytes, optional): Encrypt password. Defaults to b"".
        encrypt_type (EncryptType, optional): Encrypt type. Defaults to EncryptType.No.
        max_workers (int, optional): Max workers. Defaults to the number of CPUs.
        max_retries (int, optional): Max retries. Defaults to 3.
        slice_size (Union[str, int], optional): Slice size. Defaults to DEFAULT_SLICE_SIZE.
        only_use_rapid_upload (bool, optional): Only use rapid upload. If rapid upload fails, raise exception. Defaults to False.
        show_progress (bool, optional): Show progress. Defaults to False.

    Examples:
    - Upload one file to one remote directory

        ```python
        >>> from alipcs_py.alipcs import AliPCSApi
        >>> from alipcs_py.commands.upload import upload, from_tos
        >>> api = AliPCSApi(...)
        >>> remotedir = "/remote/dir"
        >>> localpath = "/local/file"
        >>> from_to_list = from_tos([localpath], remotedir)
        >>> upload(api, from_to_list)
        ```

    - Upload multiple files and directories recursively to one remote directory

        ```python
        >>> from alipcs_py.alipcs import AliPCSApi
        >>> from alipcs_py.commands.upload import upload, from_tos
        >>> api = AliPCSApi(...)
        >>> remotedir = "/remote/dir"
        >>> target_paths = ['/local/file1', '/local/file2', '/local/dir1', '/local/dir2']
        >>> from_to_list = from_tos(target_paths, remotedir)
        >>> upload(api, from_to_list)
        ```
    """

    logger.debug(
        "======== Uploading start ========\n-> Size of from_to_list: %s",
        len(from_to_list),
    )

    futures = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for idx, from_to in enumerate(from_to_list):
            logger.debug("`upload_many`: Upload: index: %s", idx)

            retry_upload_file = retry(
                max_retries,
                except_callback=lambda err, fail_count: logger.warning(
                    "`upload_file`: fails: error: %s, fail_count: %s",
                    err,
                    fail_count,
                    exc_info=err,
                ),
            )(upload_file)

            fut = executor.submit(
                retry_upload_file,
                api,
                from_to,
                check_name_mode,
                encrypt_password=encrypt_password,
                encrypt_type=encrypt_type,
                slice_size=slice_size,
                only_use_rapid_upload=only_use_rapid_upload,
                show_progress=show_progress,
            )
            futures.append(fut)

    # Wait for all futures done
    for fut in as_completed(futures):
        # Raise the exception if the result of the future is an exception
        fut.result()


def _need_to_upload(api: AliPCSApi, remotepath: str, check_name_mode: CheckNameMode) -> bool:
    """Check wether the `remotepath` needs to be uploaded

    If `check_name_mode` is `refuse` and the `remotepath` exists, then it does not need to be uploaded.
    """

    pcs_file = api.get_file(remotepath=remotepath)
    if pcs_file is not None and check_name_mode == "refuse":
        logger.debug("`_init_encrypt_io`: remote file already exists")
        return False
    return True


def _init_encrypt_io(
    localpath: PathType,
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
    localpath: PathType,
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

        reset_progress_task(task_id)
        if err.error_code == 31079:  # 31079: '未找到文件MD5，请使用上传API上传整个文件。'
            logger.debug("`_can_rapid_upload`: %s, no exist in remote", localpath)
            return False
        else:
            logger.warning("`_can_rapid_upload`: unknown error: %s", err)
            raise err


def upload_file(
    api: AliPCSApi,
    from_to: FromTo[PathType, str],
    check_name_mode: CheckNameMode,
    encrypt_password: bytes = b"",
    encrypt_type: EncryptType = EncryptType.No,
    slice_size: Union[str, int] = DEFAULT_SLICE_SIZE,
    only_use_rapid_upload: bool = False,
    callback_for_monitor: Optional[Callable[[int], Any]] = None,
    show_progress: bool = False,
) -> None:
    """Upload a file from `from_to[0]` to `from_to[1]`

    First try to rapid upload, if failed, then upload file's slices.

    Raise exception if any error occurs.

    Args:
        api (AliPCSApi): AliPCSApi instance
        from_to (FromTo[PathType, str]): FromTo instance decides the local path needed to upload and the remote path to upload to.
        check_name_mode (CheckNameMode): CheckNameMode
        encrypt_password (bytes, optional): Encrypt password. Defaults to b"".
        encrypt_type (EncryptType, optional): Encrypt type. Defaults to EncryptType.No.
        slice_size (Union[str, int], optional): Slice size. Defaults to DEFAULT_SLICE_SIZE.
        only_use_rapid_upload (bool, optional): Only use rapid upload. If rapid upload fails, raise exception. Defaults to False.
        task_id (Optional[TaskID], optional): Task ID. Defaults to None.
        callback_for_monitor (Optional[Callable[[int], Any]], optional): Callback for progress monitor. Defaults to None.
            The callback should accept one argument which is the offset of the uploaded bytes.

    Examples:
    - Upload one file to one remote directory

        ```python
        >>> from alipcs_py.alipcs import AliPCSApi
        >>> from alipcs_py.commands.upload import upload, from_tos
        >>> api = AliPCSApi(...)
        >>> remotedir = "/remote/dir"
        >>> localpath = "/local/file"
        >>> from_to = (localpath, remotedir)
        >>> upload_file(api, from_to)
        ```

    - With tqdm progress bar

        ```python
        >>> from alipcs_py.alipcs import AliPCSApi
        >>> from alipcs_py.commands.upload import upload, from_tos
        >>> api = AliPCSApi(...)
        >>> remotedir = "/remote/dir"
        >>> localpath = "/local/file"
        >>> from_to = (localpath, remotedir)
        >>> with tqdm.tqdm(total=Path(localpath).stat().st_size) as pbar:
        >>>    upload_file(api, from_to, callback_for_monitor=lambda offset: pbar.n = offset)
        ```
    """

    _wait_start()

    # Upload basic info
    localpath, remotepath = from_to

    remotedir = posix_path_dirname(remotepath)
    dest_dir = api.get_file(remotepath=remotedir)
    if dest_dir is None:
        dest_dir = api.makedir_path(remotedir)[0]
    else:
        assert dest_dir.is_dir, f"`{remotedir}` is not a directory"
    dest_file_id = dest_dir.file_id

    filename = posix_path_basename(remotepath)

    # Progress bar
    task_id: Optional[TaskID] = None
    if show_progress:
        init_progress_bar()
        task_id = _progress.add_task("upload", start=False, title=from_to[0])

    if not _need_to_upload(api, remotepath, check_name_mode):
        if task_id is not None:
            print(f"`{remotepath}` already exists.")
        remove_progress_task(task_id)
        return

    # Upload IO info
    info = _init_encrypt_io(localpath, encrypt_password=encrypt_password, encrypt_type=encrypt_type)
    encrypt_io, encrypt_io_len, local_ctime, local_mtime = info
    if isinstance(slice_size, str):
        slice_size = human_size_to_int(slice_size)
    slice_size = adjust_slice_size(slice_size, encrypt_io_len)
    part_number = math.ceil(encrypt_io_len / slice_size)

    # Progress bar
    if task_id is not None and progress_task_exists(task_id):
        _progress.update(task_id, total=encrypt_io_len)
        _progress.start_task(task_id)

    slice_completed = 0

    def callback_for_slice(offset: int):
        if callback_for_monitor is not None:
            callback_for_monitor(slice_completed + offset)
        else:
            if task_id is not None and progress_task_exists(task_id):
                _progress.update(task_id, completed=slice_completed + offset)

    def teardown():
        encrypt_io.close()
        remove_progress_task(task_id)

    # Rapid Upload
    try:
        slice1k_hash = ""
        content_hash = ""
        pcs_prepared_file = None
        if encrypt_type == EncryptType.No and encrypt_io_len >= 1 * constant.OneK:
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
                ok = _rapid_upload(
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
                if ok:
                    teardown()
                    return
    except Exception as origin_err:
        teardown()
        msg = f'Rapid upload "{localpath}" to "{remotepath}" failed. error: {origin_err}'
        logger.debug(msg)
        err = RapidUploadError(msg, localpath=localpath, remotepath=remotepath)
        raise err from origin_err

    if only_use_rapid_upload:
        teardown()
        msg = f'Only use rapid upload but rapid upload failed. localpath: "{localpath}", remotepath: "{remotepath}"'
        logger.debug(msg)
        err = RapidUploadError(msg, localpath=localpath, remotepath=remotepath)
        raise err

    # Upload file slice
    try:
        logger.debug("`upload_file`: upload_slice starts")

        if not pcs_prepared_file:
            pcs_prepared_file = api.create_file(
                filename,
                dest_file_id,
                encrypt_io_len,
                part_number=part_number,
                check_name_mode=check_name_mode,
            )

        assert len(pcs_prepared_file.upload_urls()) == part_number

        reset_encrypt_io(encrypt_io)

        hasher = sha1()

        file_id = pcs_prepared_file.file_id
        upload_id = pcs_prepared_file.upload_id
        assert file_id and upload_id

        upload_urls = pcs_prepared_file.upload_urls()
        slice_idx = 0
        while slice_idx < len(upload_urls):
            _wait_start()

            logger.debug("`upload_file`: upload_slice: slice_completed: %s", slice_completed)

            size = min(slice_size, encrypt_io_len - slice_completed)
            if size == 0:
                break

            data = encrypt_io.read(size)
            hasher.update(data)

            io = BytesIO(data)

            logger.debug("`upload_file`: upload_slice: size should be %s == %s", size, len(data))

            fail_count = 0
            while True:  # Retry upload until success
                try:
                    assert (
                        pcs_prepared_file.part_info_list
                        and not pcs_prepared_file.part_info_list[slice_idx].is_expired()
                    )

                    upload_url = upload_urls[slice_idx]
                    api.upload_slice(io, upload_url, callback_for_monitor=callback_for_slice)
                    slice_idx += 1
                    break
                except Exception as origin_err:
                    fail_count += 1
                    io.seek(0, 0)
                    logger.debug(
                        "Upload slice failed. error: %s, fail_count: %s",
                        origin_err,
                        fail_count,
                        exc_info=origin_err,
                    )
                    _wait_start()

                    # Update upload slice urls
                    new_pcs_prepared_file = api.get_upload_url(upload_id, file_id, part_number=part_number)
                    pcs_prepared_file.part_info_list = new_pcs_prepared_file.part_info_list
                    upload_urls = new_pcs_prepared_file.upload_urls()

            slice_completed += size

        local_file_hash = hasher.hexdigest()
        uploaded_pcs_file = api.upload_complete(file_id, upload_id)
        assert uploaded_pcs_file.rapid_upload_info
        remote_file_hash = uploaded_pcs_file.rapid_upload_info.content_hash
        if remote_file_hash.lower() != local_file_hash.lower():
            raise ValueError(
                f"Hashs do not match between local file and remote file: local sha1 ({local_file_hash}) != remote sha1 ({remote_file_hash})"
            )

        logger.debug(
            "`upload_file`: upload_slice and combine_slices success, task_id: %s",
            task_id,
        )
    except Exception as origin_err:
        msg = f'Upload "{localpath}" to "{remotepath}" failed. error: {origin_err}'
        logger.debug(msg)
        err = UploadError(msg, localpath=localpath, remotepath=remotepath)
        raise err from origin_err
    finally:
        teardown()
