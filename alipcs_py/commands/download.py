from typing import Iterable, Optional, List, Sequence, Tuple, Union
from concurrent.futures import ThreadPoolExecutor, as_completed
from enum import Enum
from pathlib import Path
import os
import time
import shutil
import subprocess
import random

from alipcs_py.alipcs import AliPCSApi, PcsFile
from alipcs_py.alipcs.errors import AliPCSError, DownloadError
from alipcs_py.alipcs.pcs import PCS_UA
from alipcs_py.common.path import PathType
from alipcs_py.utils import human_size_to_int
from alipcs_py.common import constant
from alipcs_py.common.io import RangeRequestIO, to_decryptio, DecryptIO, READ_SIZE
from alipcs_py.common.downloader import MeDownloader
from alipcs_py.common.progress_bar import (
    _progress,
    init_progress_bar,
    remove_progress_task,
    reset_progress_task,
)
from alipcs_py.commands.sifter import Sifter, sift
from alipcs_py.commands.log import get_logger

_print = print

from rich import print
from rich.progress import TaskID

logger = get_logger(__name__)

USER_AGENT = PCS_UA

DEFAULT_CONCURRENCY = os.cpu_count() or 1
DEFAULT_CHUNK_SIZE = str(100 * constant.OneM)

# This is the threshold of range request setted by Ali server
MAX_CHUNK_SIZE = 50 * constant.OneM


class Downloader(Enum):
    me = "me"
    aget_py = "aget"  # https://github.com/PeterDing/aget
    aget_rs = "ag"  # https://github.com/PeterDing/aget-rs
    aria2 = "aria2c"  # https://github.com/aria2/aria2

    # No use axel. It Can't handle URLs of length over 1024
    # axel = 'axel'  # https://github.com/axel-download-accelerator/axel

    # No use wget. the file url of alipan only supports `Range` request

    def which(self) -> Optional[str]:
        return shutil.which(self.value)

    def download(
        self,
        url: str,
        localpath: str,
        concurrency: int = DEFAULT_CONCURRENCY,
        chunk_size: Union[str, int] = DEFAULT_CHUNK_SIZE,
        show_progress: bool = False,
        max_retries: int = 2,
        out_cmd: bool = False,
        encrypt_password: bytes = b"",
    ):
        global DEFAULT_DOWNLOADER
        if not self.which():
            self = DEFAULT_DOWNLOADER

        localpath_tmp = localpath + ".tmp"

        if self == Downloader.me:
            self._me_download(
                url,
                localpath_tmp,
                chunk_size=chunk_size,
                show_progress=show_progress,
                max_retries=max_retries,
                encrypt_password=encrypt_password,
            )
            shutil.move(localpath_tmp, localpath)
            return
        elif self == Downloader.aget_py:
            cmd = self._aget_py_cmd(
                url,
                localpath_tmp,
                concurrency=concurrency,
                chunk_size=chunk_size,
                show_progress=show_progress,
                max_retries=max_retries,
            )
        elif self == Downloader.aget_rs:
            cmd = self._aget_rs_cmd(
                url,
                localpath_tmp,
                concurrency=concurrency,
                chunk_size=chunk_size,
                show_progress=show_progress,
                max_retries=max_retries,
            )
        else:  # elif self == Downloader.aria2:
            cmd = self._aria2_cmd(
                url,
                localpath_tmp,
                concurrency=concurrency,
                chunk_size=chunk_size,
                show_progress=show_progress,
                max_retries=max_retries,
            )

        # Print out command
        if out_cmd:
            _print(" ".join((repr(c) for c in cmd)))
            return

        returncode = self.spawn(cmd, show_progress=show_progress)

        logger.debug("`download`: cmd returncode: %s", returncode)

        if returncode != 0:
            print(f"[italic]{self.value}[/italic] fails. return code: [red]{returncode}[/red]")
        else:
            if encrypt_password:
                dio = to_decryptio(open(localpath_tmp, "rb"), encrypt_password)
                if isinstance(dio, DecryptIO):
                    with open(localpath, "wb") as fd:
                        while True:
                            buf = dio.read(READ_SIZE)
                            if not buf:
                                break
                            fd.write(buf)

                    os.remove(localpath_tmp)
                    return
            shutil.move(localpath_tmp, localpath)

    def spawn(self, cmd: List[str], show_progress: bool = False):
        child = subprocess.run(cmd, stdout=subprocess.DEVNULL if not show_progress else None)
        return child.returncode

    def _me_download(
        self,
        url: str,
        localpath: str,
        chunk_size: Union[str, int] = DEFAULT_CHUNK_SIZE,
        show_progress: bool = False,
        max_retries: int = 2,
        encrypt_password: bytes = b"",
    ):
        headers = {
            "Referer": "https://www.aliyundrive.com/",
            "User-Agent": USER_AGENT,
            "Connection": "Keep-Alive",
        }

        task_id: Optional[TaskID] = None
        if show_progress:
            init_progress_bar()
            task_id = _progress.add_task("MeDownloader", start=False, title=localpath)

        def done_callback():
            remove_progress_task(task_id)

        def monitor_callback(offset: int):
            if task_id is not None:
                _progress.update(task_id, completed=offset + 1)

        def except_callback(err):
            reset_progress_task(task_id)

        if isinstance(chunk_size, str):
            chunk_size = human_size_to_int(chunk_size)
        io = RangeRequestIO(
            "GET",
            url,
            headers=headers,
            max_chunk_size=chunk_size,
            callback=monitor_callback,
            encrypt_password=encrypt_password,
        )

        if task_id is not None:
            length = len(io)
            _progress.update(task_id, total=length)
            _progress.start_task(task_id)

        meDownloader = MeDownloader(
            io,
            localpath=localpath,
            continue_=True,
            max_retries=max_retries,
            done_callback=done_callback,
            except_callback=except_callback,
        )
        meDownloader.download()

    def _aget_py_cmd(
        self,
        url: str,
        localpath: str,
        concurrency: int = DEFAULT_CONCURRENCY,
        chunk_size: Union[str, int] = DEFAULT_CHUNK_SIZE,
        show_progress: bool = False,
        max_retries: int = 2,
    ):
        cmd = [
            self.which(),
            url,
            "-o",
            localpath,
            "-H",
            f"User-Agent: {USER_AGENT}",
            "-H",
            "Connection: Keep-Alive",
            "-H",
            "Referer: https://www.aliyundrive.com/",
            "-s",
            str(concurrency),
            "-k",
            chunk_size,
        ]
        if not show_progress:
            cmd.append("-q")
        return cmd

    def _aget_rs_cmd(
        self,
        url: str,
        localpath: str,
        concurrency: int = DEFAULT_CONCURRENCY,
        chunk_size: Union[str, int] = DEFAULT_CHUNK_SIZE,
        show_progress: bool = False,
        max_retries: int = 2,
    ):
        cmd = [
            self.which(),
            url,
            "-o",
            localpath,
            "-H",
            f"User-Agent: {USER_AGENT}",
            "-H",
            "Connection: Keep-Alive",
            "-H",
            "Referer: https://www.aliyundrive.com/",
            "-s",
            str(concurrency),
            "-k",
            chunk_size,
        ]
        if not show_progress:
            cmd.append("--quiet")
        if max_retries > 0:
            cmd.append("--retries")
            cmd.append(str(max_retries))
        return cmd

    def _aria2_cmd(
        self,
        url: str,
        localpath: str,
        concurrency: int = DEFAULT_CONCURRENCY,
        chunk_size: Union[str, int] = DEFAULT_CHUNK_SIZE,
        show_progress: bool = False,
        max_retries: int = 2,
    ):
        directory, filename = os.path.split(localpath)
        cmd = [
            self.which(),
            "-c",
            "--dir",
            directory,
            "-o",
            filename,
            "--header",
            f"User-Agent: {USER_AGENT}",
            "--header",
            "Connection: Keep-Alive",
            "--header",
            "Referer: https://www.aliyundrive.com/",
            "-s",
            str(concurrency),
            "-k",
            chunk_size,
            url,
        ]
        if not show_progress:
            cmd.append("--quiet")
        return cmd


DEFAULT_DOWNLOADER = Downloader.me


def download_file(
    api: AliPCSApi,
    remote_file: Union[str, PcsFile],
    localdir: PathType = ".",
    share_id: Optional[str] = None,
    downloader: Union[str, Downloader] = DEFAULT_DOWNLOADER,
    concurrency: int = DEFAULT_CONCURRENCY,
    chunk_size: Union[str, int] = DEFAULT_CHUNK_SIZE,
    show_progress: bool = False,
    max_retries: int = 2,
    out_cmd: bool = False,
    encrypt_password: bytes = b"",
) -> None:
    """Download a `remote_file` to the `localdir`

    Raise the exception if any error occurred.

    Args:
        api (AliPCSApi): AliPCSApi instance.
        remote_file (str | PcsFile): The remote file to download.
        localdir (str | PathLike | Path, optional): The local directory to save file. Defaults to ".".
        share_id (str, optional): The share_id of file. Defaults to None.
        downloader (str, Downloader, optional): The downloader(or its name) to download file. Defaults to DEFAULT_DOWNLOADER.
        concurrency (int, optional): The number of concurrent downloads. Defaults to DEFAULT_CONCURRENCY.
        chunk_size (str | int, optional): The chunk size of each download. Defaults to DEFAULT_CHUNK_SIZE.
        show_progress (bool, optional): Whether show progress bar. Defaults to False.
        max_retries (int, optional): The max retries of download. Defaults to 2.
        out_cmd (bool, optional): Whether print out the command. Defaults to False.
        encrypt_password (bytes, optional): The password to decrypt the file. Defaults to b"".
    """

    if isinstance(downloader, str):
        downloader = getattr(Downloader, downloader)
    assert isinstance(downloader, Downloader)  # For linters

    if isinstance(remote_file, str):
        remote_pcs_file = api.get_file(remotepath=remote_file)
        if remote_pcs_file is None:
            raise ValueError(f"Remote file `{remote_file}` does not exists.")
    else:
        remote_pcs_file = remote_file

    localpath = Path(localdir) / remote_pcs_file.name

    # Make sure parent directory existed
    if not localpath.parent.exists():
        localpath.parent.mkdir(parents=True, exist_ok=True)

    if not out_cmd and localpath.exists():
        if not show_progress:
            print(f"[yellow]{localpath}[/yellow] is ready existed.")
        return

    if not show_progress and downloader != Downloader.me:
        print(f"[italic blue]Download[/italic blue]: {remote_pcs_file.path or remote_pcs_file.name} to {localpath}")

    if share_id:
        shared_pcs_file_id = remote_pcs_file.file_id
        shared_pcs_filename = remote_pcs_file.name
        remote_temp_dir = "/__alipcs_py_temp__"
        pcs_temp_dir = api.path(remote_temp_dir) or api.makedir_path(remote_temp_dir)[0]
        pf = api.transfer_shared_files([shared_pcs_file_id], pcs_temp_dir.file_id, share_id)[0]
        target_file_id = pf.file_id
        while True:
            pfs = api.search_all(shared_pcs_filename)
            for pf_ in pfs:
                if pf_.file_id == target_file_id:
                    remote_pcs_file = pf_
                    break
            else:
                time.sleep(2)
                continue

            break

    if not remote_pcs_file or remote_pcs_file.is_dir:
        return

    while True:
        try:
            remote_pcs_file = api.update_download_url(remote_pcs_file)
            break
        except AliPCSError as err:
            if err.error_code == "TooManyRequests":
                time.sleep(random.randint(1, 2))
                continue
            raise err

    download_url = remote_pcs_file.download_url
    assert download_url

    try:
        downloader.download(
            download_url,
            str(localpath),
            concurrency=concurrency,
            chunk_size=chunk_size,
            show_progress=show_progress,
            max_retries=max_retries,
            out_cmd=out_cmd,
            encrypt_password=encrypt_password,
        )
    except Exception as origin_err:
        msg = f'Download "{remote_pcs_file.path}" (file_id = "{remote_pcs_file.file_id}") to "{localpath}" failed. error: {origin_err}'
        logger.debug(msg)
        err = DownloadError(msg, remote_pcs_file=remote_pcs_file, localdir=str(localdir))
        raise err from origin_err

    if share_id:
        api.remove(remote_pcs_file.file_id)


def walk_remote_paths(
    api: AliPCSApi,
    pcs_files: List[PcsFile],
    localdir: PathType,
    share_id: Optional[str] = None,
    sifters: List[Sifter] = [],
    recursive: bool = False,
    from_index: int = 0,
    deep: int = 0,
) -> Iterable[Tuple[PcsFile, PathType]]:
    pcs_files = [pf for pf in sift(pcs_files, sifters, recursive=recursive)]
    for pf in pcs_files:
        if pf.is_file:
            yield pf, localdir
        else:
            if deep > 0 and not recursive:
                continue

            localdir_ = Path(localdir) / pf.name
            for pcs_file in api.list_iter(pf.file_id, share_id=share_id):
                if pcs_file.is_file:
                    yield pcs_file, localdir_
                else:
                    yield from walk_remote_paths(
                        api,
                        [pcs_file],
                        localdir_,
                        share_id=share_id,
                        sifters=sifters,
                        recursive=recursive,
                        from_index=from_index,
                        deep=deep + 1,
                    )


def download(
    api: AliPCSApi,
    remotepaths: Sequence[Union[str, PcsFile]] = [],
    file_ids: List[str] = [],
    localdir: PathType = ".",
    share_id: Optional[str] = None,
    sifters: List[Sifter] = [],
    recursive: bool = False,
    from_index: int = 0,
    downloader: Union[str, Downloader] = DEFAULT_DOWNLOADER,
    concurrency: int = DEFAULT_CONCURRENCY,
    chunk_size: Union[str, int] = DEFAULT_CHUNK_SIZE,
    show_progress: bool = False,
    max_retries: int = 2,
    out_cmd: bool = False,
    encrypt_password: bytes = b"",
) -> None:
    """Download files with their `remotepaths` and `file_ids` to the `localdir`

    Use a `ThreadPoolExecutor` to download files concurrently and raise the exception if any error occurred.

    Args:
        api (AliPCSApi): AliPCSApi instance.
        remotepaths (List[Union[str, PcsFile]], optional): The remotepaths of files or directories to download.
        file_ids (List[str], optional): The file_ids of files or directories to download. Defaults to [].
        localdir (str | PathLike | Path, optional): The local directory to save files. Defaults to ".".
        share_id (str, optional): The share_id of files. Defaults to None.
        sifters (List[Sifter], optional): The sifters to filter files. Defaults to [].
        recursive (bool, optional): Whether download files recursively. Defaults to False.
        from_index (int, optional): The index of the first file to download. Defaults to 0.
        downloader (str, Downloader, optional): The downloader(or its name) to download files. Defaults to Downloader.me.
        concurrency (int, optional): The number of concurrent downloads. Defaults to DEFAULT_CONCURRENCY.
        chunk_size (str | int, optional): The chunk size of each download. Defaults to DEFAULT_CHUNK_SIZE.
        show_progress (bool, optional): Whether show progress bar. Defaults to False.
        max_retries (int, optional): The max retries of download. Defaults to 2.
        out_cmd (bool, optional): Whether print out the command. Defaults to False.
        encrypt_password (bytes, optional): The password to decrypt the file. Defaults to b"".
    """

    logger.debug(
        "download: remotepaths=%s, file_ids=%s, localdir=%s, share_id=%s, sifters=%s, recursive=%s, from_index=%s, downloader=%s, concurrency=%s, chunk_size=%s, quiet=%s, max_retries=%s, out_cmd=%s, encrypt_password=%s",
        [rp.path if isinstance(rp, PcsFile) else rp for rp in remotepaths],
        file_ids,
        localdir,
        share_id,
        sifters,
        recursive,
        from_index,
        downloader,
        concurrency,
        chunk_size,
        show_progress,
        max_retries,
        out_cmd,
        encrypt_password,
    )

    assert len(remotepaths) + len(file_ids) > 0, "No remotepaths or file_ids to download."

    pcs_files = []
    for rp in remotepaths:
        if isinstance(rp, PcsFile):
            pcs_files.append(rp)
            continue
        pf = api.get_file(remotepath=rp, share_id=share_id)
        if pf is None:
            if not show_progress:
                print(f"[yellow]WARNING[/yellow]: `remotepath={rp}` does not exist.")
            continue
        pcs_files.append(pf)

    for file_id in file_ids:
        pf = api.get_file(file_id=file_id, share_id=share_id)
        if pf is None:
            if not show_progress:
                print(f"[yellow]WARNING[/yellow]: `{file_id=}` does not exist.")
            continue
        pcs_files.append(pf)

    futures = []
    with ThreadPoolExecutor(concurrency) as executor:
        for pf, localdir_ in walk_remote_paths(
            api,
            pcs_files,
            localdir,
            share_id=share_id,
            sifters=sifters,
            recursive=recursive,
            from_index=from_index,
        ):
            fut = executor.submit(
                download_file,
                api,
                pf,
                localdir_,
                share_id=share_id,
                downloader=downloader,
                concurrency=concurrency,
                chunk_size=chunk_size,
                show_progress=show_progress,
                max_retries=max_retries,
                out_cmd=out_cmd,
                encrypt_password=encrypt_password,
            )
            futures.append(fut)

    # Wait for all futures done
    for fut in as_completed(futures):
        # Throw the exception if the future has exception
        fut.result()

    if not show_progress:
        _progress.stop()
