from typing import Iterable, Optional, List, Tuple
from types import SimpleNamespace
from enum import Enum
from pathlib import Path
import os
import time
import shutil
import subprocess
import random

from alipcs_py.alipcs import AliPCSApi, PcsFile
from alipcs_py.alipcs.errors import AliPCSError
from alipcs_py.alipcs.pcs import PCS_UA
from alipcs_py.common.concurrent import Executor
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


class DownloadParams(SimpleNamespace):
    concurrency: int = DEFAULT_CONCURRENCY
    chunk_size: str = DEFAULT_CHUNK_SIZE
    quiet: bool = False
    retries: int = 2


DEFAULT_DOWNLOADPARAMS = DownloadParams()


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
        downloadparams: DownloadParams = DEFAULT_DOWNLOADPARAMS,
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
                downloadparams=downloadparams,
                encrypt_password=encrypt_password,
            )
            shutil.move(localpath_tmp, localpath)
            return
        elif self == Downloader.aget_py:
            cmd = self._aget_py_cmd(url, localpath_tmp, downloadparams)
        elif self == Downloader.aget_rs:
            cmd = self._aget_rs_cmd(url, localpath_tmp, downloadparams)
        else:  # elif self == Downloader.aria2:
            cmd = self._aria2_cmd(url, localpath_tmp, downloadparams)

        # Print out command
        if out_cmd:
            _print(" ".join((repr(c) for c in cmd)))
            return

        returncode = self.spawn(cmd, downloadparams.quiet)

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

    def spawn(self, cmd: List[str], quiet: bool = False):
        child = subprocess.run(cmd, stdout=subprocess.DEVNULL if quiet else None)
        return child.returncode

    def _me_download(
        self,
        url: str,
        localpath: str,
        downloadparams: DownloadParams = DEFAULT_DOWNLOADPARAMS,
        encrypt_password: bytes = b"",
    ):
        headers = {
            "Referer": "https://www.aliyundrive.com/",
            "User-Agent": USER_AGENT,
            "Connection": "Keep-Alive",
        }

        task_id: Optional[TaskID] = None
        if not downloadparams.quiet:
            init_progress_bar()
            task_id = _progress.add_task("MeDownloader", start=False, title=localpath)

        def done_callback():
            remove_progress_task(task_id)

        def monitor_callback(offset: int):
            if task_id is not None:
                _progress.update(task_id, completed=offset + 1)

        def except_callback(err):
            reset_progress_task(task_id)

        chunk_size_int = human_size_to_int(downloadparams.chunk_size)
        io = RangeRequestIO(
            "GET",
            url,
            headers=headers,
            max_chunk_size=chunk_size_int,
            callback=monitor_callback,
            encrypt_password=encrypt_password,
        )

        if task_id is not None:
            length = len(io)
            _progress.update(task_id, total=length)
            _progress.start_task(task_id)

        meDownloader = MeDownloader(
            io,
            localpath=Path(localpath),
            continue_=True,
            retries=downloadparams.retries,
            done_callback=done_callback,
            except_callback=except_callback,
        )
        meDownloader.download()

    def _aget_py_cmd(
        self,
        url: str,
        localpath: str,
        downloadparams: DownloadParams = DEFAULT_DOWNLOADPARAMS,
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
            str(downloadparams.concurrency),
            "-k",
            downloadparams.chunk_size,
        ]
        return cmd

    def _aget_rs_cmd(
        self,
        url: str,
        localpath: str,
        downloadparams: DownloadParams = DEFAULT_DOWNLOADPARAMS,
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
            str(downloadparams.concurrency),
            "-k",
            downloadparams.chunk_size,
        ]
        return cmd

    def _aria2_cmd(
        self,
        url: str,
        localpath: str,
        downloadparams: DownloadParams = DEFAULT_DOWNLOADPARAMS,
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
            str(downloadparams.concurrency),
            "-k",
            downloadparams.chunk_size,
            url,
        ]
        return cmd


DEFAULT_DOWNLOADER = Downloader.me


def download_file(
    api: AliPCSApi,
    pcs_file: PcsFile,
    localdir: str,
    share_id: str = None,
    downloader: Downloader = DEFAULT_DOWNLOADER,
    downloadparams: DownloadParams = DEFAULT_DOWNLOADPARAMS,
    out_cmd: bool = False,
    encrypt_password: bytes = b"",
):
    quiet = downloadparams.quiet
    localpath = Path(localdir) / pcs_file.name

    # Make sure parent directory existed
    if not localpath.parent.exists():
        localpath.parent.mkdir(parents=True)

    if not out_cmd and localpath.exists():
        if not quiet:
            print(f"[yellow]{localpath}[/yellow] is ready existed.")
        return

    if not quiet and downloader != Downloader.me:
        print(f"[italic blue]Download[/italic blue]: {pcs_file.path or pcs_file.name} to {localpath}")

    download_url: Optional[str]
    if share_id:
        remote_temp_dir = "/__alipcs_py_temp__"
        pcs_temp_dir = api.path(remote_temp_dir) or api.makedir_path(remote_temp_dir)
        pcs_file = api.transfer_shared_files([pcs_file.file_id], pcs_temp_dir.file_id, share_id)[0]

        while True:
            pcs_file = api.meta(pcs_file.file_id)[0]
            if pcs_file.download_url:
                break
            time.sleep(2)

    if not pcs_file or pcs_file.is_dir:
        return

    while True:
        try:
            pcs_file = api.update_download_url(pcs_file)
            break
        except AliPCSError as err:
            if err.error_code == "TooManyRequests":
                time.sleep(random.randint(1, 2))
                continue
            raise err

    download_url = pcs_file.download_url

    assert download_url

    try:
        downloader.download(
            download_url,
            str(localpath),
            downloadparams=downloadparams,
            out_cmd=out_cmd,
            encrypt_password=encrypt_password,
        )
    except Exception as err:
        logger.error("`download_file` fails: error: %s", err)
        if not quiet:
            print(f"[red]ERROR[/red]: `{pcs_file.path or pcs_file.name}` download fails.")

    if share_id:
        api.remove(pcs_file.file_id)


def walk_remote_paths(
    api: AliPCSApi,
    pcs_files: List[PcsFile],
    localdir: str,
    share_id: str = None,
    sifters: List[Sifter] = [],
    recursive: bool = False,
    from_index: int = 0,
    deep: int = 0,
) -> Iterable[Tuple[PcsFile, str]]:
    pcs_files = [pf for pf in sift(pcs_files, sifters, recursive=recursive)]
    for pf in pcs_files:
        if pf.is_file:
            yield pf, localdir
        else:
            if deep > 0 and not recursive:
                continue

            _localdir = Path(localdir) / pf.name
            for pcs_file in api.list_iter(pf.file_id, share_id=share_id):
                if pcs_file.is_file:
                    yield pcs_file, str(_localdir)
                else:
                    yield from walk_remote_paths(
                        api,
                        [pcs_file],
                        str(_localdir),
                        share_id=share_id,
                        sifters=sifters,
                        recursive=recursive,
                        from_index=from_index,
                        deep=deep + 1,
                    )


def download(
    api: AliPCSApi,
    remotepaths: List[str],
    file_ids: List[str],
    localdir: str,
    share_id: str = None,
    sifters: List[Sifter] = [],
    recursive: bool = False,
    from_index: int = 0,
    downloader: Downloader = DEFAULT_DOWNLOADER,
    downloadparams: DownloadParams = DEFAULT_DOWNLOADPARAMS,
    out_cmd: bool = False,
    encrypt_password: bytes = b"",
):
    """Download `remotepaths` to the `localdir`

    Args:
        `from_index` (int): The start index of downloading entries from EACH remote directory
    """

    logger.debug(
        "`download`: sifters: %s, recursive: %s, from_index: %s, "
        "downloader: %s, downloadparams: %s, out_cmd: %s, has encrypt_password: %s",
        sifters,
        recursive,
        from_index,
        downloader,
        downloadparams,
        out_cmd,
        bool(encrypt_password),
    )

    quiet = downloadparams.quiet

    pcs_files = []
    for rp in remotepaths:
        pf = api.path(rp, share_id=share_id)
        if pf is None:
            if not quiet:
                print(f"[yellow]WARNING[/yellow]: `{rp}` does not exist.")
            continue
        pcs_files.append(pf)

    for file_id in file_ids:
        info = api.meta(file_id, share_id=share_id)
        if len(info) == 0:
            if not quiet:
                print(f"[yellow]WARNING[/yellow]: `{file_id}` does not exist.")
            continue
        pcs_files.append(info[0])

    using_me_downloader = downloader == Downloader.me
    with Executor(downloadparams.concurrency if using_me_downloader else 1) as executor:
        for pf, _localdir in walk_remote_paths(
            api,
            pcs_files,
            localdir,
            share_id=share_id,
            sifters=sifters,
            recursive=recursive,
            from_index=from_index,
        ):
            executor.submit(
                download_file,
                api,
                pf,
                _localdir,
                share_id=share_id,
                downloader=downloader,
                downloadparams=downloadparams,
                out_cmd=out_cmd,
                encrypt_password=encrypt_password,
            )

    if not quiet:
        _progress.stop()
