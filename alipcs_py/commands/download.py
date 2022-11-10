from typing import Optional, List, Dict, Any, Callable
from types import SimpleNamespace
from enum import Enum
from pathlib import Path
import os
import shutil
import subprocess
from concurrent.futures import Future

from alipcs_py.alipcs import AliPCSApi, PcsFile
from alipcs_py.alipcs.pcs import PCS_UA
from alipcs_py.utils import human_size_to_int
from alipcs_py.common import constant
from alipcs_py.common.io import to_decryptio, DecryptIO, READ_SIZE
from alipcs_py.common.downloader import MeDownloader
from alipcs_py.common.progress_bar import (
    _progress,
    init_progress_bar,
    progress_task_exists,
)
from alipcs_py.commands.sifter import Sifter, sift
from alipcs_py.commands.log import get_logger

_print = print

from rich import print
from rich.progress import TaskID

logger = get_logger(__name__)

USER_AGENT = PCS_UA

DEFAULT_CONCURRENCY = 5
DEFAULT_CHUNK_SIZE = str(1 * constant.OneM)

# This is the threshold of range request setted by Ali server
MAX_CHUNK_SIZE = 50 * constant.OneM


class DownloadParams(SimpleNamespace):
    concurrency: int = DEFAULT_CONCURRENCY
    chunk_size: str = DEFAULT_CHUNK_SIZE
    quiet: bool = False


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

        def done_callback(fut: Future):
            err = fut.exception()
            if not err:
                shutil.move(localpath_tmp, localpath)
            else:
                logger.info("`download`: MeDownloader fails: error: %s", err)

        if self == Downloader.me:
            self._me_download(
                url,
                localpath_tmp,
                downloadparams=downloadparams,
                done_callback=done_callback,
                encrypt_password=encrypt_password,
            )
            return
        elif self == Downloader.aget_py:
            cmd = self._aget_py_cmd(url, localpath_tmp, downloadparams)
        elif self == Downloader.aget_rs:
            cmd = self._aget_rs_cmd(url, localpath_tmp, downloadparams)
        elif self == Downloader.aria2:
            cmd = self._aria2_cmd(url, localpath_tmp, downloadparams)
        else:
            cmd = self._aget_py_cmd(url, localpath_tmp, downloadparams)

        # Print out command
        if out_cmd:
            _print(" ".join((repr(c) for c in cmd)))
            return

        returncode = self.spawn(cmd, downloadparams.quiet)

        logger.debug("`download`: cmd returncode: %s", returncode)

        if returncode != 0:
            print(
                f"[italic]{self.value}[/italic] fails. return code: [red]{returncode}[/red]"
            )
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
        done_callback: Optional[Callable[[Future], Any]] = None,
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

        def _wrap_done_callback(fut: Future):
            if task_id is not None:
                _progress.remove_task(task_id)
            if done_callback:
                done_callback(fut)

        def monit_callback(task_id: Optional[TaskID], offset: int):
            if task_id is not None:
                _progress.update(task_id, completed=offset + 1)

        def except_callback(task_id: Optional[TaskID]):
            if task_id is not None and progress_task_exists(task_id):
                _progress.reset(task_id)

        chunk_size_int = human_size_to_int(downloadparams.chunk_size)
        meDownloader = MeDownloader(
            "GET",
            url,
            headers=headers,
            max_workers=downloadparams.concurrency,
            max_chunk_size=chunk_size_int,
            callback=monit_callback,
            encrypt_password=encrypt_password,
        )

        if task_id is not None:
            length = len(meDownloader)
            _progress.update(task_id, total=length)
            _progress.start_task(task_id)

        meDownloader.download(
            Path(localpath),
            task_id=task_id,
            continue_=True,
            done_callback=_wrap_done_callback,
            except_callback=except_callback,
        )

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
    localpath = Path(localdir) / pcs_file.name

    # Make sure parent directory existed
    if not localpath.parent.exists():
        localpath.parent.mkdir(parents=True)

    if not out_cmd and localpath.exists():
        print(f"[yellow]{localpath}[/yellow] is ready existed.")
        return

    if not pcs_file:
        return

    if downloader != Downloader.me:
        print(
            f"[italic blue]Download[/italic blue]: {pcs_file.path or pcs_file.name} to {localpath}"
        )

    download_url: Optional[str]
    if share_id:
        download_url = api.shared_file_download_url(pcs_file.file_id, share_id)
    else:
        pcs_file.update_download_url(api)
        download_url = pcs_file.download_url

    assert download_url

    downloader.download(
        download_url,
        str(localpath),
        downloadparams=downloadparams,
        out_cmd=out_cmd,
        encrypt_password=encrypt_password,
    )


def download_dir(
    api: AliPCSApi,
    pcs_file: PcsFile,
    localdir: str,
    share_id: str = None,
    sifters: List[Sifter] = [],
    recursive: bool = False,
    from_index: int = 0,
    downloader: Downloader = DEFAULT_DOWNLOADER,
    downloadparams=DEFAULT_DOWNLOADPARAMS,
    out_cmd: bool = False,
    encrypt_password: bytes = b"",
):
    remotefiles = list(api.list_iter(pcs_file.file_id, share_id=share_id))
    remotefiles = sift(remotefiles, sifters, recursive=recursive)
    for rp in remotefiles[from_index:]:
        if rp.is_file:
            download_file(
                api,
                rp,
                localdir,
                share_id=share_id,
                downloader=downloader,
                downloadparams=downloadparams,
                out_cmd=out_cmd,
                encrypt_password=encrypt_password,
            )
        else:  # is_dir
            if recursive:
                _localdir = Path(localdir) / os.path.basename(rp.path)
                download_dir(
                    api,
                    rp,
                    str(_localdir),
                    share_id=share_id,
                    sifters=sifters,
                    recursive=recursive,
                    from_index=from_index,
                    downloader=downloader,
                    downloadparams=downloadparams,
                    out_cmd=out_cmd,
                    encrypt_password=encrypt_password,
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

    for rp in remotepaths:
        rpf = api.path(rp, share_id=share_id)
        if not rpf:
            print(f"[yellow]WARNING[/yellow]: `{rp}` does not exist.")
            continue

        if rpf.is_file:
            download_file(
                api,
                rpf,
                localdir,
                share_id=share_id,
                downloader=downloader,
                downloadparams=downloadparams,
                out_cmd=out_cmd,
                encrypt_password=encrypt_password,
            )
        else:
            _localdir = str(Path(localdir) / rpf.name)
            download_dir(
                api,
                rpf,
                _localdir,
                share_id=share_id,
                sifters=sifters,
                recursive=recursive,
                from_index=from_index,
                downloader=downloader,
                downloadparams=downloadparams,
                out_cmd=out_cmd,
                encrypt_password=encrypt_password,
            )

    for file_id in file_ids:
        rpf = api.meta(file_id, share_id=share_id)[0]
        if not rpf:
            print(f"[yellow]WARNING[/yellow]: file_id `{file_id}` does not exist.")
            continue

        if rpf.is_file:
            download_file(
                api,
                rpf,
                localdir,
                share_id=share_id,
                downloader=downloader,
                downloadparams=downloadparams,
                out_cmd=out_cmd,
                encrypt_password=encrypt_password,
            )
        else:
            _localdir = str(Path(localdir) / rpf.name)
            download_dir(
                api,
                rpf,
                _localdir,
                share_id=share_id,
                sifters=sifters,
                recursive=recursive,
                from_index=from_index,
                downloader=downloader,
                downloadparams=downloadparams,
                out_cmd=out_cmd,
                encrypt_password=encrypt_password,
            )

    if downloader == Downloader.me:
        MeDownloader._exit_executor()

    _progress.stop()
