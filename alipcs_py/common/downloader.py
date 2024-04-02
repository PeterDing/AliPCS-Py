from typing import Optional, Any, Callable
from os import PathLike
from pathlib import Path

from alipcs_py.common.io import RangeRequestIO
from alipcs_py.common.concurrent import retry


DEFAULT_MAX_WORKERS = 5


class MeDownloader:
    def __init__(
        self,
        range_request_io: RangeRequestIO,
        localpath: PathLike,
        continue_: bool = False,
        retries: int = 2,
        done_callback: Optional[Callable[..., Any]] = None,
        except_callback: Optional[Callable[[Exception], Any]] = None,
    ) -> None:
        self.range_request_io = range_request_io
        self.localpath = localpath
        self.continue_ = continue_
        self.retries = retries
        self.done_callback = done_callback
        self.except_callback = except_callback

    def _init_fd(self):
        if self.continue_:
            path = Path(self.localpath)
            if self.range_request_io.seekable():
                offset = path.stat().st_size if path.exists() else 0
                fd = path.open("ab")
                fd.seek(offset, 0)
            else:
                offset = 0
                fd = path.open("wb")
        else:
            offset = 0
            fd = open(self.localpath, "wb")

        self.offset = offset
        self.fd = fd

    def download(self):
        """
        Download the url content to `localpath`

        Args:
            continue_ (bool): If set to True, only downloading the remain content depended on
            the size of `localpath`
        """

        @retry(
            self.retries,
            except_callback=lambda err, fails: (
                self.range_request_io.reset(),
                self.except_callback(err) if self.except_callback else None,
            ),
        )
        def _download():
            self._init_fd()

            self.range_request_io.seek(self.offset)

            for buf in self.range_request_io.read_iter():
                self.fd.write(buf)
                self.offset += len(buf)

            if self.done_callback:
                self.done_callback()

            self.fd.close()

        _download()
