from typing import Optional, Any
from functools import wraps
import time
import logging

from alipcs_py.common.path import PathType
from alipcs_py.alipcs.inner import PcsFile

logger = logging.getLogger(__name__)


class AliPCSBaseError(Exception):
    """Base exception for all errors.

    Args:
        message (Optional[object]): The message object stringified as 'message' attribute
        keyword error (Exception): The original exception if any
    """

    def __init__(self, message: Optional[object], *args: Any, **kwargs: Any) -> None:
        self.inner_exception: Optional[BaseException] = kwargs.get("error")

        self.message = str(message)
        super().__init__(self.message, *args)


class AliPCSError(AliPCSBaseError):
    """The error returned from alipan server when the client’s request is incorrect or the token is expired.

    It is throwed at `AliPCS` class when an error occurs, then transports to the upper level class.
    """

    def __init__(self, message: str, error_code: Optional[str] = None):
        self.error_code = error_code
        super().__init__(message)


class DownloadError(AliPCSBaseError):
    """An error occurred while downloading a file."""

    def __init__(self, message: str, remote_pcs_file: PcsFile, localdir: PathType):
        self.remote_pcs_file = remote_pcs_file
        self.localdir = localdir
        super().__init__(message)


class UploadError(AliPCSBaseError):
    """An error occurred while uploading a file."""

    def __init__(self, message: str, localpath: PathType, remotepath: str):
        self.local_file = localpath
        self.remote_dir = remotepath
        super().__init__(message)


class RapidUploadError(UploadError):
    """An error occurred while rapid uploading a file."""

    def __init__(self, message: str, localpath: PathType, remotepath: str):
        super().__init__(message, localpath, remotepath)


def make_alipcs_error(error_code: str, info: Any = None) -> AliPCSError:
    msg = f"API error code: {error_code}, response: {info}"
    return AliPCSError(msg, error_code=error_code)


def handle_error(func):
    """Handle error when calling AliPCS API."""

    @wraps(func)
    def retry(*args, **kwargs):
        self = args[0]
        max_retries = getattr(self, "_error_max_retries", 2)
        code = "This is impossible !!!"
        result = None
        for _ in range(max_retries):
            result = func(*args, **kwargs)
            if not isinstance(result, dict):
                return result

            code = result.get("code")
            if not code:
                return result

            # Error code
            # {{{
            if code == "AccessTokenInvalid":
                self.refresh()
                continue

            elif code == "ShareLinkTokenInvalid":
                varnames = func.__code__.co_varnames
                idx = varnames.index("share_id")
                if idx < len(args):
                    share_id = args[idx]
                else:
                    share_id = kwargs.get("share_id")

                share_auth = self.__class__.SHARE_AUTHS.get(share_id)
                if share_auth:
                    share_auth.expire_time = 0
                continue

            elif code == "ParamFlowException":
                logger.warning("ParamFlowException occurs. sleep 10s.")
                time.sleep(10)
                continue

            elif code == "DeviceSessionSignatureInvalid":
                self._signature = ""
                continue

            elif code.startswith("NotFound."):
                break
            # }}}

            # Status code
            # {{{
            elif code == "PreHashMatched":  # AliPCS.create_file: Pre hash matched.
                return result
            # }}}

        raise make_alipcs_error(code, info=result)

    return retry
