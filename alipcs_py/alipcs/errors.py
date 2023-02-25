from typing import Optional, Any
from functools import wraps


class AliPCSError(Exception):
    def __init__(self, message: str, error_code: Optional[str] = None, cause=None):
        self.__cause__ = cause
        self.error_code = error_code
        super().__init__(message)


def parse_error(error_code: str, info: Any = None) -> AliPCSError:
    msg = f"error_code: {error_code}, response: {info}"
    return AliPCSError(msg, error_code=error_code)


def assert_ok(func):
    """Assert the errno of response is not 0"""

    @wraps(func)
    def check(*args, **kwargs):
        info = func(*args, **kwargs)
        error_code = info.get("code")

        if error_code:
            err = parse_error(error_code, str(info))
            raise err

        return info

    return check


def handle_error(func):
    @wraps(func)
    def refresh(*args, **kwargs):
        code = "This is impossible !!!"
        for _ in range(2):
            self = args[0]

            info = func(*args, **kwargs)
            code = info.get("code")
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
                    share_auth.expire_time = 0.0
                continue

            elif code == "DeviceSessionSignatureInvalid":
                self._signature = ""
                continue

            return info

        raise parse_error(code)

    return refresh
