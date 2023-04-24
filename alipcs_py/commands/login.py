from typing import Tuple
from io import StringIO
import time

import qrcode


_print = print

from rich import print

from alipcs_py.alipcs import AliOpenAuth, AliPCSError
from alipcs_py.alipcs.inner import AuthInfo


def openapi_qrcode_login(client_id: str, client_secret: str, client_server: str) -> AuthInfo:
    aliopenauth = AliOpenAuth(client_id=client_id, client_secret=client_secret, client_server=client_server)

    # Get qrcode info
    info = aliopenauth.get_qrcode_info()
    sid = info["sid"]

    qrcode_url = f"https://www.aliyundrive.com/o/oauth/authorize?sid={sid}"

    qr = qrcode.QRCode()
    qr.add_data(qrcode_url)
    f = StringIO()
    qr.print_ascii(out=f, tty=False, invert=True)
    f.seek(0)
    _print(f.read())

    print("  [red b]Please scan the qrcode to login in 120 seconds[/red b]")

    interval = 2 * 60  # wait 2min
    sleep = 2

    auth_code = ""
    for _ in range(interval // sleep):
        time.sleep(2)

        info = aliopenauth.scan_status(sid)
        if info["status"] == "LoginSuccess":
            auth_code = info["authCode"]
            break

    if not auth_code:
        raise AliPCSError("Login failed")

    info = aliopenauth.get_refresh_token(auth_code)
    return AuthInfo.from_(info)
