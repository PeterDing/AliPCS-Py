from typing import Optional

from alipcs_py.alipcs import AliPCSApi
from alipcs_py.commands.display import display_blocked_remotepath

import chardet  # type: ignore


def cat(
    api: AliPCSApi,
    remotepath: str,
    encoding: Optional[str] = None,
    encrypt_password: bytes = b"",
):
    pcs_file = api.get_file(remotepath=remotepath)
    if not pcs_file:
        return

    fs = api.file_stream(pcs_file.file_id, encrypt_password=encrypt_password)
    if not fs:
        display_blocked_remotepath(remotepath)
        return

    cn = fs.read()
    if cn:
        if not encoding:
            r = chardet.detect(cn)
            if r["confidence"] > 0.5:
                encoding = r["encoding"]

        if encoding:
            print(cn.decode(encoding))
        else:
            print(cn)
