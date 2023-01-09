from typing import Optional, Dict
from pathlib import Path
import os
import mimetypes
import asyncio
import secrets
import copy
from urllib.parse import quote

import uvicorn

from alipcs_py.alipcs import AliPCSApi
from alipcs_py.common.io import RangeRequestIO
from alipcs_py.common.constant import CPU_NUM
from alipcs_py.common.path import join_path
from alipcs_py.utils import format_date

from fastapi import Depends, FastAPI, Request, status, HTTPException
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.responses import HTMLResponse, StreamingResponse, Response
from jinja2 import Template

from rich import print

mimetypes.init()

app = FastAPI()

_api: Optional[AliPCSApi] = None
_root_dir: str = "/"
_encrypt_password: bytes = b""

# For Auth
_username: Optional[str] = None
_password: Optional[str] = None

# This template is from https://github.com/rclone/rclone/blob/master/cmd/serve/httplib/serve/data/templates/index.html
_html_tempt: Template = Template(
    (Path(__file__).parent / "index.html").open(encoding="utf-8").read()
)


def fake_io(io: RangeRequestIO, start: int = 0, end: int = -1):
    for b in io._auto_decrypt_request.read((start, end)):
        yield b


async def handle_request(
    request: Request,
    remotepath: str,
    order: str = "asc",  # desc , asc
    sort: str = "name",  # name, time, size
):
    desc = order == "desc"
    name = sort == "name"
    time = sort == "time"
    size = sort == "size"

    global _root_dir
    global _api
    assert _api

    remotepath = remotepath.strip("/")

    _rp = join_path(_root_dir, remotepath)

    # Anti path traversal attack
    if not _rp.startswith(_root_dir):
        raise HTTPException(status_code=404, detail="Item not found")

    _range = request.headers.get("range")

    rpf = _api.path(_rp)
    if not rpf:
        raise HTTPException(status_code=404, detail="Item not found")

    is_dir = rpf.is_dir
    if is_dir:
        chunks = ["/"] + (remotepath.split("/") if remotepath != "" else [])
        navigation = [
            (i - 1, "../" * (len(chunks) - i), name) for i, name in enumerate(chunks, 1)
        ]
        pcs_files = _api.list_path_iter(_rp, desc=desc, name=name, time=time, size=size)
        entries = []
        for f in pcs_files:
            p = Path(f.path)
            entries.append(
                (
                    f.is_dir,
                    p.name,
                    quote(p.name),
                    f.size,
                    format_date(f.updated_at or 0),
                )
            )
        cn = _html_tempt.render(
            root_dir=remotepath, navigation=navigation, entries=entries
        )
        return HTMLResponse(cn)
    else:
        try:
            fs = _api.file_stream(rpf.file_id, encrypt_password=_encrypt_password)
        except Exception as err:
            print("Error:", err)
            raise HTTPException(
                status_code=500, detail=f"Error: {err}, remotepath: {_rp}"
            )

        if not fs:
            raise HTTPException(status_code=404, detail=f"No download link: {_rp}")

        length = len(fs)

        headers: Dict[str, str] = {
            "accept-ranges": "bytes",
            "connection": "Keep-Alive",
            "access-control-allow-origin": "*",
        }

        ext = os.path.splitext(remotepath)[-1]
        content_type = mimetypes.types_map.get(ext)

        if content_type:
            headers["content-type"] = content_type

        if _range and fs.seekable():
            assert _range.startswith("bytes=")

            status_code = 206
            start, end = _range[6:].split("-")
            _s, _e = int(start or 0), int(end or length - 1) + 1
            _iter_io = fake_io(fs, _s, _e)
            headers["content-range"] = f"bytes {_s}-{_e-1}/{length}"
            headers["content-length"] = str(_e - _s)
        else:
            status_code = 200
            _iter_io = fake_io(fs)
            headers["content-length"] = str(length)
        return StreamingResponse(_iter_io, status_code=status_code, headers=headers)


_security = HTTPBasic()


def to_auth(credentials: HTTPBasicCredentials = Depends(_security)) -> str:
    correct_username = secrets.compare_digest(credentials.username, _username or "")
    correct_password = secrets.compare_digest(credentials.password, _password or "")
    if not (correct_username and correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username


def make_auth_http_server(path: str = ""):
    @app.get("%s/{remotepath:path}" % path)
    async def auth_http_server(
        username: str = Depends(to_auth), response: Response = Depends(handle_request)
    ):
        if username:
            return response


def make_http_server(path: str = ""):
    @app.get("%s/{remotepath:path}" % path)
    async def http_server(response: Response = Depends(handle_request)):
        return response


def start_server(
    api: AliPCSApi,
    root_dir: str = "/",
    path: str = "",
    host: str = "localhost",
    port: int = 8000,
    workers: int = CPU_NUM,
    encrypt_password: bytes = b"",
    log_level: str = "info",
    username: Optional[str] = None,
    password: Optional[str] = None,
):
    """Create a http server on remote `root_dir`"""

    global _encrypt_password
    _encrypt_password = encrypt_password

    global _root_dir
    _root_dir = root_dir

    global _api
    if not _api:
        _api = api

    global _username
    if not _username:
        _username = username

    global _password
    if not _password:
        _password = password

    if path == "/" or not path:
        path = ""
    else:
        path = "/" + path.strip("/")

    print(f"[yellow]Server running on[/yellow] [b]http://{host}:{port}{path}/[/b]")

    if _username and _password:
        make_auth_http_server(path)
    else:
        make_http_server(path)

    log_config = copy.deepcopy(uvicorn.config.LOGGING_CONFIG)
    log_config["formatters"]["access"][
        "fmt"
    ] = '%(asctime)s - %(levelprefix)s %(client_addr)s - "%(request_line)s" %(status_code)s - %(msecs)d ms'
    uvicorn.run(
        "alipcs_py.commands.server:app",
        host=host,
        port=port,
        log_level=log_level,
        log_config=log_config,
        workers=1,
    )
