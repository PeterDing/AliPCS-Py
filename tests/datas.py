import os
from dataclasses import dataclass

from alipcs_py.alipcs.inner import PcsFile

REFRESH_TOKEN = os.getenv("REFRESH_TOKEN", "")


@dataclass
class Datas:
    local_dir: str
    local_paths: list[str]
    remote_dir: str
    remote_dir_pcs_file: PcsFile
    remote_paths: list[str]
