from typing import Tuple, Union, Iterator
from pathlib import Path, PurePosixPath
import os
from os import PathLike

from alipcs_py.common.platform import IS_WIN

PathType = Union["str", PathLike, Path]


def exists(localpath: PathType) -> bool:
    localpath = Path(localpath)
    return localpath.exists()


def is_file(localpath: PathType) -> bool:
    localpath = Path(localpath)
    return localpath.is_file()


def is_dir(localpath: PathType) -> bool:
    localpath = Path(localpath)
    return localpath.is_dir()


def walk(localpath: PathType) -> Iterator[str]:
    for root, _, files in os.walk(localpath):
        r = Path(root)
        for fl in files:
            yield (r / fl).as_posix()


def join_path(parent: PathType, *children: PathType) -> str:
    """Join posix paths"""

    _path = Path(parent)
    for child in children:
        _path = _path / child

    path = _path.as_posix()
    has_root = path.startswith("/")
    if not has_root:
        path = "/" + path

    path = Path(path).resolve().as_posix()

    if IS_WIN:
        p = path.split(":", 1)[-1]
        if not has_root:
            return p[1:]
        else:
            return p
    else:
        if not has_root:
            return path[1:]
        else:
            return path


def split_posix_path(path: PathType) -> Tuple[str, ...]:
    return PurePosixPath(path).parts


def posix_path_basename(path: PathType) -> str:
    return PurePosixPath(path).name


def posix_path_dirname(path: PathType) -> str:
    return PurePosixPath(path).parent.as_posix()
