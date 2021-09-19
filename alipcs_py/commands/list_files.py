from typing import Optional, List, Tuple

from threading import Semaphore
from concurrent.futures import ThreadPoolExecutor, as_completed

from alipcs_py.alipcs import AliPCSApi
from alipcs_py.alipcs.inner import PcsFile, PcsRapidUploadInfo
from alipcs_py.common.path import join_path
from alipcs_py.common.concurrent import sure_release
from alipcs_py.common.localstorage import save_rapid_upload_info
from alipcs_py.commands.log import get_logger
from alipcs_py.commands.sifter import Sifter, sift
from alipcs_py.commands.display import display_files

from rich import print

logger = get_logger(__name__)

DEFAULT_MAX_WORKERS = 10


def list_file(
    api: AliPCSApi,
    remotepath: str,
    file_id: str = None,
    desc: bool = False,
    name: bool = False,
    time: bool = False,
    size: bool = False,
    all: bool = False,
    limit: int = 100,
    url_expire_sec: int = 7200,
    recursive: bool = False,
    sifters: List[Sifter] = [],
    highlight: bool = False,
    rapiduploadinfo_file: Optional[str] = None,
    user_id: Optional[str] = None,
    user_name: Optional[str] = None,
    show_size: bool = False,
    show_date: bool = False,
    show_file_id: bool = False,
    show_hash: bool = False,
    show_absolute_path: bool = False,
    show_dl_link: bool = False,
    csv: bool = False,
    only_dl_link: bool = False,
    only_hash_link: bool = False,
):
    pcs_file: Optional[PcsFile]
    if file_id:
        pcs_file = api.meta(file_id)[0]
    else:
        pcs_file = api.path(remotepath)
    if not pcs_file:
        return

    is_dir = pcs_file.is_dir
    if is_dir:
        pcs_files = api.list_path(
            remotepath,
            file_id=pcs_file.file_id,
            desc=desc,
            name=name,
            time=time,
            size=size,
            all=all,
            limit=limit,
            url_expire_sec=url_expire_sec,
        )
    else:
        pcs_files = [pcs_file]

    pcs_files = sift(pcs_files, sifters, recursive=recursive)
    if not pcs_files:
        return

    if show_dl_link:
        for pcs_file in pcs_files:
            if only_dl_link:
                print(pcs_file.download_url)

    if not only_dl_link:
        display_files(
            pcs_files,
            remotepath,
            sifters=sifters,
            highlight=highlight,
            show_size=show_size,
            show_date=show_date,
            show_file_id=show_file_id,
            show_hash=show_hash,
            show_absolute_path=show_absolute_path,
            show_dl_link=show_dl_link,
            csv=csv,
        )

    if is_dir and recursive:
        for pcs_file in pcs_files:
            if pcs_file.is_dir:
                list_file(
                    api,
                    join_path(remotepath, pcs_file.name),
                    file_id=pcs_file.file_id,
                    desc=desc,
                    name=name,
                    time=time,
                    size=size,
                    all=all,
                    limit=limit,
                    url_expire_sec=url_expire_sec,
                    recursive=recursive,
                    sifters=sifters,
                    highlight=highlight,
                    rapiduploadinfo_file=rapiduploadinfo_file,
                    user_id=user_id,
                    user_name=user_name,
                    show_size=show_size,
                    show_date=show_date,
                    show_file_id=show_file_id,
                    show_hash=show_hash,
                    show_absolute_path=show_absolute_path,
                    show_dl_link=show_dl_link,
                    csv=csv,
                    only_dl_link=only_dl_link,
                    only_hash_link=only_hash_link,
                )


def list_files(
    api: AliPCSApi,
    *remotepaths: str,
    file_ids: List[str] = [],
    desc: bool = False,
    name: bool = False,
    time: bool = False,
    size: bool = False,
    all: bool = False,
    limit: int = 100,
    url_expire_sec: int = 7200,
    recursive: bool = False,
    sifters: List[Sifter] = [],
    highlight: bool = False,
    rapiduploadinfo_file: Optional[str] = None,
    user_id: Optional[str] = None,
    user_name: Optional[str] = None,
    show_size: bool = False,
    show_date: bool = False,
    show_file_id: bool = False,
    show_hash: bool = False,
    show_absolute_path: bool = False,
    show_dl_link: bool = False,
    csv: bool = False,
    only_dl_link: bool = False,
    only_hash_link: bool = False,
):
    for rp in remotepaths:
        list_file(
            api,
            rp,
            desc=desc,
            name=name,
            time=time,
            size=size,
            all=all,
            limit=limit,
            url_expire_sec=url_expire_sec,
            recursive=recursive,
            sifters=sifters,
            highlight=highlight,
            rapiduploadinfo_file=rapiduploadinfo_file,
            user_id=user_id,
            user_name=user_name,
            show_size=show_size,
            show_date=show_date,
            show_file_id=show_file_id,
            show_hash=show_hash,
            show_absolute_path=show_absolute_path,
            show_dl_link=show_dl_link,
            csv=csv,
            only_dl_link=only_dl_link,
            only_hash_link=only_hash_link,
        )

    for file_id in file_ids:
        list_file(
            api,
            "",
            file_id=file_id,
            desc=desc,
            name=name,
            time=time,
            size=size,
            all=all,
            limit=limit,
            url_expire_sec=url_expire_sec,
            recursive=recursive,
            sifters=sifters,
            highlight=highlight,
            rapiduploadinfo_file=rapiduploadinfo_file,
            user_id=user_id,
            user_name=user_name,
            show_size=show_size,
            show_date=show_date,
            show_file_id=show_file_id,
            show_hash=show_hash,
            show_absolute_path=show_absolute_path,
            show_dl_link=show_dl_link,
            csv=csv,
            only_dl_link=only_dl_link,
            only_hash_link=only_hash_link,
        )
