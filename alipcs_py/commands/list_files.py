from typing import Optional, List

from alipcs_py.alipcs import AliPCSApi
from alipcs_py.alipcs.inner import PcsFile
from alipcs_py.common.path import join_path
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
    share_id: str = None,
    desc: bool = False,
    name: bool = False,
    time: bool = False,
    size: bool = False,
    all: bool = False,
    limit: int = 200,
    url_expire_sec: int = 14400,
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
):
    pcs_file = api.get_file(remotepath=remotepath, file_id=file_id, share_id=share_id)
    if pcs_file is None:
        return

    is_dir = pcs_file.is_dir
    if is_dir:
        pcs_files = []
        for sub_pf in api.list_iter(
            pcs_file.file_id,
            share_id=share_id,
            desc=desc,
            name=name,
            time=time,
            size=size,
            all=all,
            limit=limit,
            url_expire_sec=url_expire_sec,
        ):
            sub_pf.path = join_path(remotepath, sub_pf.path)
            pcs_files.append(sub_pf)
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
                    pcs_file.path,
                    file_id=pcs_file.file_id,
                    share_id=share_id,
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
                )


def list_files(
    api: AliPCSApi,
    *remotepaths: str,
    file_ids: List[str] = [],
    share_id: str = None,
    desc: bool = False,
    name: bool = False,
    time: bool = False,
    size: bool = False,
    all: bool = False,
    limit: int = 200,
    url_expire_sec: int = 14400,
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
):
    for rp in remotepaths:
        list_file(
            api,
            rp,
            share_id=share_id,
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
        )

    for file_id in file_ids:
        list_file(
            api,
            "",
            file_id=file_id,
            share_id=share_id,
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
        )
