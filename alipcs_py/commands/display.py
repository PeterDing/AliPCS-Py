from typing import Optional, List, Tuple, Dict, Union, Pattern, Any
import time


from alipcs_py.alipcs import (
    PcsFile,
    FromTo,
    PcsSharedLink,
    PcsSharedLinkInfo,
    PcsUser,
    PcsRapidUploadInfo,
)
from alipcs_py.common.path import join_path
from alipcs_py.commands.sifter import Sifter
from alipcs_py.utils import format_date, format_time, human_size

_print = print

from rich.console import Console
from rich.table import Table
from rich.box import SIMPLE, MINIMAL
from rich.text import Text
from rich.highlighter import Highlighter as RichHighlighter
from rich.panel import Panel
from rich.style import Style
from rich import print


class Highlighter(RichHighlighter):
    def __init__(self, patterns: List[Union[Pattern, str]], style: Union[str, Style]):
        super().__init__()
        self.patterns = patterns
        self.style = style

    def highlight(self, text: Text):
        for pat in self.patterns:
            if isinstance(pat, Pattern):
                for m in pat.finditer(text.plain):
                    text.stylize(style=self.style, start=m.start(), end=m.end())
            else:
                pat_len = len(pat)
                start = 0
                while True:
                    idx = text.plain.find(pat, start)
                    if idx == -1:
                        break
                    text.stylize(style=self.style, start=idx, end=idx + pat_len)
                    start = idx + 1


def display_files(
    pcs_files: List[PcsFile],
    remotepath: str = "",
    sifters: List[Sifter] = [],
    highlight: bool = False,
    show_size: bool = False,
    show_date: bool = False,
    show_file_id: bool = False,
    show_hash: bool = False,
    show_absolute_path: bool = False,
    show_dl_link: bool = False,
    csv: bool = False,
):
    if not pcs_files:
        return

    table = Table(box=SIMPLE, padding=0, show_edge=False)
    table.add_column()
    headers = []  # for csv
    headers.append("\t")
    if show_size:
        header = "Size"
        table.add_column(header, justify="right")
        headers.append(header)
    if show_date:
        header = "Updated Time"
        table.add_column(header, justify="center")
        headers.append(header)
    if show_file_id:
        header = "File ID"
        table.add_column(header, justify="left")
        headers.append(header)
    if show_hash:
        header = "SHA1"
        table.add_column(header, justify="left")
        headers.append(header)
    header = "Path"
    table.add_column(header, justify="left", overflow="fold")
    headers.append(header)
    if show_dl_link:
        header = "Download Link"
        table.add_column(header, justify="left", overflow="fold")
        headers.append(header)

    rows = []  # for csv

    max_size_str_len = max([len(str(pcs_file.size)) for pcs_file in pcs_files])
    for pcs_file in pcs_files:
        row: List[Union[str, Text]] = []

        if csv:
            row.append("-")
        else:
            tp = Text("-", style="bold red")
            row.append(tp)

        if show_size:
            size = human_size(pcs_file.size) if pcs_file.size else ""
            if csv:
                row.append(f"{size} {pcs_file.size}")
            else:
                row.append(f"{size} {pcs_file.size or 0: >{max_size_str_len}}")
        if show_date:
            date = format_date(pcs_file.updated_at) if pcs_file.updated_at else ""
            row.append(date)
        if show_file_id:
            file_id = pcs_file.file_id or ""
            row.append(file_id)
        if show_hash:
            rapid_upload_info = pcs_file.rapid_upload_info
            content_hash = rapid_upload_info and rapid_upload_info.content_hash or ""
            row.append(content_hash)

        if show_absolute_path:
            path = pcs_file.path
            if not path:
                path = join_path(remotepath, pcs_file.name)
        else:
            path = pcs_file.name
        background = Text()
        if pcs_file.is_dir:
            if csv:
                row[0] = "d"
            else:
                tp._text = ["d"]
                background.style = "blue"

        if highlight and sifters:
            pats: List[Union[Pattern, str]] = list(
                filter(
                    None, [sifter.pattern() for sifter in sifters if sifter.include()]
                )
            )
            highlighter = Highlighter(pats, "yellow")
            _path = highlighter(path)
        else:
            _path = Text(path)

        if csv:
            row.append(path)
        else:
            row.append(background + _path)

        if show_dl_link:
            row.append(pcs_file.download_url or "")

        if csv:
            rows.append(row)
        else:
            table.add_row(*row)

    if csv:
        _print(remotepath)
        _print("\t".join(headers))
        for row in rows:
            _print("\t".join(row))  # type: ignore
    else:
        console = Console()
        if remotepath:
            title = Text(remotepath, style="italic green")
            console.print(title)
        console.print(table)


def display_from_to(*from_to_list: FromTo):
    if not from_to_list:
        return

    table = Table(box=SIMPLE, padding=0, show_edge=False)
    table.add_column("From", justify="left", overflow="fold")
    table.add_column("To", justify="left", overflow="fold")

    for from_to in from_to_list:
        table.add_row(from_to.from_, from_to.to_)

    console = Console()
    console.print(table)


_SHARED_LINK_INFO_FORMAT = (
    "share id: {share_id}\n"
    "shared url: [bold]{url}[/bold]\n"
    "time left: [i yellow]{expired_time}[/i yellow]\n"
    "password: [bold red]{password}[/bold red]\n"
    "name: {name}"
)


def display_shared_links(*shared_links: PcsSharedLink):
    panels = []
    for shared_link in shared_links:
        share_id = shared_link.share_id
        url = shared_link.share_url

        if shared_link.expired:
            expired_time = "已经超期"
        elif not shared_link.expiration:
            expired_time = "永久"
        else:
            expired_time = format_time((shared_link.expiration or 0) - int(time.time()))

        password = shared_link.share_pwd or ""
        name = shared_link.share_name

        panel = Panel(
            _SHARED_LINK_INFO_FORMAT.format(
                share_id=share_id,
                url=url,
                expired_time=expired_time,
                password=password,
                name=name,
            ),
            highlight=True,
        )
        panels.append(panel)

    console = Console()
    console.print(*panels)


def display_invalid_shared_link_infos(*shared_links: PcsSharedLinkInfo):
    for shared_link in shared_links:
        print(
            f"[i yellow]Remove[/i yellow]: {shared_link.share_url}\t{shared_link.share_name}"
        )


def display_shared_paths(*shared_paths):
    table = Table(box=SIMPLE, padding=0, show_edge=False)
    table.add_column()
    table.add_column("Size", justify="right")
    table.add_column("Path", justify="left", overflow="fold")

    max_size_str_len = max(
        [len(str(shared_path.size or 0)) for shared_path in shared_paths]
    )
    for shared_path in shared_paths:
        row: List[Union[str, Text]] = []

        # Is file
        tp = Text("-", style="bold red")
        row.append(tp)

        size = human_size(shared_path.size) if shared_path.size else ""
        row.append(f"{size} {shared_path.size or 0: >{max_size_str_len}}")

        path = shared_path.path
        background = Text()
        if shared_path.is_dir:
            tp._text = ["d"]
            background.style = "blue"

        _path = Text(path)
        row.append(background + _path)

        table.add_row(*row)

    console = Console()
    console.print(table)


def display_shared_link_infos(*shared_link_infos: PcsSharedLinkInfo):
    table = Table(box=SIMPLE, show_edge=False, highlight=True)
    table.add_column("Share URL", justify="left")
    table.add_column("Password", justify="left")
    table.add_column("Name", justify="left", overflow="fold")
    table.add_column("Expiration", justify="left", overflow="fold")

    for shared_link_info in shared_link_infos:
        table.add_row(
            f"https://www.aliyundrive.com/s/{shared_link_info.share_id}",
            shared_link_info.share_pwd or "",
            shared_link_info.share_name or shared_link_info.display_name or "",
            format_date(shared_link_info.expiration)
            if shared_link_info.expiration
            else "Never",
        )

    console = Console()
    console.print(table)


_SHARED_FILE_FORMAT = (
    "Share URL: {share_url}\n"
    "Password: [red]{password}[/red]\n"
    "Share Name: {share_name}\n"
    "Type: [yellow]{type}[/yellow]\n"
    "Size: {size}\n"
    "File ID: {file_id}\n"
    "Name: {name}\n"
    "Share URL Directory: {share_url_directory}"
)


def display_shared_files(
    *shared_file_and_links: Tuple[PcsFile, PcsSharedLinkInfo], verbose: bool = False
):
    if verbose:
        panels = []
        for shared_file, shared_link_info in shared_file_and_links:
            if shared_file.is_dir:
                share_url = f"https://www.aliyundrive.com/s/{shared_link_info.share_id}/folder/{shared_file.file_id}"
            else:
                share_url = f"https://www.aliyundrive.com/s/{shared_link_info.share_id}"
            panel = Panel(
                _SHARED_FILE_FORMAT.format(
                    share_url=share_url,
                    password=shared_link_info.share_pwd or "",
                    share_name=shared_link_info.share_name
                    or shared_link_info.display_name
                    or "",
                    type=shared_file.type,
                    size=human_size(shared_file.size or 0),
                    file_id=shared_file.file_id,
                    name=shared_file.name,
                    share_url_directory=f"https://www.aliyundrive.com/s/{shared_link_info.share_id}/folder/{shared_file.parent_file_id}",
                ),
                highlight=True,
            )
            panels.append(panel)
        console = Console()
        console.print(*panels)
    else:
        table = Table(box=SIMPLE, show_edge=False, highlight=True)
        table.add_column("Share URL", justify="left")
        table.add_column("Password", justify="left")
        table.add_column("Share Name", justify="left", overflow="fold")
        table.add_column("Type", justify="left")
        table.add_column("Size", justify="right")
        table.add_column("Name", justify="left", overflow="fold")

        for shared_file, shared_link_info in shared_file_and_links:
            table.add_row(
                f"https://www.aliyundrive.com/s/{shared_link_info.share_id}",
                shared_link_info.share_pwd or "",
                shared_link_info.share_name or shared_link_info.display_name or "",
                shared_file.type,
                human_size(shared_file.size or 0),
                shared_file.name,
                #  format_date(shared_link_info.expiration)
                #  if shared_link_info.expiration
                #  else "Never",
            )

        console = Console()
        console.print(table)


def display_user_info(user_info: PcsUser):
    user_id = user_info.user_id
    default_drive_id = user_info.default_drive_id
    domain_id = user_info.domain_id
    user_name = user_info.user_name
    nick_name = user_info.nick_name
    phone = user_info.phone
    personal_space_info = user_info.personal_space_info

    refresh_token = user_info.refresh_token
    access_token = user_info.access_token
    expire_time = format_date(user_info.expire_time or 0)

    assert personal_space_info
    quota_str = (
        human_size(personal_space_info.used_size)
        + "/"
        + human_size(personal_space_info.total_size)
    )

    _tempt = (
        f"user id: {user_id}\n"
        f"user name: {user_name}\n"
        f"nick name: {nick_name}\n"
        f"user phone: {phone}\n"
        f"default drive id: {default_drive_id}\n"
        f"domain id: {domain_id}\n"
        f"quota: {quota_str}\n"
        "\n"
        f"refresh token: {refresh_token}\n"
        f"access token: {access_token}\n"
        f"expire time: {expire_time}\n"
    )

    console = Console()
    console.print(_tempt, highlight=True)


def display_user_infos(
    *user_infos: Tuple[PcsUser, str, str], recent_user_id: Optional[int] = None
):
    """
    Args:
        user_infos (*Tuple[PcsUser, pwd: str])
    """

    table = Table(box=SIMPLE, show_edge=False, highlight=True)
    table.add_column("Index", justify="left")
    table.add_column("Recent", justify="left")
    table.add_column("Account Name", justify="left", overflow="fold")
    table.add_column("User Name", justify="left", overflow="fold")
    table.add_column("Nick Name", justify="left", overflow="fold")
    table.add_column("Quota", justify="left")
    table.add_column("VIP", justify="left", overflow="fold")
    table.add_column("pwd", justify="left", overflow="fold")

    for idx, (user_info, pwd, account_name) in enumerate(user_infos, 1):
        is_recent = "[green]✔[/green]" if user_info.user_id == recent_user_id else ""

        user_name = user_info.user_name
        nick_name = user_info.nick_name
        personal_space_info = user_info.personal_space_info
        vip = ""
        if user_info.user_vip_info:
            vip = f"[b red]{user_info.user_vip_info.identity}[/b red]"

        assert personal_space_info
        quota_str = (
            human_size(personal_space_info.used_size)
            + "/"
            + human_size(personal_space_info.total_size)
        )

        table.add_row(
            str(idx), is_recent, account_name, user_name, nick_name, quota_str, vip, pwd
        )

    console = Console()
    console.print(table)


def display_blocked_remotepath(remotepath: str):
    print(f"[i yellow]Remote path is blocked[/i yellow]: {remotepath}")
