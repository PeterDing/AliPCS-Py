from typing import Optional, List, Tuple, Union
import os

# Enable UTF-8 Mode for Windows
# https://www.python.org/dev/peps/pep-0540/
if os.name == "nt":
    os.environ["PYTHONUTF8"] = "1"

from collections import OrderedDict
from functools import wraps
from multiprocessing import Process
from pathlib import Path
import signal
import time
import logging
import traceback

from alipcs_py import __version__
from alipcs_py.alipcs import AliPCSApi, AliPCSError
from alipcs_py.app import account as account_module
from alipcs_py.app.account import Account, AccountManager
from alipcs_py.commands.env import CONFIG_PATH, ACCOUNT_DATA_PATH, SHARED_STORE_PATH
from alipcs_py.common.progress_bar import _progress, init_progress_bar
from alipcs_py.common.path import join_path
from alipcs_py.common.net import random_avail_port
from alipcs_py.common.io import EncryptType
from alipcs_py.common.event import keyboard_listener_start
from alipcs_py.commands.sifter import (
    IncludeSifter,
    ExcludeSifter,
    IsFileSifter,
    IsDirSifter,
)
from alipcs_py.commands.display import (
    display_invalid_shared_link_infos,
    display_shared_files,
    display_shared_link_infos,
    display_user_info,
    display_user_infos,
)
from alipcs_py.commands.list_files import list_files
from alipcs_py.commands.cat import cat as _cat
from alipcs_py.commands import file_operators
from alipcs_py.commands.search import search as _search
from alipcs_py.commands.download import (
    download as _download,
    Downloader,
    DownloadParams,
    DEFAULT_DOWNLOADER,
    DEFAULT_CONCURRENCY,
    DEFAULT_CHUNK_SIZE,
)
from alipcs_py.commands.play import play as _play, Player, DEFAULT_PLAYER
from alipcs_py.commands.upload import upload as _upload, from_tos, CPU_NUM, UploadType
from alipcs_py.commands.sync import sync as _sync
from alipcs_py.commands import share as _share
from alipcs_py.commands.server import start_server
from alipcs_py.commands.log import get_logger
from alipcs_py.storage.store import AliPCSApiWithSharedStore, SharedStore
from alipcs_py.config import AppConfig
from alipcs_py.app.config import init_config

import click

from rich import print
from rich.console import Console
from rich.prompt import Prompt

logger = get_logger(__name__)

DEBUG = logger.level == logging.DEBUG


def _teardown():
    logger.debug("`app`: _teardown: start")

    from alipcs_py.common.event import _KEYBOARD_LISTENER

    if _KEYBOARD_LISTENER is not None:
        logger.debug("`app`: _teardown: reset old terminal")
        _KEYBOARD_LISTENER.set_normal_term()

    logger.debug("`app`: _teardown: end")


def _exit_progress_bar():
    if _progress.live._started:
        logger.debug("`app`: _exit_progress_bar: stop progress bar")

        print()
        # Stop _progress, otherwise terminal stdout will be contaminated
        _progress.stop()


def handle_signal(sign_num, frame):
    logger.debug("`app`: handle_signal: %s", sign_num)

    _exit_progress_bar()
    _teardown()

    # No use sys.exit() which only exits the main thread
    os._exit(1)


signal.signal(signal.SIGINT, handle_signal)


def handle_error(func):
    """Handle command error wrapper"""

    @wraps(func)
    def wrap(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except AliPCSError as err:
            _exit_progress_bar()

            logger.debug("`app`: AliPCSError: %s", traceback.format_exc())

            print(f"(v{__version__}) [bold red]ERROR[/bold red]: AliPCSError: {err}")
            if DEBUG:
                console = Console()
                console.print_exception()

        except Exception as err:
            _exit_progress_bar()

            logger.debug("`app`: System Error: %s", traceback.format_exc())

            print(f"(v{__version__}) [bold red]System ERROR[/bold red]: {err}")
            if DEBUG:
                console = Console()
                console.print_exception()

        finally:
            _exit_progress_bar()
            _teardown()

    return wrap


def _app_config(ctx):
    """App Configuration"""

    return ctx.obj.app_config


def _user_ids(ctx) -> Optional[List[int]]:
    """Select use_ids by their name probes"""

    am = ctx.obj.account_manager
    account_name_probes = ctx.obj.accounts

    user_ids = []
    for user_id, account in am._accounts.items():
        account_name = account.account_name
        for probe in account_name_probes:
            if probe in account_name:
                user_ids.append(user_id)
                break
    return user_ids


def _change_account(ctx, user_id: int):
    """Change recent account with `user_id`"""

    am = ctx.obj.account_manager
    am.su(user_id)


def multi_user_do(func):
    """Run command on multi users"""

    @wraps(func)
    def wrap(*args, **kwargs):
        ctx = args[0]
        user_ids = _user_ids(ctx)
        if not user_ids:
            return func(*args, **kwargs)

        am = ctx.obj.account_manager
        for user_id in user_ids:
            accout = am.who(user_id)
            if not accout:
                continue

            user_name = accout.user.user_name
            print(
                "[i yellow]@Do[/i yellow]: "
                f"user_name: [b]{user_name}[/b], "
                f"user_id: [b]{user_id}[/b]"
            )
            _change_account(ctx, user_id)
            func(*args, **kwargs)
            print()

    return wrap


def _recent_account(ctx) -> Optional[Account]:
    """Return recent user's `AliPCSApi`"""

    am = ctx.obj.account_manager
    account = am.who()
    if account:
        return account
    else:
        print("[italic red]No recent user, please adding or selecting one[/]")
        return None


def _recent_api(ctx) -> Union[AliPCSApi, AliPCSApiWithSharedStore, None]:
    """Return recent user's `AliPCSApi`"""

    app_config = _app_config(ctx)
    account = _recent_account(ctx)
    if account:
        api = account.pcsapi()
        if app_config.share.store:
            api._sharedstore = SharedStore()  # type: ignore
        return api
    else:
        return None


def _pwd(ctx) -> str:
    """Return recent user's pwd"""

    am = ctx.obj.account_manager
    return am.pwd


def _encrypt_password(ctx) -> bytes:
    """Return recent user's encryption key"""

    account = _recent_account(ctx)
    if account:
        ep = account.encrypt_password
        return bytes(ep or "", encoding="utf-8")
    else:
        return b""


ALIAS = OrderedDict(
    **{
        # Account
        "w": "who",
        "uu": "updateuser",
        "su": "su",
        "ul": "userlist",
        "ua": "useradd",
        "ud": "userdel",
        "ep": "encryptpwd",
        # File Operations
        "l": "ls",
        "f": "search",
        "md": "mkdir",
        "mv": "move",
        "rn": "rename",
        "cp": "copy",
        "rm": "remove",
        "d": "download",
        "p": "play",
        "u": "upload",
        "sn": "sync",
        # Share
        "S": "share",
        "sl": "shared",
        "cs": "cancelshared",
        "s": "save",
        "ssl": "storesharedlinks",
        "lsl": "listsharedlinks",
        "lsf": "listsharedfiles",
        "fsl": "findsharedlinks",
        "fsf": "findsharedfiles",
        "fs": "findshared",
        "dss": "deletestoredshared",
        "cst": "cleanstore",
        # Server
        "sv": "server",
    }
)


class AliasedGroup(click.Group):
    def get_command(self, ctx, cmd_name):
        # As normal command name
        rv = click.Group.get_command(self, ctx, cmd_name)
        if rv is not None:
            return rv

        # Check alias command name
        if cmd_name not in ALIAS:
            ctx.fail(f"No command: {cmd_name}")

        normal_cmd_name = ALIAS[cmd_name]
        return click.Group.get_command(self, ctx, normal_cmd_name)

    def list_commands(self, ctx):
        return self.commands


_APP_DOC = f"""AliPCS App v{__version__}

    \b
    如果第一次使用，你需要运行 `AliPCS-Py useradd` 添加 `refresh_token`。
    如何获取 `refresh_token` 见 https://github.com/PeterDing/AliPCS-Py#%E6%B7%BB%E5%8A%A0%E7%94%A8%E6%88%B7
    用 `AliPCS-Py {{command}} --help` 查看具体的用法。"""

_ALIAS_DOC = "Command 别名:\n\n\b\n" + "\n".join(
    [f"{alias: >3} : {cmd}" for alias, cmd in ALIAS.items()]
)


@click.group(cls=AliasedGroup, help=_APP_DOC, epilog=_ALIAS_DOC)
@click.option(
    "--config", "-c", type=str, default=CONFIG_PATH, help="Configuration file"
)
@click.option(
    "--account-data-path",
    "--adp",
    type=str,
    default=ACCOUNT_DATA_PATH,
    help="Account data file",
)
@click.option("--accounts", "-u", type=str, default=None, help="帐号名片段，用“,”分割")
@click.pass_context
def app(ctx, config, account_data_path, accounts):
    ctx.obj.account_manager = AccountManager.load_data(account_data_path)
    ctx.obj.accounts = [] if accounts is None else accounts.split(",")

    # Load app config
    app_config = AppConfig.load(config)
    ctx.obj.app_config = app_config
    init_config(app_config)

    if app_config.share.store:
        account_module.AliPCSApi = AliPCSApiWithSharedStore


# Account
# {{{
@app.command()
@click.argument("user_id", type=int, default=None, required=False)
@click.option("--show-encrypt-password", "-K", is_flag=True, help="显示加密密码")
@click.pass_context
@handle_error
def who(ctx, user_id, show_encrypt_password):
    """显示当前用户的信息

    也可指定 `user_id`
    """

    am = ctx.obj.account_manager
    account = am.who(user_id)
    if account:
        display_user_info(account.user)
        if show_encrypt_password:
            encrypt_password = _encrypt_password(ctx)

            print(f"[red]encrypt password[/red]: {encrypt_password}")
    else:
        print("[italic red]No recent user, please adding or selecting one[/]")


@app.command()
@click.argument("user_ids", type=int, nargs=-1, default=None, required=False)
@click.pass_context
@handle_error
def updateuser(ctx, user_ids):
    """更新用户信息 （默认更新当前用户信息）

    也可指定多个 `user_id`
    """

    am = ctx.obj.account_manager
    if not user_ids:
        user_ids = [am._who]

    for user_id in user_ids:
        am.update(user_id)
        account = am.who(user_id)
        if account:
            display_user_info(account.user)

    am.save()


@app.command()
@click.argument("user_index", type=int, default=None, required=False)
@click.pass_context
@handle_error
def su(ctx, user_index):
    """切换当前用户"""

    am = ctx.obj.account_manager
    ls = sorted(
        [(a.user, a.pwd, a.account_name) for a in am.accounts],
        key=lambda x: x[0].user_id,
    )
    display_user_infos(*ls, recent_user_id=am._who)

    if user_index:
        assert 0 < user_index < len(ls) + 1, f"Out range index {user_index}"

        i = user_index
        print(f"Select the user index: {i}")
    else:
        indexes = list(str(idx) for idx in range(1, len(ls) + 1))
        i = Prompt.ask("Select an user index", choices=indexes, default="")

        if not i:
            return

    user_id = ls[int(i) - 1][0].user_id
    am.su(user_id)
    am.save()


@app.command()
@click.pass_context
@handle_error
def userlist(ctx):
    """显示所有用户"""

    am = ctx.obj.account_manager
    ls = sorted(
        [(a.user, a.pwd, a.account_name) for a in am.accounts],
        key=lambda x: x[0].user_id,
    )
    display_user_infos(*ls, recent_user_id=am._who)


@app.command()
@click.option(
    "--account_name",
    prompt="Account Name",
    hide_input=False,
    default="",
    help="账号名 [默认为 user id]",
)
@click.option(
    "--refresh-token",
    prompt="refresh token",
    hide_input=True,
    help="用户 refresh_token",
)
@click.pass_context
@handle_error
def useradd(ctx, account_name, refresh_token):
    """添加一个用户并设置为当前用户"""

    assert refresh_token, "No `refresh_token`"

    account = Account.from_refresh_token(refresh_token, account_name=account_name)
    am = ctx.obj.account_manager
    am.add_account(account)
    am.su(account.user.user_id)
    am.save()


@app.command()
@click.pass_context
@handle_error
def userdel(ctx):
    """删除一个用户"""

    am = ctx.obj.account_manager
    ls = sorted(
        [(a.user, a.pwd, a.account_name) for a in am.accounts],
        key=lambda x: x[0].user_id,
    )
    display_user_infos(*ls, recent_user_id=am._who)

    indexes = list(str(idx) for idx in range(1, len(ls) + 1))
    i = Prompt.ask("Delete an user index", choices=indexes, default="")
    if not i:
        return

    user_id = ls[int(i) - 1][0].user_id
    am.delete_account(user_id)
    am.save()

    print(f"Delete user {user_id}")


@app.command()
@click.option(
    "--encrypt-password",
    "--ep",
    prompt="encrypt-password",
    hide_input=True,
    help="加密密码，任意字符",
)
@click.pass_context
@handle_error
def encryptpwd(ctx, encrypt_password):
    """设置加密密码"""

    assert len(encrypt_password) > 0, "No encrypt-password"

    am = ctx.obj.account_manager
    am.set_encrypt_password(encrypt_password)
    am.save()


@app.command()
@click.argument("remotedir", type=str, default="/", required=False)
@click.pass_context
@handle_error
def cd(ctx, remotedir):
    """切换当前工作目录"""

    am = ctx.obj.account_manager
    am.cd(remotedir)
    am.save()


@app.command()
@click.pass_context
@handle_error
@multi_user_do
def pwd(ctx):
    """显示当前工作目录"""

    pwd = _pwd(ctx)
    print(pwd)


# }}}

# File Operations
# {{{
@app.command()
@click.argument("remotepaths", nargs=-1, type=str)
@click.option("--file-id", "-i", multiple=True, type=str, help="文件 ID")
@click.option("--share-id", "--si", nargs=1, type=str, help="列出这个分享ID下的文件")
@click.option("--share-url", "--su", nargs=1, type=str, help="列出这个分享url下的文件")
@click.option("--password", "-p", type=str, help="分享链接密码，如果没有不用设置")
@click.option("--desc", "-r", is_flag=True, help="逆序排列文件")
@click.option("--name", "-n", is_flag=True, help="依名字排序")
@click.option("--time", "-t", is_flag=True, help="依时间排序")
@click.option("--size", "-s", is_flag=True, help="依文件大小排序")
@click.option("--recursive", "-R", is_flag=True, help="递归列出文件")
@click.option("--include", "-I", type=str, help="筛选包含这个字符串的文件")
@click.option("--include-regex", "--IR", type=str, help="筛选包含这个正则表达式的文件")
@click.option("--exclude", "-E", type=str, help="筛选 不 包含这个字符串的文件")
@click.option("--exclude-regex", "--ER", type=str, help="筛选 不 包含这个正则表达式的文件")
@click.option("--is-file", "-f", is_flag=True, help="筛选 非 目录文件")
@click.option("--is-dir", "-d", is_flag=True, help="筛选目录文件")
@click.option("--no-highlight", "--NH", is_flag=True, help="取消匹配高亮")
@click.option("--show-size", "-S", is_flag=True, help="显示文件大小")
@click.option("--show-date", "-D", is_flag=True, help="显示文件创建时间")
@click.option("--show-file-id", "--ID", is_flag=True, help="显示文件 ID")
@click.option("--show-hash", "-H", is_flag=True, help="显示文件 sha1")
@click.option("--show-absolute-path", "-A", is_flag=True, help="显示文件绝对路径")
@click.option("--show-dl-link", "--DL", is_flag=True, help="显示文件下载连接")
@click.option("--csv", is_flag=True, help="用 csv 格式显示，单行显示，推荐和 --DL 或 --HL 一起用")
@click.option("--only-dl-link", "--ODL", is_flag=True, help="只显示文件下载连接")
@click.pass_context
@handle_error
@multi_user_do
def ls(
    ctx,
    remotepaths,
    file_id,
    share_id,
    share_url,
    password,
    desc,
    name,
    time,
    size,
    recursive,
    include,
    include_regex,
    exclude,
    exclude_regex,
    is_file,
    is_dir,
    no_highlight,
    show_size,
    show_date,
    show_file_id,
    show_hash,
    show_absolute_path,
    show_dl_link,
    csv,
    only_dl_link,
):
    """列出网盘路径下的文件和对应的文件信息"""

    api = _recent_api(ctx)
    if not api:
        return

    sifters = []
    if include:
        sifters.append(IncludeSifter(include, regex=False))
    if include_regex:
        sifters.append(IncludeSifter(include_regex, regex=True))
    if exclude:
        sifters.append(ExcludeSifter(exclude, regex=False))
    if exclude_regex:
        sifters.append(ExcludeSifter(exclude_regex, regex=True))
    if is_file:
        sifters.append(IsFileSifter())
    if is_dir:
        sifters.append(IsDirSifter())

    if share_id or share_url:
        if not file_id and not remotepaths and "folder" not in (share_url or ""):
            remotepaths = ["/"]

        assert all([r.startswith("/") for r in remotepaths])

        _share.list_shared_files(
            api,
            *remotepaths,
            share_id=share_id,
            share_url=share_url,
            password=password,
            file_ids=file_id,
            desc=desc,
            name=name,
            time=time,
            size=size,
            recursive=recursive,
            sifters=sifters,
            highlight=not no_highlight,
            show_size=show_size,
            show_date=show_date,
            show_file_id=show_file_id,
            show_hash=show_hash,
            show_absolute_path=show_absolute_path,
            csv=csv,
        )
    else:
        pwd = _pwd(ctx)
        if not file_id and not remotepaths:
            remotepaths = [pwd]

        remotepaths = [join_path(pwd, r) for r in list(remotepaths)]

        list_files(
            api,
            *remotepaths,
            file_ids=file_id,
            desc=desc,
            name=name,
            time=time,
            size=size,
            recursive=recursive,
            sifters=sifters,
            highlight=not no_highlight,
            show_size=show_size,
            show_date=show_date,
            show_file_id=show_file_id,
            show_hash=show_hash,
            show_absolute_path=show_absolute_path,
            show_dl_link=show_dl_link,
            csv=csv,
            only_dl_link=only_dl_link,
        )


@app.command()
@click.argument("keyword", nargs=1, type=str)
@click.option("--include", "-I", type=str, help="筛选包含这个字符串的文件")
@click.option("--include-regex", "--IR", type=str, help="筛选包含这个正则表达式的文件")
@click.option("--exclude", "-E", type=str, help="筛选 不 包含这个字符串的文件")
@click.option("--exclude-regex", "--ER", type=str, help="筛选 不 包含这个正则表达式的文件")
@click.option("--is-file", "-f", is_flag=True, help="筛选 非 目录文件")
@click.option("--is-dir", "-d", is_flag=True, help="筛选目录文件")
@click.option("--no-highlight", "--NH", is_flag=True, help="取消匹配高亮")
@click.option("--show-size", "-S", is_flag=True, help="显示文件大小")
@click.option("--show-date", "-D", is_flag=True, help="显示文件创建时间")
@click.option("--show-hash", "-H", is_flag=True, help="显示文件 sha1")
@click.option("--csv", is_flag=True, help="用 csv 格式显示")
@click.pass_context
@handle_error
@multi_user_do
def search(
    ctx,
    keyword,
    include,
    include_regex,
    exclude,
    exclude_regex,
    is_file,
    is_dir,
    no_highlight,
    show_size,
    show_date,
    show_hash,
    csv,
):
    """搜索包含 `keyword` 的文件"""

    api = _recent_api(ctx)
    if not api:
        return

    sifters = []
    if include:
        sifters.append(IncludeSifter(include, regex=False))
    if include_regex:
        sifters.append(IncludeSifter(include_regex, regex=True))
    if exclude:
        sifters.append(ExcludeSifter(exclude, regex=False))
    if exclude_regex:
        sifters.append(ExcludeSifter(exclude_regex, regex=True))
    if is_file:
        sifters.append(IsFileSifter())
    if is_dir:
        sifters.append(IsDirSifter())

    _search(
        api,
        keyword,
        sifters=sifters,
        highlight=not no_highlight,
        show_size=show_size,
        show_date=show_date,
        show_file_id=True,
        show_hash=show_hash,
        csv=csv,
    )


@app.command()
@click.argument("remotepath", nargs=1, type=str)
@click.option("--encoding", "-e", type=str, help="文件编码，默认自动解码")
@click.option("--no-decrypt", "--ND", is_flag=True, help="不解密")
@click.option(
    "--encrypt-password", "--ep", type=str, default=None, help="加密密码，默认使用用户设置的"
)
@click.pass_context
@handle_error
@multi_user_do
def cat(ctx, remotepath, encoding, no_decrypt, encrypt_password):
    """显示文件内容"""

    api = _recent_api(ctx)
    if not api:
        return

    pwd = _pwd(ctx)
    remotepath = join_path(pwd, remotepath)

    if no_decrypt:
        encrypt_password = b""
    else:
        encrypt_password = encrypt_password or _encrypt_password(ctx)

    _cat(api, remotepath, encoding=encoding, encrypt_password=encrypt_password)


@app.command()
@click.argument("remotedirs", nargs=-1, type=str)
@click.option("--show", "-S", is_flag=True, help="显示目录")
@click.pass_context
@handle_error
@multi_user_do
def mkdir(ctx, remotedirs, show):
    """创建目录"""

    api = _recent_api(ctx)
    if not api:
        return

    pwd = _pwd(ctx)
    remotedirs = (join_path(pwd, d) for d in remotedirs)

    file_operators.makedir(api, *remotedirs, show=show)


@app.command()
@click.argument("remotepaths", nargs=-1, type=str)
@click.option("--show", "-S", is_flag=True, help="显示结果")
@click.pass_context
@handle_error
@multi_user_do
def move(ctx, remotepaths, show):
    """移动文件

    \b
    examples:
        move /file1 /file2 /to/dir
    """

    api = _recent_api(ctx)
    if not api:
        return

    pwd = _pwd(ctx)
    remotepaths = [join_path(pwd, r) for r in remotepaths]

    if len(remotepaths) < 2:
        ctx.fail("remote paths < 2")
    file_operators.move(api, *remotepaths, show=show)


@app.command()
@click.argument("remotepath", nargs=1, type=str)
@click.argument("new_name", nargs=1, type=str)
@click.option("--show", "-S", is_flag=True, help="显示结果")
@click.pass_context
@handle_error
@multi_user_do
def rename(ctx, remotepath, new_name, show):
    """文件重命名

    \b
    examples:
        rename /path/to/far new_name_foo
        # /path/to/far -> /path/to/new_name_foo
    """

    api = _recent_api(ctx)
    if not api:
        return

    pwd = _pwd(ctx)
    remotepath = join_path(pwd, remotepath)

    file_operators.rename(api, remotepath, new_name, show=show)


@app.command()
@click.argument("remotepaths", nargs=-1, type=str)
@click.option("--show", "-S", is_flag=True, help="显示结果")
@click.pass_context
@handle_error
@multi_user_do
def copy(ctx, remotepaths, show):
    """拷贝文件

    \b
    examples:
        copy /file1 /file2 /to/dir
    """

    api = _recent_api(ctx)
    if not api:
        return

    pwd = _pwd(ctx)
    remotepaths = [join_path(pwd, r) for r in remotepaths]

    if len(remotepaths) < 2:
        ctx.fail("remote paths < 2")
    file_operators.copy(api, *remotepaths, show=show)


@app.command()
@click.argument("remotepaths", nargs=-1, type=str)
@click.option("--file-id", "-i", multiple=True, type=str, help="文件 ID")
@click.pass_context
@handle_error
@multi_user_do
def remove(ctx, remotepaths, file_id):
    """删除文件"""

    api = _recent_api(ctx)
    if not api:
        return

    pwd = _pwd(ctx)
    remotepaths = (join_path(pwd, r) for r in remotepaths)

    file_operators.remove(api, *remotepaths)

    if file_id:
        api.remove(*file_id)


@app.command()
@click.argument("remotepaths", nargs=-1, type=str)
@click.option("--file-id", "-i", multiple=True, type=str, help="文件 ID")
@click.option("--outdir", "-o", nargs=1, type=str, default=".", help="指定下载本地目录，默认为当前目录")
@click.option("--share-id", "--si", nargs=1, type=str, help="下载这个分享ID下的文件")
@click.option("--share-url", "--su", nargs=1, type=str, help="下载这个分享url下的文件")
@click.option("--password", "-p", type=str, help="分享链接密码，如果没有不用设置")
@click.option("--recursive", "-R", is_flag=True, help="递归下载")
@click.option(
    "--from-index", "-f", type=int, default=0, help="从所有目录中的第几个文件开始下载，默认为0（第一个）"
)
@click.option("--include", "-I", type=str, help="筛选包含这个字符串的文件")
@click.option("--include-regex", "--IR", type=str, help="筛选包含这个正则表达式的文件")
@click.option("--exclude", "-E", type=str, help="筛选 不 包含这个字符串的文件")
@click.option("--exclude-regex", "--ER", type=str, help="筛选 不 包含这个正则表达式的文件")
@click.option(
    "-d",
    "--downloader",
    type=click.Choice([d.name for d in Downloader]),
    default=DEFAULT_DOWNLOADER.name,
    help="""指定下载应用

    \b
    默认为 me (AliPCS-Py 自己的下载器，支持断续下载)
        me 使用多文件并发下载。

    除 me 外，其他下载器，不使用多文件并发下载，使用一个文件多链接下载。

    \b
    aget_py (https://github.com/PeterDing/aget) 默认安装
    aget_rs (下载 https://github.com/PeterDing/aget-rs/releases)
    aria2 (下载 https://github.com/aria2/aria2/releases)
    """,
)
@click.option(
    "--concurrency",
    "-s",
    type=int,
    default=DEFAULT_CONCURRENCY,
    help=f"下载同步链接数，默认为{DEFAULT_CONCURRENCY}。建议小于 10",
)
@click.option(
    "--chunk-size", "-k", type=str, default=DEFAULT_CHUNK_SIZE, help="同步链接分块大小"
)
@click.option("--no-decrypt", "--ND", is_flag=True, help="不解密")
@click.option("--quiet", "-q", is_flag=True, help="取消第三方下载应用输出")
@click.option("--out-cmd", "--OC", is_flag=True, help="输出第三方下载应用命令")
@click.option(
    "--encrypt-password", "--ep", type=str, default=None, help="加密密码，默认使用用户设置的"
)
@click.pass_context
@handle_error
@multi_user_do
def download(
    ctx,
    remotepaths,
    file_id,
    outdir,
    share_id,
    share_url,
    password,
    recursive,
    from_index,
    include,
    include_regex,
    exclude,
    exclude_regex,
    downloader,
    concurrency,
    chunk_size,
    no_decrypt,
    quiet,
    out_cmd,
    encrypt_password,
):
    """下载文件"""

    if out_cmd:
        assert downloader != Downloader.me.name, "输出命令只能用于第三方下载应用"

    api = _recent_api(ctx)
    if not api:
        return

    sifters = []
    if include:
        sifters.append(IncludeSifter(include, regex=False))
    if include_regex:
        sifters.append(IncludeSifter(include_regex, regex=True))
    if exclude:
        sifters.append(ExcludeSifter(exclude, regex=False))
    if exclude_regex:
        sifters.append(ExcludeSifter(exclude_regex, regex=True))

    if no_decrypt:
        encrypt_password = b""
    else:
        encrypt_password = encrypt_password or _encrypt_password(ctx)

    if not quiet and downloader == Downloader.me.name:
        init_progress_bar()

    if share_id or share_url:
        assert all([r.startswith("/") for r in remotepaths])

        _share.download_shared(
            api,
            remotepaths,
            file_id,
            outdir,
            share_id=share_id,
            share_url=share_url,
            password=password,
            sifters=sifters,
            recursive=recursive,
            from_index=from_index,
            downloader=getattr(Downloader, downloader),
            downloadparams=DownloadParams(
                concurrency=concurrency, chunk_size=chunk_size, quiet=quiet
            ),
            out_cmd=out_cmd,
            encrypt_password=encrypt_password,
        )
    else:
        pwd = _pwd(ctx)
        remotepaths = [join_path(pwd, r) for r in remotepaths]
        _download(
            api,
            remotepaths,
            file_id,
            outdir,
            sifters=sifters,
            recursive=recursive,
            from_index=from_index,
            downloader=getattr(Downloader, downloader),
            downloadparams=DownloadParams(
                concurrency=concurrency, chunk_size=chunk_size, quiet=quiet
            ),
            out_cmd=out_cmd,
            encrypt_password=encrypt_password,
        )


@app.command()
@click.argument("remotepaths", nargs=-1, type=str)
@click.option("--file-id", "-i", multiple=True, type=str, help="文件 ID")
@click.option("--share-id", "--si", nargs=1, type=str, help="播放这个分享ID下的文件")
@click.option("--share-url", "--su", nargs=1, type=str, help="播放这个分享url下的文件")
@click.option("--password", "-p", type=str, help="分享链接密码，如果没有不用设置")
@click.option("--recursive", "-R", is_flag=True, help="递归播放")
@click.option(
    "--from-index", "-f", type=int, default=0, help="从所有目录中的第几个文件开始播放，默认为0（第一个）"
)
@click.option("--include", "-I", type=str, help="筛选包含这个字符串的文件")
@click.option("--include-regex", "--IR", type=str, help="筛选包含这个正则表达式的文件")
@click.option("--exclude", "-E", type=str, help="筛选 不 包含这个字符串的文件")
@click.option("--exclude-regex", "--ER", type=str, help="筛选 不 包含这个正则表达式的文件")
@click.option(
    "--player",
    "--pl",
    type=click.Choice([d.name for d in Player]),
    default=DEFAULT_PLAYER.name,
    help="""指定第三方播放器

    \b
    默认为 mpv (https://mpv.io),
    """,
)
@click.option("--player-params", "--PP", multiple=True, type=str, help="第三方播放器参数")
@click.option("--quiet", "-q", is_flag=True, help="取消第三方播放器输出")
@click.option("--shuffle", "--sf", is_flag=True, help="随机播放")
@click.option("--ignore-ext", "--IE", is_flag=True, help="不用文件名后缀名来判断媒体文件")
@click.option("--out-cmd", "--OC", is_flag=True, help="输出第三方播放器命令")
@click.option("--use-local-server", "-s", is_flag=True, help="使用本地服务器播放。")
@click.option(
    "--encrypt-password", "--ep", type=str, default=None, help="加密密码，默认使用用户设置的"
)
@click.pass_context
@handle_error
@multi_user_do
def play(
    ctx,
    remotepaths,
    file_id,
    share_id,
    share_url,
    password,
    recursive,
    from_index,
    include,
    include_regex,
    exclude,
    exclude_regex,
    player,
    player_params,
    quiet,
    shuffle,
    ignore_ext,
    out_cmd,
    use_local_server,
    encrypt_password,
):
    """播放媒体文件"""

    api = _recent_api(ctx)
    if not api:
        return

    sifters = []
    if include:
        sifters.append(IncludeSifter(include, regex=False))
    if include_regex:
        sifters.append(IncludeSifter(include_regex, regex=True))
    if exclude:
        sifters.append(ExcludeSifter(exclude, regex=False))
    if exclude_regex:
        sifters.append(ExcludeSifter(exclude_regex, regex=True))

    local_server = ""
    if use_local_server:
        if share_id or file_id:
            assert ValueError(
                "Recently local server can't play others shared items and using `file_id`"
            )

        encrypt_password = encrypt_password or _encrypt_password(ctx)

        host = "localhost"
        port = random_avail_port()

        local_server = f"http://{host}:{port}"

        ps = Process(
            target=start_server,
            args=(
                api,
                "/",
            ),
            kwargs=dict(
                host=host,
                port=port,
                workers=CPU_NUM,
                encrypt_password=encrypt_password,
                log_level="warning",
            ),
        )
        ps.start()
        time.sleep(1)

    if share_id or share_url:
        assert all([r.startswith("/") for r in remotepaths])

        _share.play_shared(
            api,
            remotepaths,
            file_ids=file_id,
            share_id=share_id,
            share_url=share_url,
            password=password,
            sifters=sifters,
            recursive=recursive,
            from_index=from_index,
            player=getattr(Player, player),
            player_params=player_params,
            quiet=quiet,
            shuffle=shuffle,
            ignore_ext=ignore_ext,
            out_cmd=out_cmd,
            local_server=local_server,
        )
    else:
        pwd = _pwd(ctx)
        remotepaths = [join_path(pwd, r) for r in list(remotepaths)]
        _play(
            api,
            remotepaths,
            file_ids=file_id,
            sifters=sifters,
            recursive=recursive,
            from_index=from_index,
            player=getattr(Player, player),
            player_params=player_params,
            quiet=quiet,
            shuffle=shuffle,
            ignore_ext=ignore_ext,
            out_cmd=out_cmd,
            local_server=local_server,
        )

    if use_local_server:
        ps.terminate()


@app.command()
@click.argument("localpaths", nargs=-1, type=str)
@click.argument("remotedir", nargs=1, type=str)
@click.option(
    "--upload-type",
    "-t",
    type=click.Choice([t.name for t in UploadType]),
    default=UploadType.Many.name,
    help=(
        "上传方式，Many: 同时上传多个文件，"
        "One: 一次只上传一个文件，但同时上传文件的多个分片 "
        "(阿里网盘不支持单文件并发上传。`upload --upload-type One` 失效)"
    ),
)
@click.option(
    "--encrypt-password", "--ep", type=str, default=None, help="加密密码，默认使用用户设置的"
)
@click.option(
    "--encrypt-type",
    "-e",
    type=click.Choice([t.name for t in EncryptType]),
    default=EncryptType.No.name,
    help="文件加密方法，默认为 No 不加密",
)
@click.option(
    "--max-workers", "-w", type=int, default=CPU_NUM, help="同时上传连接数量，默认为 CPU 核数"
)
@click.option("--no-ignore-existing", "--NI", is_flag=True, help="上传已经存在的文件")
@click.option("--no-show-progress", "--NP", is_flag=True, help="不显示上传进度")
@click.pass_context
@handle_error
@multi_user_do
def upload(
    ctx,
    localpaths,
    remotedir,
    upload_type,
    encrypt_password,
    encrypt_type,
    max_workers,
    no_ignore_existing,
    no_show_progress,
):
    """上传文件"""

    if upload_type == UploadType.One.name:
        raise ValueError("阿里网盘不支持单文件并发上传。`upload --upload-type One` 失效")

    # Keyboard listener start
    keyboard_listener_start()

    api = _recent_api(ctx)
    if not api:
        return

    encrypt_password = encrypt_password or _encrypt_password(ctx)
    if encrypt_type != EncryptType.No.name and not encrypt_password:
        raise ValueError(f"Encrypting with {encrypt_type} must have a key")

    pwd = _pwd(ctx)
    remotedir = join_path(pwd, remotedir)

    from_to_list = from_tos(localpaths, remotedir)

    if not no_show_progress:
        init_progress_bar()

    check_name_mode = "refuse" if not no_ignore_existing else "auto_rename"

    _upload(
        api,
        from_to_list,
        upload_type=getattr(UploadType, upload_type),
        check_name_mode=check_name_mode,
        encrypt_password=encrypt_password,
        encrypt_type=getattr(EncryptType, encrypt_type),
        max_workers=max_workers,
        show_progress=not no_show_progress,
    )


@app.command()
@click.argument("localdir", nargs=1, type=str)
@click.argument("remotedir", nargs=1, type=str)
@click.option(
    "--encrypt-password", "--ep", type=str, default=None, help="加密密码，默认使用用户设置的"
)
@click.option(
    "--encrypt-type",
    "-e",
    type=click.Choice([t.name for t in EncryptType]),
    default=EncryptType.No.name,
    help="文件加密方法，默认为 No 不加密",
)
@click.option("--max-workers", "-w", type=int, default=CPU_NUM, help="同时上传文件数")
@click.option("--no-show-progress", "--NP", is_flag=True, help="不显示上传进度")
@click.pass_context
@handle_error
@multi_user_do
def sync(
    ctx,
    localdir,
    remotedir,
    encrypt_password,
    encrypt_type,
    max_workers,
    no_show_progress,
):
    """同步本地目录到远端"""

    # Keyboard listener start
    keyboard_listener_start()

    api = _recent_api(ctx)
    if not api:
        return

    pwd = _pwd(ctx)
    remotedir = join_path(pwd, remotedir)

    encrypt_password = encrypt_password or _encrypt_password(ctx)
    if encrypt_type != EncryptType.No.name and not encrypt_password:
        raise ValueError(f"Encrypting with {encrypt_type} must have a key")

    _sync(
        api,
        localdir,
        remotedir,
        encrypt_password=encrypt_password,
        encrypt_type=getattr(EncryptType, encrypt_type),
        max_workers=max_workers,
        show_progress=not no_show_progress,
    )


# }}}


# Share
# {{{
@app.command()
@click.argument("remotepaths", nargs=-1, type=str)
@click.option("--password", "-p", type=str, default="", help="设置秘密，默认没有秘密")
@click.option("--period-time", "--pt", type=int, default=0, help="设置分享有效期，单位为天")
@click.pass_context
@handle_error
@multi_user_do
def share(ctx, remotepaths, password, period_time):
    """分享文件

    \b
    examples:
        share /path1 path2
    """

    api = _recent_api(ctx)
    if not api:
        return

    pwd = _pwd(ctx)
    remotepaths = (join_path(pwd, r) for r in remotepaths)

    _share.share_files(api, *remotepaths, password=password, period=period_time or 0)


@app.command()
@click.option("--show-all", "-A", is_flag=True, help="显示所有分享的链接，默认只显示有效的分享链接")
@click.pass_context
@handle_error
@multi_user_do
def shared(ctx, show_all):
    """列出分享链接"""

    api = _recent_api(ctx)
    if not api:
        return

    _share.list_shared(api, show_all=show_all)


@app.command()
@click.argument("share_ids", nargs=-1, type=str)
@click.pass_context
@handle_error
@multi_user_do
def cancelshared(ctx, share_ids):
    """取消分享链接"""

    api = _recent_api(ctx)
    if not api:
        return

    _share.cancel_shared(api, *share_ids)


@app.command()
@click.argument("share_url_or_id", nargs=1, type=str)
@click.argument("remotedir", nargs=1, type=str)
@click.option("--file-id", "-i", multiple=True, type=str, help="文件 ID")
@click.option("--password", "-p", type=str, help="分享链接密码，如果没有不用设置")
@click.pass_context
@handle_error
@multi_user_do
def save(ctx, share_url_or_id, file_id, remotedir, password):
    """保存其他用户分享的链接"""

    api = _recent_api(ctx)
    if not api:
        return

    pwd = _pwd(ctx)
    remotedir = join_path(pwd, remotedir)

    share_url = ""
    share_id = ""
    if "/s/" in share_url_or_id:
        share_url = share_url_or_id
    else:
        share_id = share_url_or_id

    _share.save_shared(
        api,
        remotedir,
        share_id=share_id,
        share_url=share_url,
        file_ids=file_id,
        password=password,
    )


@app.command()
@click.argument("share_urls_or_ids", nargs=-1, type=str)
@click.option("--password", "-p", type=str, help="分享链接密码，如果没有不用设置")
@click.pass_context
@handle_error
def storesharedlinks(ctx, share_urls_or_ids, password):
    """保存分享连接至本地

    注意: 使用这个命令必须将配置文件 ~/.alipcs-py/config.toml 中的 [share] store 设为 true
    """

    app_config = _app_config(ctx)
    if not app_config.share.store:
        print(
            "App configuration `[share] store is false`. So the command does not work"
        )
        return

    api = _recent_api(ctx)
    if not api:
        return

    for share_url_or_id in share_urls_or_ids:
        share_url = ""
        share_id = ""
        if "/s/" in share_url_or_id:
            share_url = share_url_or_id
        else:
            share_id = share_url_or_id

        _share.get_share_token(
            api, share_id=share_id, share_url=share_url, password=password
        )


@app.command()
@click.pass_context
@handle_error
def listsharedlinks(ctx):
    """显示本地保存的分享连接

    注意: 使用这个命令必须将配置文件 ~/.alipcs-py/config.toml 中的 [share] store 设为 true
    """

    shared_store = SharedStore()

    offset = 0
    while True:
        shared_links = shared_store.list_shared_links(offset=offset)
        display_shared_link_infos(*shared_links)
        yes = Prompt.ask("Next page", choices=["y", "n"], default="y")
        if yes == "y":
            offset += len(shared_links)
        else:
            break


@app.command()
@click.option("--share-id", "--si", multiple=True, type=str, help="指定显示 share id 下的文件")
@click.pass_context
@handle_error
def listsharedfiles(ctx, share_id):
    """显示本地保存的分享文件

    注意: 使用这个命令必须将配置文件 ~/.alipcs-py/config.toml 中的 [share] store 设为 true
    """

    shared_store = SharedStore()

    offset = 0
    while True:
        shared_files = shared_store.list_shared_files(share_ids=share_id, offset=offset)
        display_shared_files(*shared_files)
        yes = Prompt.ask("Next page", choices=["y", "n"], default="y")
        if yes == "y":
            offset += len(shared_files)
        else:
            break


def _find_shared_links(keywords: List[str]):
    shared_store = SharedStore()
    shared_links = shared_store.search_shared_links(*keywords)
    display_shared_link_infos(*shared_links)


@app.command()
@click.argument("keywords", nargs=-1, type=str)
@click.pass_context
@handle_error
def findsharedlinks(ctx, keywords):
    """查找本地保存的分享连接

    注意: 使用这个命令必须将配置文件 ~/.alipcs-py/config.toml 中的 [share] store 设为 true
    """

    _find_shared_links(keywords)


def _find_shared_files(
    keywords: List[str], share_ids: List[str] = [], verbose: bool = False
):
    shared_store = SharedStore()
    shared_files = shared_store.search_shared_files(*keywords, share_ids=share_ids)
    display_shared_files(*shared_files, verbose=verbose)


@app.command()
@click.argument("keywords", nargs=-1, type=str)
@click.option("--share-id", "--si", multiple=True, type=str, help="要搜索的 share id")
@click.option("--verbose", "-v", is_flag=True, help="显示细节")
@click.pass_context
@handle_error
def findsharedfiles(ctx, keywords, share_id, verbose):
    """查找本地保存的分享文件

    注意: 使用这个命令必须将配置文件 ~/.alipcs-py/config.toml 中的 [share] store 设为 true
    """

    _find_shared_files(keywords, share_ids=share_id, verbose=verbose)


@app.command()
@click.argument("keywords", nargs=-1, type=str)
@click.option("--verbose", "-v", is_flag=True, help="显示细节")
@click.pass_context
@handle_error
def findshared(ctx, keywords, verbose):
    """查找本地保存的分享连接和文件

    注意: 使用这个命令必须将配置文件 ~/.alipcs-py/config.toml 中的 [share] store 设为 true
    """

    _find_shared_links(keywords)
    print()
    _find_shared_files(keywords, verbose=verbose)


@app.command()
@click.argument("share_ids", nargs=-1, type=str)
@click.option(
    "--keyword", "-k", multiple=True, type=str, help="要删除文件名的关键字，如果为空则删除share_id下的所有文件"
)
@click.pass_context
@handle_error
def deletestoredshared(ctx, share_ids, keyword):
    """删除本地保存的分享连接或文件

    注意: 使用这个命令必须将配置文件 ~/.alipcs-py/config.toml 中的 [share] store 设为 true
    """

    if not share_ids:
        return

    shared_store = SharedStore()

    if not keyword:
        shared_links = []
        for share_id in share_ids:
            sli = shared_store.get_shared_link_info(share_id)
            if sli:
                shared_links.append(sli)
        display_shared_link_infos(*shared_links)
        yes = Prompt.ask(
            "Are you sure to delete these stored shared links and their files?",
            choices=["n", "Yes"],
            default="n",
        )
        if yes == "Yes":
            shared_store.delete_shared_links(*share_ids)
    else:
        shared_files = shared_store.search_shared_files(*keyword, share_ids=share_ids)
        display_shared_files(*shared_files)
        yes = Prompt.ask(
            "Are you sure to delete these stored shared files?",
            choices=["n", "Yes"],
            default="n",
        )
        if yes == "Yes":
            file_ids = [sf.file_id for sf, _ in shared_files]
            shared_store.delete_shared_files(*file_ids)


@app.command()
@click.pass_context
@handle_error
def cleanstore(ctx):
    """清理本地保存的无效分享连接

    注意: 使用这个命令必须将配置文件 ~/.alipcs-py/config.toml 中的 [share] store 设为 true
    """

    app_config = _app_config(ctx)
    if not app_config.share.store:
        print(
            "App configuration `[share] store is false`. So the command does not work"
        )
        return

    api: AliPCSApiWithSharedStore = _recent_api(ctx)
    if not api:
        return

    store = api.sharedstore
    if not store:
        return

    shared_links = store.list_shared_links()
    for shared_link in shared_links:
        if not api.is_shared_valid(shared_link.share_id):
            store.delete_shared_links(shared_link.share_id)
            display_invalid_shared_link_infos(shared_link)


# }}}

# Server
# {{{
@app.command()
@click.argument("root_dir", type=str, default="/", required=False)
@click.option("--path", type=str, default="/", help="服务路径，默认为 “/”")
@click.option("--host", "-h", type=str, default="localhost", help="监听 host")
@click.option("--port", "-p", type=int, default=8000, help="监听 port")
@click.option("--workers", "-w", type=int, default=CPU_NUM, help="进程数")
@click.option(
    "--encrypt-password", "--ep", type=str, default=None, help="加密密码，默认使用用户设置的"
)
@click.option("--username", type=str, default=None, help="HTTP Basic Auth 用户名")
@click.option("--password", type=str, default=None, help="HTTP Basic Auth 密钥")
@click.pass_context
@handle_error
@multi_user_do
def server(
    ctx, root_dir, path, host, port, workers, encrypt_password, username, password
):
    """开启 HTTP 服务"""

    api = _recent_api(ctx)
    if not api:
        return

    encrypt_password = encrypt_password or _encrypt_password(ctx)

    if username:
        assert password, "Must set password"

    start_server(
        api,
        root_dir=root_dir,
        path=path,
        host=host,
        port=port,
        workers=workers,
        encrypt_password=encrypt_password,
        username=username,
        password=password,
    )


# }}}
