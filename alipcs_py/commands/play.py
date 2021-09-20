from typing import Optional, List, Dict
from enum import Enum
from pathlib import Path
import os
import shutil
import subprocess
import random
import time
from urllib.parse import quote

from alipcs_py.alipcs import AliPCSApi, PcsFile
from alipcs_py.commands.sifter import Sifter, sift
from alipcs_py.commands.download import USER_AGENT
from alipcs_py.commands.errors import CommandError
from alipcs_py.common.file_type import MEDIA_EXTS

_print = print

from rich import print


def _with_media_ext(path: str) -> bool:
    ext = os.path.splitext(path)[-1].lower()
    if ext in MEDIA_EXTS:
        return True
    else:
        return False


class Player(Enum):
    mpv = "mpv"  # https://mpv.io

    def which(self) -> Optional[str]:
        return shutil.which(self.value)

    def play(
        self,
        url: str,
        quiet: bool = False,
        player_params: List[str] = [],
        out_cmd: bool = False,
        use_local_server: bool = False,
    ):
        global DEFAULT_PLAYER
        if not self.which():
            print(
                f"[yellow]No player {self.name}[/yellow], using default player: {DEFAULT_PLAYER.name}"
            )
            self = DEFAULT_PLAYER
        if not self.which():
            raise CommandError(f"No player: {self.name}")

        if self == Player.mpv:
            cmd = self._mpv_cmd(
                url,
                quiet=quiet,
                player_params=player_params,
                use_local_server=use_local_server,
            )
        else:
            cmd = self._mpv_cmd(
                url,
                quiet=quiet,
                player_params=player_params,
                use_local_server=use_local_server,
            )

        # Print out command
        if out_cmd:
            _print(" ".join((repr(c) for c in cmd)))
            return

        returncode = self.spawn(cmd)
        if returncode != 0:
            print(
                f"[italic]{self.value}[/italic] fails. return code: [red]{returncode}[/red]"
            )

    def spawn(self, cmd: List[str], quiet: bool = False):
        child = subprocess.run(
            cmd,
            stdout=subprocess.DEVNULL if quiet else None,
        )
        return child.returncode

    def _mpv_cmd(
        self,
        url: str,
        quiet: bool = False,
        player_params: List[str] = [],
        use_local_server: bool = False,
    ):
        if use_local_server:
            cmd = [self.which(), url, *player_params]
        else:
            cmd = [
                self.which(),
                url,
                "--no-ytdl",
                "--http-header-fields="
                f"User-Agent: {USER_AGENT.replace(',', '')},Referer: https://www.aliyundrive.com/,Connection: Keep-Alive",
                *player_params,
            ]
        if quiet:
            cmd.append("--really-quiet")
        return cmd


DEFAULT_PLAYER = Player.mpv


def play_file(
    api: AliPCSApi,
    pcs_file: PcsFile,
    share_id: str = None,
    share_token: str = None,
    player: Player = DEFAULT_PLAYER,
    player_params: List[str] = [],
    quiet: bool = False,
    ignore_ext: bool = False,
    out_cmd: bool = False,
    local_server: str = "",
):
    if not ignore_ext and not _with_media_ext(pcs_file.name):
        return

    print(f"[italic blue]Play[/italic blue]: {pcs_file.path or pcs_file.name}")

    # For typing
    url: Optional[str] = None

    use_local_server = bool(local_server)

    if share_id:
        assert share_token, "Need share_token"

        use_local_server = False
        url = api.shared_file_download_url(pcs_file.file_id, share_id, share_token)
    elif use_local_server:
        url = f"{local_server}{quote(pcs_file.path)}"
        print("url:", url)
    else:
        if not pcs_file or not pcs_file.download_url:
            return
        url = pcs_file.download_url

    if url:
        player.play(
            url,
            quiet=quiet,
            player_params=player_params,
            out_cmd=out_cmd,
            use_local_server=use_local_server,
        )


def play_dir(
    api: AliPCSApi,
    pcs_file: PcsFile,
    share_id: str = None,
    share_token: str = None,
    sifters: List[Sifter] = [],
    recursive: bool = False,
    from_index: int = 0,
    player: Player = DEFAULT_PLAYER,
    player_params: List[str] = [],
    quiet: bool = False,
    shuffle: bool = False,
    ignore_ext: bool = False,
    out_cmd: bool = False,
    local_server: str = "",
):
    remotefiles = list(
        api.list_iter(pcs_file.file_id, share_id=share_id, share_token=share_token)
    )
    remotefiles = sift(remotefiles, sifters, recursive=recursive)

    if shuffle:
        rg = random.Random(time.time())
        rg.shuffle(remotefiles)

    for rp in remotefiles[from_index:]:
        if rp.is_file:
            play_file(
                api,
                rp,
                share_id=share_id,
                share_token=share_token,
                player=player,
                player_params=player_params,
                quiet=quiet,
                ignore_ext=ignore_ext,
                out_cmd=out_cmd,
                local_server=local_server,
            )
        else:  # is_dir
            if recursive:
                play_dir(
                    api,
                    rp,
                    share_id=share_id,
                    share_token=share_token,
                    sifters=sifters,
                    recursive=recursive,
                    from_index=from_index,
                    player=player,
                    player_params=player_params,
                    quiet=quiet,
                    shuffle=shuffle,
                    ignore_ext=ignore_ext,
                    out_cmd=out_cmd,
                    local_server=local_server,
                )


def play(
    api: AliPCSApi,
    remotepaths: List[str],
    file_ids: List[str],
    share_id: str = None,
    share_token: str = None,
    sifters: List[Sifter] = [],
    recursive: bool = False,
    from_index: int = 0,
    player: Player = DEFAULT_PLAYER,
    player_params: List[str] = [],
    quiet: bool = False,
    shuffle: bool = False,
    ignore_ext: bool = False,
    out_cmd: bool = False,
    local_server: str = "",
):
    """Play media file in `remotepaths`

    Args:
        `from_index` (int): The start index of playing entries from EACH remote directory
    """

    if shuffle:
        rg = random.Random(time.time())
        rg.shuffle(remotepaths)

    for rp in remotepaths:
        rpf = api.path(rp, share_id=share_id, share_token=share_token)
        if not rpf:
            print(f"[yellow]WARNING[/yellow]: `{rp}` does not exist.")
            continue

        if rpf.is_file:
            play_file(
                api,
                rpf,
                share_id=share_id,
                share_token=share_token,
                player=player,
                player_params=player_params,
                quiet=quiet,
                ignore_ext=ignore_ext,
                out_cmd=out_cmd,
                local_server=local_server,
            )
        else:
            play_dir(
                api,
                rpf,
                share_id=share_id,
                share_token=share_token,
                sifters=sifters,
                recursive=recursive,
                from_index=from_index,
                player=player,
                player_params=player_params,
                quiet=quiet,
                shuffle=shuffle,
                ignore_ext=ignore_ext,
                out_cmd=out_cmd,
                local_server=local_server,
            )

    for file_id in file_ids:
        rpf = api.meta(file_id, share_id=share_id, share_token=share_token)[0]
        if not rpf:
            print(f"[yellow]WARNING[/yellow]: file_id `{file_id}` does not exist.")
            continue

        if rpf.is_file:
            play_file(
                api,
                rpf,
                share_id=share_id,
                share_token=share_token,
                player=player,
                player_params=player_params,
                quiet=quiet,
                ignore_ext=ignore_ext,
                out_cmd=out_cmd,
                local_server=local_server,
            )
        else:
            play_dir(
                api,
                rpf,
                share_id=share_id,
                share_token=share_token,
                sifters=sifters,
                recursive=recursive,
                from_index=from_index,
                player=player,
                player_params=player_params,
                quiet=quiet,
                shuffle=shuffle,
                ignore_ext=ignore_ext,
                out_cmd=out_cmd,
                local_server=local_server,
            )
