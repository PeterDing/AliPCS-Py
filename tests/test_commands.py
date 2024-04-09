import os
import sys
import random
import time
import io
from pathlib import Path, PosixPath

from alipcs_py.alipcs import AliPCSApi
from alipcs_py.commands.list_files import list_files
from alipcs_py.commands.search import search
from alipcs_py.commands.file_operators import makedir, move, rename, copy, remove
from alipcs_py.commands.upload import upload, from_tos, _rapid_upload
from alipcs_py.commands.share import (
    list_shared,
    share_files,
    cancel_shared,
    save_shared,
    list_shared_files,
)
from alipcs_py.commands.user import show_user_info
from alipcs_py.commands.download import download, Downloader
from alipcs_py.commands.server import start_server
from alipcs_py.commands.crypto import decrypt_file
from alipcs_py.common.crypto import calc_proof_code, calc_sha1

import pytest
from faker import Faker

from alipcs_py.common.io import EncryptType, reset_encrypt_io

from tests.datas import REFRESH_TOKEN, Datas


fake = Faker()


class CaptureStdout:
    def __init__(self):
        self.sys_stdout = sys.stdout
        self.io = io.StringIO()
        sys.stdout = self.io

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        sys.stdout = self.sys_stdout

    def get_output(self):
        return self.io.getvalue()


class TestCommands:
    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_list_file(self, alipcsapi: AliPCSApi, datas: Datas):
        remote_dir = datas.remote_dir

        with CaptureStdout() as cs:
            list_files(
                alipcsapi,
                remote_dir,
                show_size=True,
                recursive=False,
                sifters=[],
                highlight=True,
                show_file_id=True,
                show_date=True,
            )

        output = cs.get_output()
        part1, part2 = remote_dir.rsplit("/", 1)
        assert part1 in output and part2 in output

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_search(self, alipcsapi: AliPCSApi, datas: Datas):
        remote_path = random.choice(datas.remote_paths)
        name = os.path.basename(remote_path)
        time.sleep(10)  # Wait for the file to be indexed

        with CaptureStdout() as cs:
            search(alipcsapi, name)

        output = cs.get_output()
        assert name in output

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_makedir(self, alipcsapi: AliPCSApi):
        path = "/test_makedir_cmd/test_makedir_cmd1/test_makedir_cmd2"
        with CaptureStdout() as cs:
            makedir(alipcsapi, path, show=True)

        output = cs.get_output()
        try:
            assert alipcsapi.get_file(remotepath=path) is not None
            assert path in output
        finally:
            remove(alipcsapi, "/".join(path.split("/")[:2]))

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_move(self, alipcsapi: AliPCSApi):
        from_path = "/test_move_cmd/test_move_cmd1/test_move_cmd2"
        to_path = "/test_move_cmd/tmp/test_move_cmd3"
        from_paths = alipcsapi.makedir_path(from_path)

        with CaptureStdout() as cs:
            move(alipcsapi, from_path, to_path, show=True)

        output = cs.get_output()
        try:
            assert alipcsapi.get_file(remotepath=to_path) is not None
            assert alipcsapi.get_file(remotepath=from_path) is None
            assert to_path in output
        finally:
            alipcsapi.remove(from_paths[-1].file_id)

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_rename(self, alipcsapi: AliPCSApi):
        path = "/test_rename_cmd/test_rename_cmd1"
        new_name = "test_rename_cmd2"
        from_paths = alipcsapi.makedir_path(path)

        with CaptureStdout() as cs:
            rename(alipcsapi, path, new_name, show=True)

        output = cs.get_output()
        try:
            assert alipcsapi.get_file(remotepath="/".join(path.split("/")[:-1] + [new_name])) is not None
            assert new_name in output
        finally:
            alipcsapi.remove(from_paths[-1].file_id)

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_copy(self, alipcsapi: AliPCSApi):
        from_path = "/test_copy_cmd/test_copy_cmd1/test_copy_cmd2"
        to_path = "/test_copy_cmd/tmp"
        from_paths = alipcsapi.makedir_path(from_path)

        with CaptureStdout() as cs:
            copy(alipcsapi, from_path, to_path, show=True)

        output = cs.get_output()
        try:
            pcs_file = alipcsapi.get_file(remotepath=to_path + "/test_copy_cmd2")
            assert pcs_file is not None
            assert pcs_file.file_id in output
        finally:
            alipcsapi.remove(from_paths[-1].file_id)

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_remove(self, alipcsapi: AliPCSApi):
        path = "/test_remove_cmd"
        paths = alipcsapi.makedir_path(path)
        remove(alipcsapi, path)
        assert not alipcsapi.exists(paths[0].file_id)
        assert alipcsapi.exists_in_trash(paths[0].file_id)

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_upload(self, alipcsapi: AliPCSApi, tmp_path: str):
        remotedir = "/test_upload_cmd"
        file_size = 1024 * 1024 * 10  # 10MB
        content = os.urandom(file_size)
        sha1 = calc_sha1(content)
        name = "test_upload_cmd"
        local_path = Path(tmp_path) / name
        with open(local_path, "wb") as f:
            f.write(content)

        # Upload file
        upload(alipcsapi, from_to_list=from_tos([local_path], remotedir), show_progress=False)
        try:
            pcs_file = alipcsapi.get_file(remotepath=remotedir + "/" + name)
            assert pcs_file is not None
            assert pcs_file.size == file_size
            assert pcs_file.rapid_upload_info is not None
            assert pcs_file.rapid_upload_info.content_hash.lower() == sha1.lower()
        finally:
            remove(alipcsapi, remotedir)

        # Rapid Upload
        file_io = open(local_path, "rb")
        slice1k_bytes = file_io.read(1024)
        reset_encrypt_io(file_io)
        slice1k_hash = calc_sha1(slice1k_bytes)

        remote_pcs_file = alipcsapi.makedir_path(remotedir + "/tmp")[0]

        pcs_prepared_file = alipcsapi.prepare_file(
            name,
            remote_pcs_file.file_id,
            file_size,
            slice1k_hash,
            part_number=1,
            check_name_mode="overwrite",
        )
        content_hash = calc_sha1(file_io)
        proof_code = calc_proof_code(file_io, file_size, alipcsapi.access_token)

        try:
            assert pcs_prepared_file.can_rapid_upload()
            assert _rapid_upload(
                alipcsapi,
                local_path.as_posix(),
                name,
                remote_pcs_file.file_id,
                content_hash,
                proof_code,
                file_size,
                check_name_mode="overwrite",
                task_id=None,
            )
            assert alipcsapi.get_file(remotepath=remotedir + "/tmp/" + name) is not None
        finally:
            remove(alipcsapi, remotedir)

        # Encrypt Upload
        password = b"1234"
        for enc_type in EncryptType:
            upload(
                alipcsapi,
                from_to_list=from_tos([local_path], remotedir),
                encrypt_password=password,
                encrypt_type=enc_type,
                show_progress=False,
            )
            try:
                pcs_file = alipcsapi.get_file(remotepath=remotedir + "/" + name)
                assert pcs_file is not None
                download(
                    alipcsapi, [pcs_file.path], localdir=Path(tmp_path, enc_type.value), encrypt_password=password
                )
                target_path = Path(tmp_path, enc_type.value, pcs_file.name)
                assert target_path.exists()
                target_sha1 = calc_sha1(target_path.read_bytes())
                assert target_sha1 == sha1
            finally:
                remove(alipcsapi, remotedir)

        # Upload directory
        upload(alipcsapi, from_to_list=from_tos([tmp_path], remotedir), show_progress=False)
        try:
            pcs_file = alipcsapi.get_file(remotepath=remotedir + "/" + os.path.basename(tmp_path) + "/" + name)
            assert pcs_file is not None
            assert pcs_file.size == file_size
            assert pcs_file.rapid_upload_info is not None
            assert pcs_file.rapid_upload_info.content_hash.lower() == sha1.lower()
        finally:
            remove(alipcsapi, remotedir)
            os.remove(local_path)

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_list_shared(self, alipcsapi: AliPCSApi):
        pass
        # share api changed, need to update

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_share(self, alipcsapi: AliPCSApi):
        pass
        # share api changed, need to update

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_cancel_shared(self, alipcsapi: AliPCSApi):
        pass
        # share api changed, need to update

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_save_shared(self, alipcsapi: AliPCSApi):
        pass
        # share api changed, need to update

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_list_shared_files(self, alipcsapi: AliPCSApi):
        pass
        # share api changed, need to update

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_show_user_info(self, alipcsapi: AliPCSApi):
        with CaptureStdout() as cs:
            show_user_info(alipcsapi)

        output = cs.get_output()
        assert alipcsapi.refresh_token in output

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_download(self, alipcsapi: AliPCSApi, datas: Datas, tmp_path):
        # Download file
        remote_path = random.choice(datas.remote_paths)
        download(alipcsapi, [remote_path], localdir=tmp_path, downloader=Downloader.me, show_progress=False)
        pcs_file = alipcsapi.get_file(remotepath=remote_path)
        assert pcs_file is not None
        assert pcs_file.rapid_upload_info is not None
        local_path = Path(tmp_path) / os.path.basename(remote_path)
        assert os.path.exists(local_path)
        sha1 = calc_sha1(local_path.open("rb"))
        assert sha1.lower() == pcs_file.rapid_upload_info.content_hash.lower()

        # Download directory
        remote_dir = datas.remote_dir
        download(
            alipcsapi,
            [remote_dir],
            localdir=tmp_path,
            downloader=Downloader.me,
            recursive=True,
            show_progress=False,
        )

        remote_dir_name = os.path.basename(remote_dir)
        remote_pcs_file = datas.remote_dir_pcs_file
        pcs_files = alipcsapi.walk(remote_pcs_file.file_id)
        for pcs_file in pcs_files:
            if pcs_file.is_dir:
                continue
            local_path = Path(tmp_path) / remote_dir_name / PosixPath(pcs_file.path)
            assert local_path.exists()
            sha1 = calc_sha1(local_path.open("rb"))
            assert pcs_file.rapid_upload_info is not None
            assert sha1.lower() == pcs_file.rapid_upload_info.content_hash.lower()

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_play(self, alipcsapi: AliPCSApi, datas: Datas):
        pass
        # No support at IC

    #
    # @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    # def test_http_server(self, alipcsapi: AliPCSApi, datas: Datas):
    #     print()
    #     start_server(alipcsapi, "/")
    #
    # @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    # def test_decrypt_file(self, alipcsapi: AliPCSApi, datas: Datas):
    #     decrypt_file("f60m", "f60m_dec", "CK-QEpQ)T@@P{kXV/GGw")
