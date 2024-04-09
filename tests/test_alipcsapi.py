from pathlib import Path, PosixPath
import os
import random
import time

from alipcs_py import AliPCSApi

import pytest
from rich import print

from tests.datas import REFRESH_TOKEN, Datas


class TestAliPCSApi:
    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_refresh_token(self, alipcsapi: AliPCSApi, datas: Datas):
        assert alipcsapi.refresh_token != ""

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_access_token(self, alipcsapi: AliPCSApi, datas: Datas):
        assert alipcsapi.access_token != ""

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_expire_time(self, alipcsapi: AliPCSApi, datas: Datas):
        assert alipcsapi.expire_time > 0

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_user_id(self, alipcsapi: AliPCSApi, datas: Datas):
        assert alipcsapi.user_id != ""

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_device_id(self, alipcsapi: AliPCSApi, datas: Datas):
        assert alipcsapi.device_id != ""

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_default_drive_id(self, alipcsapi: AliPCSApi, datas: Datas):
        assert alipcsapi.default_drive_id != ""

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_path_traceback(self, alipcsapi: AliPCSApi, datas: Datas):
        remote_path = random.choice(datas.remote_paths)
        pcs_file = alipcsapi.meta_by_path(remote_path)
        assert pcs_file is not None
        files = alipcsapi.path_traceback(pcs_file.file_id)
        assert remote_path == files[0].path

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_meta_by_path(self, alipcsapi: AliPCSApi, datas: Datas):
        remote_path = random.choice(datas.remote_paths)
        pcs_file = alipcsapi.meta_by_path(remote_path)
        assert pcs_file is not None
        assert pcs_file.path == remote_path

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_meta(self, alipcsapi: AliPCSApi, datas: Datas):
        pcs_file = datas.remote_dir_pcs_file
        pf = alipcsapi.meta(pcs_file.file_id)
        assert pf is not None
        assert pf.name == pf.path

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_get_file(self, alipcsapi: AliPCSApi, datas: Datas):
        remote_path = random.choice(datas.remote_paths)
        pcs_file = alipcsapi.get_file(remotepath=remote_path)
        assert pcs_file is not None
        assert pcs_file.path == remote_path

        pcs_file = datas.remote_dir_pcs_file
        pf = alipcsapi.get_file(file_id=pcs_file.file_id)
        assert pf is not None
        assert pf.name == pf.path

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_exists(self, alipcsapi: AliPCSApi, datas: Datas):
        pcs_file = datas.remote_dir_pcs_file
        assert alipcsapi.exists(pcs_file.file_id)
        assert not alipcsapi.exists(pcs_file.file_id[::-1])

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_is_file(self, alipcsapi: AliPCSApi, datas: Datas):
        pcs_file = datas.remote_dir_pcs_file
        assert not alipcsapi.is_file(pcs_file.file_id)

        remote_path = random.choice(datas.remote_paths)
        pcs_file = alipcsapi.meta_by_path(remote_path)
        assert pcs_file is not None
        assert alipcsapi.is_file(pcs_file.file_id)

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_is_dir(self, alipcsapi: AliPCSApi, datas: Datas):
        pass
        # Same as test_is_file

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_list(self, alipcsapi: AliPCSApi, datas: Datas):
        pcs_file = datas.remote_dir_pcs_file
        sub_pcs_files, _ = alipcsapi.list(pcs_file.file_id)
        local_dir = datas.local_dir
        for sub_pcs_file in sub_pcs_files:
            assert sub_pcs_file.path == sub_pcs_file.name
            assert Path(local_dir, sub_pcs_file.path).exists()

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_list_iter(self, alipcsapi: AliPCSApi, datas: Datas):
        pcs_file = datas.remote_dir_pcs_file
        sub_pcs_files = list(alipcsapi.list_iter(pcs_file.file_id, recursive=True, include_dir=True))
        local_dir = datas.local_dir
        for sub_pcs_file in sub_pcs_files:
            assert not sub_pcs_file.path.startswith(pcs_file.name)
            assert Path(local_dir, PosixPath(sub_pcs_file.path)).exists()

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_path(self, alipcsapi: AliPCSApi, datas: Datas):
        remote_path = sorted(datas.remote_paths, key=lambda x: len(x))[-1]
        pcs_file = alipcsapi.path(remote_path)
        assert pcs_file is not None
        assert remote_path == pcs_file.path

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_paths(self, alipcsapi: AliPCSApi, datas: Datas):
        pass
        # Tested in test_path

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_list_path_iter(self, alipcsapi: AliPCSApi, datas: Datas):
        pass
        # Deprecated

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_list_path(self, alipcsapi: AliPCSApi, datas: Datas):
        pass
        # Deprecated

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_walk(self, alipcsapi: AliPCSApi, datas: Datas):
        remote_dir_pcs_file = datas.remote_dir_pcs_file
        remote_dir = datas.remote_dir
        local_dir = datas.local_dir
        remote_paths = set(datas.remote_paths)
        wanted_paths = set()
        for pcs_file in alipcsapi.walk(remote_dir_pcs_file.file_id):
            assert Path(local_dir, pcs_file.path).exists()
            if pcs_file.is_file:
                wanted_paths.add(remote_dir + "/" + pcs_file.path)
        assert wanted_paths == remote_paths

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_create_file(self, alipcsapi: AliPCSApi, datas: Datas):
        pass
        # Tested in test_commands.py

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_prepare_file(self, alipcsapi: AliPCSApi, datas: Datas):
        pass
        # Tested in test_commands.py

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_get_upload_url(self, alipcsapi: AliPCSApi, datas: Datas):
        pass
        # Tested in test_commands.py

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_rapid_upload_file(self, alipcsapi: AliPCSApi, datas: Datas):
        pass
        # Tested in test_commands.py

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_upload_slice(self, alipcsapi: AliPCSApi, datas: Datas):
        pass
        # Tested in test_commands.py

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_upload_complete(self, alipcsapi: AliPCSApi, datas: Datas):
        pass
        # Tested in test_commands.py

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_search(self, alipcsapi: AliPCSApi, datas: Datas):
        remote_path = random.choice(datas.remote_paths)
        name = os.path.basename(remote_path)
        time.sleep(10)  # Wait for the file to be indexed
        assert any(pcs_file.name == name for pcs_file in alipcsapi.search(name)[0])

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_search_all(self, alipcsapi: AliPCSApi, datas: Datas):
        pass
        # Tested in test_search

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_makedir(self, alipcsapi: AliPCSApi, datas: Datas):
        name = "test_makedir1"
        pcs_file = alipcsapi.makedir("root", name)
        assert pcs_file is not None
        assert pcs_file.name == name
        alipcsapi.remove(pcs_file.file_id)

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_makedir_path(self, alipcsapi: AliPCSApi, datas: Datas):
        path = "/test_makedir_path2/test_makedir_path3/test_makedir_path4"
        pcs_files = alipcsapi.makedir_path(path)
        try:
            parts = path.split("/")
            for i in range(1, len(parts)):
                assert pcs_files[i - 1].path == "/".join(parts[: len(parts) - i + 1])
        finally:
            alipcsapi.remove(pcs_files[-1].file_id)

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_move(self, alipcsapi: AliPCSApi, datas: Datas):
        path = "/test_move/test_move1/test_move2"
        pcs_files = alipcsapi.makedir_path(path)
        try:
            result = alipcsapi.move(pcs_files[0].file_id, pcs_files[-1].file_id)
            assert all(result)

            assert alipcsapi.get_file(remotepath="/test_move/test_move2") is not None
            assert alipcsapi.get_file(remotepath="/test_move/test_move1/test_move2") is None
        finally:
            alipcsapi.remove(pcs_files[-1].file_id)

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_rename(self, alipcsapi: AliPCSApi, datas: Datas):
        path = "/test_rename/test_rename1/test_rename2"
        pcs_files = alipcsapi.makedir_path(path)
        try:
            pf = alipcsapi.rename(pcs_files[0].file_id, "test_rename3")
            assert pf is not None
            assert pf.name == "test_rename3"
            assert alipcsapi.get_file(remotepath=path) is None
            assert alipcsapi.get_file(remotepath=path.replace("2", "3")) is not None
        finally:
            pf = alipcsapi.get_file(remotepath="/test_rename")
            if pf is not None:
                alipcsapi.remove(pf.file_id)

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_copy(self, alipcsapi: AliPCSApi, datas: Datas):
        path = "/test_copy/test_copy1/test_copy2"
        pcs_files = alipcsapi.makedir_path(path)
        try:
            new_files = alipcsapi.copy(pcs_files[0].file_id, pcs_files[-1].file_id)
            assert len(new_files) == 1

            assert alipcsapi.get_file(remotepath="/test_copy/test_copy2") is not None
        finally:
            alipcsapi.remove(pcs_files[-1].file_id)

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_remove(self, alipcsapi: AliPCSApi, datas: Datas):
        path = "/test_remove/test_remove1/test_remove2"
        pcs_files = alipcsapi.makedir_path(path)
        try:
            assert alipcsapi.remove(pcs_files[0].file_id)
            assert alipcsapi.get_file(remotepath=path) is None
        finally:
            alipcsapi.remove(pcs_files[-1].file_id)

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_share(self, alipcsapi: AliPCSApi, datas: Datas):
        pass
        # share api changed, need to update

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_is_shared_valid(self, alipcsapi: AliPCSApi, datas: Datas):
        pass
        # share api changed, need to update

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_list_shared(self, alipcsapi: AliPCSApi, datas: Datas):
        pass
        # share api changed, need to update

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_list_shared_all(self, alipcsapi: AliPCSApi, datas: Datas):
        pass
        # share api changed, need to update

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_cancel_shared(self, alipcsapi: AliPCSApi, datas: Datas):
        pass
        # share api changed, need to update

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_get_share_token(self, alipcsapi: AliPCSApi, datas: Datas):
        pass
        # share api changed, need to update

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_shared_info(self, alipcsapi: AliPCSApi, datas: Datas):
        pass
        # share api changed, need to update

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_transfer_shared_files(self, alipcsapi: AliPCSApi, datas: Datas):
        pass
        # share api changed, need to update

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_shared_file_download_url(self, alipcsapi: AliPCSApi, datas: Datas):
        pass
        # share api changed, need to update

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_user_info(self, alipcsapi: AliPCSApi, datas: Datas):
        info = alipcsapi.user_info()
        assert info.user_id != ""

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_download_link(self, alipcsapi: AliPCSApi, datas: Datas):
        remote_path = random.choice(datas.remote_paths)
        pcs_file = alipcsapi.meta_by_path(remote_path)
        assert pcs_file is not None
        link = alipcsapi.download_link(pcs_file.file_id)
        assert link is not None
        assert link.download_url or link.url
        assert not link.expires()

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_update_download_url(self, alipcsapi: AliPCSApi, datas: Datas):
        remote_path = random.choice(datas.remote_paths)
        pcs_file = alipcsapi.meta_by_path(remote_path)
        assert pcs_file is not None
        pcs_file = alipcsapi.update_download_url(pcs_file)
        assert not pcs_file.download_url_expires()

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_file_stream(self, alipcsapi: AliPCSApi, datas: Datas):
        remote_path = random.choice(datas.remote_paths)
        remote_dir = datas.remote_dir
        local_path = Path(datas.local_dir, PosixPath(remote_path[len(remote_dir) + 1 :]))
        pcs_file = alipcsapi.meta_by_path(remote_path)
        assert pcs_file is not None
        stream = alipcsapi.file_stream(pcs_file.file_id)
        assert stream is not None
        assert stream.readable()
        assert stream.seekable()
        content = stream.read()
        assert content is not None
        assert len(content) == pcs_file.size
        assert content == local_path.read_bytes()

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_shared_file_stream(self, alipcsapi: AliPCSApi, datas: Datas):
        pass
        # share api changed, need to update
