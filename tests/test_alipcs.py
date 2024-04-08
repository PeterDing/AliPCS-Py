import os
from pathlib import Path
import time
import random

import pytest
import qrcode

from alipcs_py.alipcs import AliPCS, AliOpenPCS, AliOpenAuth

from tests.datas import REFRESH_TOKEN, Datas


class TestAliPCS:
    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_get_token(self, alipcs: AliPCS):
        info = alipcs.get_token()
        assert info["access_token"] != ""

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_refresh(self, alipcs: AliPCS):
        info = alipcs.refresh()
        assert info["access_token"] != ""

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_get_drive_info(self, alipcs: AliPCS):
        assert alipcs.device_id != ""

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_create_session(self, alipcs: AliPCS):
        info = alipcs.create_session()
        assert info["result"] and info["success"]

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_path_traceback(self, alipcs: AliPCS, datas: Datas):
        local_dir = datas.local_dir
        local_paths = datas.local_paths
        remote_dir = datas.remote_dir

        local_path = random.choice(local_paths)
        remote_path = Path(remote_dir) / local_path[len(local_dir) + 1 :]
        remote_path_posix = remote_path.as_posix()
        file_id = alipcs.meta_by_path(remote_path_posix)["file_id"]

        info = alipcs.path_traceback(file_id)
        wanted_path = Path("/", *[p["name"] for p in info["items"][::-1]])
        assert wanted_path == remote_path

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_meta_by_path(self, alipcs: AliPCS, datas: Datas):
        remote_dir = datas.remote_dir
        info = alipcs.meta_by_path(remote_dir)
        assert info["file_id"] != ""
        assert info["name"] == os.path.basename(remote_dir)

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_meta(self, alipcs: AliPCS, datas: Datas):
        pcs_file = datas.remote_dir_pcs_file
        info = alipcs.meta(pcs_file.file_id)
        assert info["file_id"] == pcs_file.file_id
        assert info["name"] == pcs_file.name

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_exists(self, alipcs: AliPCS, datas: Datas):
        pcs_file = datas.remote_dir_pcs_file
        assert alipcs.exists(pcs_file.file_id)

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_is_dir(self, alipcs: AliPCS, datas: Datas):
        pcs_file = datas.remote_dir_pcs_file
        assert alipcs.is_dir(pcs_file.file_id)

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_list(self, alipcs: AliPCS, datas: Datas):
        local_dir = datas.local_dir
        pcs_file = datas.remote_dir_pcs_file
        filenames = set(os.listdir(local_dir))
        info = alipcs.list(pcs_file.file_id)
        for v in info["items"]:
            assert v["name"] in filenames

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_walk(self, alipcs: AliPCS, datas: Datas):
        pcs_file = datas.remote_dir_pcs_file
        alipcs.walk(pcs_file.file_id)

        # More tests in test_alipcsapi.py

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_create_file(self, alipcs: AliPCS):
        pass
        # Tested in conftest.py

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_rapid_upload_file(self, alipcs: AliPCS):
        pass
        # Tested in test_alipcsapi.py

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_search(self, alipcs: AliPCS, datas: Datas):
        time.sleep(10)  # Wait for the file to be indexed

        local_paths = datas.local_paths
        local_path = random.choice(local_paths)
        name = os.path.basename(local_path)
        info = alipcs.search(name)
        assert any(v["name"] == name for v in info["items"])

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_makedir(self, alipcs: AliPCS):
        pass
        # Tested in conftest.py

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_move(self, alipcs: AliPCS, datas: Datas):
        pass
        # Tested in test_alipcsapi.py

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_rename(self, alipcs: AliPCS):
        pass
        # Tested in test_alipcsapi.py

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_copy(self, alipcs: AliPCS):
        pass
        # Tested in test_alipcsapi.py

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_remove(self, alipcs: AliPCS):
        pass
        # Tested in test_alipcsapi.py

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_share(self, alipcs: AliPCS):
        pass
        # Tested in test_alipcsapi.py

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_list_shared(self, alipcs: AliPCS):
        pass
        # Tested in test_alipcsapi.py

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_cancel_shared(self, alipcs: AliPCS):
        pass
        # Tested in test_alipcsapi.py

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_get_share_token(self, alipcs: AliPCS):
        pass
        # Tested in test_alipcsapi.py

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_shared_info(self, alipcs: AliPCS):
        pass
        # Tested in test_alipcsapi.py

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_list_shared_files(self, alipcs: AliPCS):
        pass
        # Tested in test_alipcsapi.py

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_transfer_shared_files(self, alipcs: AliPCS):
        pass
        # Tested in test_alipcsapi.py

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_shared_file_download_url(self, alipcs: AliPCS):
        pass
        # Tested in test_alipcsapi.py

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_user(self, alipcs: AliPCS):
        info = alipcs.user_info()
        assert info["user_id"] != ""

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_download_link(self, alipcs: AliPCS, datas: Datas):
        pass
        # Tested in test_alipcsapi.py

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_file_stream(self, alipcs: AliPCS):
        pass
        # Tested in test_alipcsapi.py

    @pytest.mark.skipif(not REFRESH_TOKEN, reason="No REFRESH_TOKEN")
    def test_shared_file_stream(self, alipcs: AliPCS):
        pass
        # Tested in test_alipcsapi.py

    # def test_qrcode_link(self, alipcs: AliPCS):
    #     ali_auth = AliOpenAuth(client_server=ALIYUNDRIVE_OPENAPI_AUTH_DOMAIN)
    #     info = ali_auth.get_qrcode_info()
    #     print(info)
    #
    # def test_qrcode_auth(self, alipcs: AliPCS):
    #     ali_auth = AliOpenAuth(client_server=ALIYUNDRIVE_OPENAPI_AUTH_DOMAIN)
    #
    #     # Get qrcode info
    #     info = ali_auth.get_qrcode_info()
    #     print(info)
    #     sid = info["sid"]
    #
    #     qrcode_url = f"https://www.aliyundrive.com/o/oauth/authorize?sid={sid}"
    #
    #     qr = qrcode.QRCode()
    #     qr.add_data(qrcode_url)
    #     f = io.StringIO()
    #     qr.print_ascii(out=f, tty=False, invert=True)
    #     f.seek(0)
    #     print(f.read())
    #
    #     while True:
    #         info = ali_auth.scan_status(sid)
    #         print(info)
    #         if info["status"] == "LoginSuccess":
    #             auth_code = info["authCode"]
    #             break
    #         time.sleep(2)
    #
    #     info = ali_auth.get_refresh_token(auth_code)
    #
    #     print(info)
