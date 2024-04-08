import os
from pathlib import Path

from alipcs_py import AliPCS, AliPCSApi
from alipcs_py.commands.upload import upload, from_tos
from alipcs_py.common.platform import IS_WIN

from tests.datas import REFRESH_TOKEN, Datas

import pytest


TEST_ROOT = "/AliPCS-Py-test"
LOCAL_DIR = Path("tests", "test-datas", "demo-directory")
TEST_DATA_PATH = LOCAL_DIR.parent / "demo-directory.tar.gz"


@pytest.fixture(scope="session")
def uncompress_test_data():
    if LOCAL_DIR.exists():
        if IS_WIN:
            os.system(f"rd /s /q {LOCAL_DIR}")
        else:
            os.system(f"rm -rf {LOCAL_DIR}")

    assert TEST_DATA_PATH.exists()
    if IS_WIN:
        os.system(f"tar -xf {TEST_DATA_PATH} -C tests\\test-datas")
    else:
        os.system(f"tar -xf {TEST_DATA_PATH} -C tests/test-datas")

    yield

    if LOCAL_DIR.exists():
        if IS_WIN:
            os.system(f"rd /s /q {LOCAL_DIR}")
        else:
            os.system(f"rm -rf {LOCAL_DIR}")


@pytest.fixture(scope="session")
def alipcsapi(uncompress_test_data) -> AliPCSApi:
    return AliPCSApi(refresh_token=REFRESH_TOKEN)


@pytest.fixture(scope="session")
def alipcs(alipcsapi: AliPCSApi) -> AliPCS:
    return alipcsapi._alipcs


@pytest.fixture(scope="session")
def datas(alipcsapi: AliPCSApi):
    if REFRESH_TOKEN == "":
        return

    local_paths = []
    local_dir = LOCAL_DIR
    for root, _, files in os.walk(local_dir):
        for fl in files:
            local_paths.append(str(Path(root, fl)))

    remote_dir = TEST_ROOT + "/-------"
    remote_dir_pcs_file = alipcsapi.makedir_path(remote_dir)[0]
    from_paths = [str(local_dir / fn) for fn in os.listdir(local_dir)]
    from_to_list = from_tos(from_paths, remote_dir)

    upload(alipcsapi, from_to_list)

    yield Datas(
        local_dir=str(local_dir),
        local_paths=local_paths,
        remote_dir=remote_dir,
        remote_dir_pcs_file=remote_dir_pcs_file,
        remote_paths=[to_ for _, to_ in from_to_list],
    )

    pf = alipcsapi.meta_by_path(TEST_ROOT)
    assert pf is not None
    alipcsapi.remove(pf.file_id)
