from types import SimpleNamespace
from alipcs_py.app.app import app as _app


def main():
    _app(obj=SimpleNamespace())
