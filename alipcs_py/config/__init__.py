from typing import Any
from types import SimpleNamespace
import os
import json

import toml  # type: ignore


def _to_buildin(obj: Any) -> Any:
    if isinstance(obj, SimpleNamespace):
        data = dict(obj.__dict__)
        for field in getattr(obj, "__annotations__", {}):
            val = getattr(obj, field)
            data[field] = _to_buildin(val)
        return data
    elif isinstance(obj, list):
        return [_to_buildin(item) for item in obj]
    elif isinstance(obj, dict):
        return {k: _to_buildin(v) for k, v in obj.items()}
    else:
        return obj


class Share(SimpleNamespace):
    """Share Configuration"""

    store: bool = False


class AppConfig(SimpleNamespace):
    """App Configuration"""

    share: Share = Share()

    @classmethod
    def load(cls, path: str):
        if os.path.exists(path):
            return json.loads(
                json.dumps(toml.load(path)),
                object_hook=lambda d: SimpleNamespace(**d),
            )
        else:
            return cls()

    def dumps(self):
        return toml.dumps(_to_buildin(self))

    def dump(self, path: str):
        with open(path, "w") as f:
            toml.dump(_to_buildin(self), f)
