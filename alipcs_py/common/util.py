from typing import Dict, Any
import json


def json_dumps(obj: Any) -> str:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False, sort_keys=True)


def json_dump_values(obj: Dict[str, Any]) -> Dict[str, str]:
    """Convert dict's values which are list or dict as json string"""

    new_obj = dict(obj)
    for k, v in new_obj.items():
        if isinstance(v, (dict, list)):
            new_obj[k] = json.dumps(v)
    return new_obj
