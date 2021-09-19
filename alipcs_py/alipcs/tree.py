from typing import Dict, List, Iterable, Optional
from collections import OrderedDict

from alipcs_py.alipcs.api import AliPCSApi
from alipcs_py.alipcs.inner import PcsFile
from alipcs_py.common.path import split_posix_path


class _Node:
    def __init__(self, file_id: str, pcs_file: PcsFile):
        self.file_id = file_id
        self.pcs_file = pcs_file
        self.sub_nodes: OrderedDict[str, _Node] = OrderedDict()


class PathTree:
    def __init__(self, api: AliPCSApi):
        self._api = api
        self.root = _Node("root", PcsFile.root())

    def search(
        self, remotepath: str = "", topdown: Iterable[str] = [], root: _Node = None
    ) -> Optional[PcsFile]:
        if not topdown:
            assert remotepath.startswith("/")
            topdown = split_posix_path(remotepath)

        root = root or self.root
        return self._dfs(list(topdown), root)

    def _dfs(self, topdown: List[str], root: _Node) -> Optional[PcsFile]:
        if not topdown:
            return root.pcs_file

        next_key = topdown[0]
        root_pcs_file = root.pcs_file

        if next_key not in root.sub_nodes:
            for pf in self._api.list_iter(root_pcs_file.file_id):
                root.sub_nodes[pf.name] = _Node(pf.file_id, pf)
                if pf.name == next_key:
                    break

        if next_key not in root.sub_nodes:
            return None

        return self._dfs(topdown[1:], root.sub_nodes[next_key])
