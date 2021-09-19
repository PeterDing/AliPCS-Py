from typing import List

from alipcs_py.alipcs import AliPCSApi
from alipcs_py.commands.display import display_files
from alipcs_py.commands.sifter import Sifter, IncludeSifter


def search(
    api: AliPCSApi,
    keyword: str,
    sifters: List[Sifter] = None,
    highlight: bool = False,
    show_size: bool = False,
    show_date: bool = False,
    show_file_id: bool = False,
    show_hash: bool = False,
    csv: bool = False,
):
    pcs_files = api.search_all(keyword)

    sifters = [*(sifters or []), IncludeSifter(keyword)]
    display_files(
        pcs_files,
        sifters=sifters,
        highlight=highlight,
        show_size=show_size,
        show_date=show_date,
        show_file_id=show_file_id,
        show_hash=show_hash,
        show_absolute_path=True,
        csv=csv,
    )
