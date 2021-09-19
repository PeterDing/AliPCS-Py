from alipcs_py.alipcs import AliPCSApi
from alipcs_py.commands.display import display_user_info


def show_user_info(api: AliPCSApi):
    user_info = api.user_info()
    print(user_info)
    display_user_info(user_info)
