from typing import Optional, List, Dict
from dataclasses import dataclass
from pathlib import Path
import pickle

from alipcs_py.alipcs import AliPCSApi, AliPCSApiMix, PcsUser
from alipcs_py.common.path import PathType, join_path


@dataclass
class Account:
    user: PcsUser

    # Account Name which can be set by hand
    account_name: str = ""

    # current working directory
    pwd: str = "/"
    encrypt_password: Optional[str] = None

    # Is the account is used now
    on: bool = False

    def pcsapi(self) -> AliPCSApi:
        web_refresh_token = self.user.web_refresh_token
        web_access_token = self.user.web_access_token
        web_token_type = self.user.web_token_type or "Bearer"
        web_expire_time = self.user.web_expire_time or 0

        openapi_refresh_token = self.user.openapi_refresh_token
        openapi_access_token = self.user.openapi_access_token
        openapi_token_type = self.user.openapi_token_type or "Bearer"
        openapi_expire_time = self.user.openapi_expire_time or 0

        client_id = self.user.client_id
        client_secret = self.user.client_secret
        client_server = self.user.client_server

        user_id = self.user.user_id or ""
        user_name = self.user.user_name or ""
        nick_name = self.user.nick_name or ""
        device_id = self.user.device_id or ""
        default_drive_id = self.user.default_drive_id or ""
        role = self.user.role or ""
        status = self.user.status or ""

        assert web_refresh_token, f"{self}.user.web_refresh_token is None"
        return AliPCSApiMix(
            web_refresh_token,
            web_access_token=web_access_token or "",
            web_token_type=web_token_type,
            web_expire_time=web_expire_time,
            openapi_refresh_token=openapi_refresh_token or "",
            openapi_access_token=openapi_access_token or "",
            openapi_token_type=openapi_token_type,
            openapi_expire_time=openapi_expire_time,
            client_id=client_id or "",
            client_secret=client_secret or "",
            client_server=client_server or "",
            user_id=user_id,
            user_name=user_name,
            nick_name=nick_name,
            device_id=device_id,
            default_drive_id=default_drive_id,
            role=role,
            status=status,
        )

    @staticmethod
    def from_refresh_token(
        web_refresh_token: str,
        web_access_token: str = "",
        web_token_type: str = "Bearer",
        web_expire_time: int = 0,
        openapi_refresh_token: str = "",
        openapi_access_token: str = "",
        openapi_token_type: str = "Bearer",
        openapi_expire_time: int = 0,
        client_id: str = "",
        client_secret: str = "",
        client_server: str = "",
        account_name: str = "",
    ) -> "Account":
        api = AliPCSApiMix(
            web_refresh_token,
            web_access_token=web_access_token or "",
            web_token_type=web_token_type,
            web_expire_time=web_expire_time,
            openapi_refresh_token=openapi_refresh_token or "",
            openapi_access_token=openapi_access_token or "",
            openapi_token_type=openapi_token_type,
            openapi_expire_time=openapi_expire_time,
            client_id=client_id or "",
            client_secret=client_secret or "",
            client_server=client_server or "",
        )
        user = api.user_info()

        return Account(
            user,
            account_name or user.user_id,  # Default use `user_id`
        )


class AccountManager:
    """Account Manager

    Manage all accounts
    """

    def __init__(self, data_path: Optional[PathType] = None):
        self._accounts: Dict[str, Account] = {}  # user_id (str) -> Account
        self._who: Optional[str] = None  # user_id (str)
        self._data_path = data_path
        self._apis: Dict[str, AliPCSApi] = {}

    @staticmethod
    def load_data(data_path: PathType) -> "AccountManager":
        try:
            data_path = Path(data_path).expanduser()
            am = pickle.load(data_path.open("rb"))
            am._data_path = data_path
            return am
        except Exception:
            return AccountManager(data_path=data_path)

    @property
    def accounts(self) -> List[Account]:
        """All accounts"""

        return list(self._accounts.values())

    def get_api(self, user_id: Optional[str] = None) -> Optional[AliPCSApi]:
        user_id = user_id or self._who
        if user_id:
            account = self._accounts.get(user_id)
            if not account:
                return None

            if user_id in self._apis:
                api = self._apis[user_id]
            else:
                api = account.pcsapi()
                self._apis[user_id] = api
            return api
        return None

    def set_account_name(self, account_name: str, user_id: Optional[str] = None):
        """Set account name"""

        user_id = user_id or self._who

        assert user_id, "No recent user"

        account = self._accounts.get(user_id)

        assert account

        account.account_name = account_name

    def set_encrypt_password(self, encrypt_password: Optional[str] = None):
        """Set encryption key"""

        assert self._who, "No recent user"

        account = self._accounts.get(self._who)

        assert account

        account.encrypt_password = encrypt_password

    def cd(self, remotedir: str = "/"):
        """Change current working directory"""

        assert self._who, "No recent user"

        account = self._accounts.get(self._who)

        assert account

        pwd = join_path(account.pwd, remotedir)
        account.pwd = pwd

    @property
    def pwd(self) -> str:
        """Current working directory of recent user"""

        assert self._who, "No recent user"

        account = self._accounts.get(self._who)

        assert account

        return account.pwd

    def who(self, user_id: Optional[str] = None) -> Optional[Account]:
        """Return recent `Account`"""

        user_id = user_id or self._who
        if user_id:
            return self._accounts.get(user_id)
        else:
            return None

    def refresh(self, user_id: Optional[str] = None):
        """Refresh the refresh_token and access_token"""

        user_id = user_id or self._who
        if user_id:
            api = self.get_api(user_id)
            if api:
                api.refresh()

    def update(self, user_id: Optional[str] = None):
        """Update user_info"""

        user_id = user_id or self._who
        if user_id:
            api = self.get_api(user_id)
            if api:
                account = self._accounts.get(user_id)
                assert account

                user = api.user_info()
                account.user = user

    def su(self, user_id: str):
        """Change recent user with `PcsUser.user_id`

        Args:
            user_id (str): `PcsUser.user_id`
        """

        assert user_id in self._accounts, f"No user {user_id}"

        self._switch(user_id)

    def _switch(self, user_id: str):
        for uid, account in list(self._accounts.items()):
            if uid == user_id:
                account.on = True
                self._who = account.user.user_id
            else:
                account.on = False

    def add_account(self, account: Account):
        """Add an account to the manager"""

        self._accounts[account.user.user_id] = account

    def delete_account(self, user_id: str):
        """Delete an account

        Args:
            user_id (str): `PcsUser.user_id`
        """

        if user_id in self._accounts:
            del self._accounts[user_id]
        if user_id == self._who:
            self._who = None

    def save(self, data_path: Optional[PathType] = None):
        """Serialize to local path"""

        data_path = data_path or self._data_path
        assert data_path, "No data path"

        data_path = Path(data_path).expanduser()
        if not data_path.parent.exists():
            data_path.parent.mkdir(parents=True, exist_ok=True)

        apis = self._apis
        self._apis = {}  # Ignore to save apis

        pickle.dump(self, open(data_path, "wb"))

        self._apis = apis
