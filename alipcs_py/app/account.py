from typing import Optional, List, Dict
from dataclasses import dataclass
from pathlib import Path
from glob import glob
import os
import pickle

from alipcs_py.alipcs import AliPCSApi, PcsUser
from alipcs_py.common.path import PathType, join_path, is_dir
from alipcs_py.commands.env import (
    ACCOUNT_DATA_PATH,
    ACCOUNT_DATA_FILE_PREFIX,
    ACCOUNT_DATA_FILE_SUFFIX,
)


@dataclass
class Account:
    user: PcsUser

    # current working directory
    pwd: str = "/"
    encrypt_password: Optional[str] = None

    # The path to store the account
    path: Optional[str] = None

    # Is the account is used now
    on: bool = False

    def pcsapi(self) -> AliPCSApi:
        refresh_token = self.user.refresh_token
        access_token = self.user.access_token
        expire_time = self.user.expire_time or 0

        user_id = self.user.user_id or ""
        user_name = self.user.user_name or ""
        nick_name = self.user.nick_name or ""
        token_type = self.user.token_type or ""
        device_id = self.user.device_id or ""
        default_drive_id = self.user.default_drive_id or ""
        role = self.user.role or ""
        status = self.user.status or ""

        assert refresh_token, f"{self}.user.refresh_token is None"
        return AliPCSApi(
            refresh_token,
            access_token=access_token or "",
            expire_time=expire_time,
            user_id=user_id,
            user_name=user_name,
            nick_name=nick_name,
            token_type=token_type,
            device_id=device_id,
            default_drive_id=default_drive_id,
            role=role,
            status=status,
        )

    @staticmethod
    def from_refresh_token(
        refresh_token: str, access_token: str = "", expire_time: int = 0
    ) -> "Account":
        api = AliPCSApi(
            refresh_token, access_token=access_token, expire_time=expire_time
        )
        user = api.user_info()
        return Account(user)


class AccountManager:
    """Account Manager

    Manage all accounts
    """

    def __init__(self, data_path: Optional[PathType] = None):
        self._accounts: Dict[str, Account] = {}  # user_id (int) -> Account
        self._who: Optional[str] = None  # user_id (int)
        self._data_path = data_path

    @staticmethod
    def load_data(data_path: PathType) -> "AccountManager":
        data_path = Path(data_path).expanduser()
        am = AccountManager(data_path=data_path)

        account: Account
        if is_dir(data_path):
            template = (
                join_path(data_path, ACCOUNT_DATA_FILE_PREFIX)
                + "*"
                + ACCOUNT_DATA_FILE_SUFFIX
            )
            for path in glob(template):
                try:
                    account = pickle.load(open(path, "rb"))
                    assert account.user.user_id
                    am._accounts[account.user.user_id] = account
                    if account.on:
                        am._who = account.user.user_id
                except Exception:
                    pass
        else:
            account = pickle.load(open(data_path, "rb"))
            assert account.user.user_id
            am._accounts[account.user.user_id] = account
            am._who = account.user.user_id

        return am

    @property
    def accounts(self) -> List[Account]:
        """All accounts"""

        return list(self._accounts.values())

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

    def update(self, user_id: Optional[str] = None):
        """Update user_info"""

        user_id = user_id or self._who
        if user_id:
            account = self._accounts.get(user_id)
            if not account:
                return None

            api = account.pcsapi()
            user = api.user_info()

            account.user = user

    def su(self, user_id: str):
        """Change recent user with `PcsUser.user_id`

        Args:
            who (int): `PcsUser.user_id`
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

    def useradd(self, user: PcsUser):
        """Add an user to data"""

        assert user.user_id
        assert user.refresh_token

        account = Account(user)
        account.path = (
            join_path(ACCOUNT_DATA_PATH, ACCOUNT_DATA_FILE_PREFIX)
            + user.user_id
            + ACCOUNT_DATA_FILE_SUFFIX
        )

        self._accounts[user.user_id] = account
        self._switch(user.user_id)

    def userdel(self, user_id: str):
        """Delete a user

        Args:
            who (int): `PcsUser.user_id`
        """

        if user_id in self._accounts:
            account = self._accounts.pop(user_id)
            if account.path:
                os.remove(account.path)

        if user_id == self._who:
            self._who = None

    def save(self, user_id: Optional[str] = None):
        """Serialize the reccent used account to local path"""

        user_id = user_id or self._who

        assert user_id

        account = self._accounts[user_id]
        assert account.path
        data_path = Path(account.path)
        if not data_path.parent.exists():
            data_path.parent.mkdir(parents=True)

        pickle.dump(account, open(data_path, "wb"))
