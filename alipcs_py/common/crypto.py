from typing import Union, List, Tuple, IO, Callable, Any
import re
import os
import subprocess
import random
from io import BufferedReader
from abc import ABC, abstractmethod
from zlib import crc32
import hashlib
from hashlib import md5, sha1
import base64

from passlib.crypto.digest import pbkdf1

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.padding import PKCS7
from cryptography.hazmat.backends import default_backend

from alipcs_py.common import constant
from alipcs_py.common.platform import IS_LINUX, IS_MACOS
from alipcs_py.common.simple_cipher import SimpleCryptography as _SimpleCryptography


def _md5_cmd(localpath: str) -> List[str]:
    if IS_MACOS:
        cmd = ["md5", localpath]
    elif IS_LINUX:
        cmd = ["md5sum", localpath]
    else:  # windows
        cmd = ["CertUtil", "-hashfile", localpath, "MD5"]
    return cmd


def calc_file_md5(localpath: str) -> str:
    cp = subprocess.run(
        _md5_cmd(localpath), universal_newlines=True, stdout=subprocess.PIPE
    )

    output = cp.stdout.strip()
    if IS_MACOS:
        return re.split(r"\s+", output)[-1]
    elif IS_LINUX:
        return re.split(r"\s+", output)[0]
    else:  # windows
        cn = output.split("CertUtil")[0].strip()
        cn = cn.split(":")[-1].strip().replace(" ", "")
        return cn


ReadableBuffer = Union[bytes, bytearray]  # stable


def calc_crc32_and_md5(stream: IO, chunk_size: int) -> Tuple[int, str]:
    md5_v = md5()
    crc32_v = 0
    while True:
        buf = stream.read(chunk_size)
        if buf:
            md5_v.update(buf)
            crc32_v = crc32(buf, crc32_v).conjugate()
        else:
            break
    return crc32_v.conjugate() & 0xFFFFFFFF, md5_v.hexdigest()


def calc_hash(
    hash_method: Callable, buf: Union[str, bytes, IO, BufferedReader], encoding="utf-8"
) -> str:
    assert isinstance(buf, (str, bytes, IO, BufferedReader))

    if isinstance(buf, str):
        buf = buf.encode(encoding)
        return hash_method(buf).hexdigest()
    elif isinstance(buf, bytes):
        return hash_method(buf).hexdigest()
    else:
        hasher = hash_method()
        while True:
            chunk = buf.read(constant.OneM)
            if not chunk:
                return hasher.hexdigest()
            hasher.update(chunk)


def calc_md5(buf: Union[str, bytes, IO, BufferedReader], encoding="utf-8") -> str:
    return calc_hash(md5, buf, encoding=encoding)


def calc_sha1(buf: Union[str, bytes, IO, BufferedReader], encoding="utf-8") -> str:
    return calc_hash(sha1, buf, encoding=encoding)


def calc_proof_code(io: IO, io_len: int, key: str) -> str:
    if io_len == 0:
        return ""

    key_md5 = calc_md5(key)
    offset = int("0x" + key_md5[:16], 16) % io_len
    pre_offset = io.tell()
    io.seek(offset, 0)
    buf = io.read(8) or b""
    io.seek(pre_offset, 0)
    return base64.b64encode(buf).decode("utf-8")


U8_LIST = list(range(1 << 8))


def random_bytes(size: int, seed: Any = None) -> bytes:
    """Generate random bytes"""

    rg = random.Random(seed)
    return bytes(rg.sample(U8_LIST, size))


def random_sys_bytes(size: int) -> bytes:
    """Generate random bytes with `os.urandom`"""

    return os.urandom(size)


def padding_key(
    key: Union[str, bytes], length: int = 0, value: bytes = b"\xff"
) -> bytes:
    """padding key with `value`"""

    assert len(value) < 2

    if isinstance(key, str):
        key = key.encode("utf-8")

    assert len(key) <= length

    pad_len = length - len(key)
    if value:
        pad_bytes = value * (pad_len)
    else:
        pad_bytes = random_sys_bytes(pad_len)
    return key + pad_bytes


def padding_size(length: int, block_size: int, ceil: bool = True) -> int:
    """Return minimum the multiple which is large or equal than the `length`

    Args:
        block_size (int): the length of bytes, no the length of bit
    """

    remainder = length % block_size
    if ceil:
        return (block_size - remainder) * int(remainder != 0) + length
    else:
        return length - remainder


def pkcs7_padding(data: bytes, block_size):
    """
    Args:
        block_size (int): the length of bytes, no the length of bit
    """

    padder = PKCS7(block_size * 8).padder()
    return padder.update(data) + padder.finalize()


def pkcs7_unpadding(data: bytes, block_size):
    """
    Args:
        block_size (int): the length of bytes, no the length of bit
    """

    unpadder = PKCS7(block_size * 8).unpadder()
    return unpadder.update(data) + unpadder.finalize()


def generate_salt(size: int = 8) -> bytes:
    return random_sys_bytes(size)


# Generate key and iv with password and salt
# https://security.stackexchange.com/a/117654
# {{{
def generate_key_iv(
    password: bytes, salt: bytes, key_size: int, iv_size: int, algo: str = "md5"
) -> Tuple[bytes, bytes]:
    def hasher(algo: str, data: bytes) -> bytes:
        hashes = {
            "md5": hashlib.md5,
            "sha256": hashlib.sha256,
            "sha512": hashlib.sha512,
        }
        h = hashes[algo]()
        h.update(data)
        return h.digest()

    if algo == "md5":
        temp = pbkdf1("md5", password, salt, 1, 16)
    else:
        temp = b""

    fd = temp
    while len(fd) < key_size + iv_size:
        temp = hasher(algo, temp + password + salt)
        fd += temp

    key = fd[0:key_size]
    iv = fd[key_size : key_size + iv_size]

    return key, iv


# }}}


class Cryptography(ABC):
    @abstractmethod
    def encrypt(self, data: bytes) -> bytes:
        pass

    @abstractmethod
    def decrypt(self, data: bytes) -> bytes:
        pass

    @abstractmethod
    def reset(self):
        pass

    @abstractmethod
    def finalize(self):
        """Finalize encryptor and decryptor, no return data"""


class SimpleCryptography(Cryptography):
    """Simple Cryptography

    This crypto algorithm uses a random uint8 map to transfer an uint8 to another uint8.
    So, the decryption process does not depend on previous decrypted data.

    The algorithm is vulnerable, so NO using to encrypt important data.
    """

    def __init__(self, key):
        self._c = _SimpleCryptography(key)
        self._key = key

    def encrypt(self, data: bytes) -> bytes:
        return self._c.encrypt(data)

    def decrypt(self, data: bytes) -> bytes:
        return self._c.decrypt(data)

    def reset(self):
        pass

    def finalize(self):
        pass


class ChaCha20Cryptography(Cryptography):
    """ChaCha20 Cryptography

    ChaCha20 stream algorithm.

    The decryption process does depend on previous decrypted data.
    """

    def __init__(self, key: bytes, nonce: bytes):
        assert len(key) == 32
        assert len(nonce) == 16

        self._key = key
        self._nonce = nonce
        self.reset()

    def encrypt(self, data: bytes) -> bytes:
        return self._encryptor.update(data)

    def decrypt(self, data: bytes) -> bytes:
        return self._decryptor.update(data)

    def reset(self):
        cipher = Cipher(
            algorithms.ChaCha20(self._key, self._nonce),
            mode=None,
            backend=default_backend(),
        )
        self._encryptor = cipher.encryptor()
        self._decryptor = cipher.decryptor()

    def finalize(self):
        self._encryptor.finalize()
        self._decryptor.finalize()


class AES256CBCCryptography(Cryptography):
    def __init__(self, key: bytes, iv: bytes):
        assert len(key) == 32
        assert len(iv) == 16

        self._key = key
        self._iv = iv
        self._mode = modes.CBC(iv)
        self.reset()

    def encrypt(self, data: bytes) -> bytes:
        assert len(data) % 16 == 0
        return self._encryptor.update(data)

    def decrypt(self, data: bytes) -> bytes:
        return self._decryptor.update(data)

    def reset(self):
        cipher = Cipher(algorithms.AES(self._key), mode=self._mode)
        self._encryptor = cipher.encryptor()
        self._decryptor = cipher.decryptor()

    def finalize(self):
        self._encryptor.finalize()
        self._decryptor.finalize()


def aes256cbc_encrypt(data: bytes, key: bytes, iv: bytes):
    crypto = AES256CBCCryptography(key, iv)
    return crypto.encrypt(data) + crypto._encryptor.finalize()


def aes256cbc_decrypt(data: bytes, key: bytes, iv: bytes):
    crypto = AES256CBCCryptography(key, iv)
    return crypto.decrypt(data) + crypto._decryptor.finalize()
