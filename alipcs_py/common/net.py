import socket

import requests
import requests.adapters


def avail_port(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("localhost", port)) != 0


def random_avail_port() -> int:
    """Find a random available port using port 0

    https://www.lifewire.com/port-0-in-tcp-and-udp-818145
    Port 0 is a wildcard port that tells the system to find a suitable port number.
    Unix, Windows, and other operating systems vary in the handling of port 0, but
    the same general convention applies.
    """

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        s.listen()
        _, port = s.getsockname()
        return port


def make_http_session(
    max_keepalive_connections: int = 50,
    max_connections: int = 50,
    keepalive_expiry: float = 10 * 60,
    max_retries: int = 2,
) -> requests.Session:
    """Make a http session with keepalive connections, maximum connections and retries"""

    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=max_keepalive_connections,
        pool_maxsize=max_connections,
        max_retries=max_retries,
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session
