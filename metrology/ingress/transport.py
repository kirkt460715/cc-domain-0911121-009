"""Loopback HTTP transport for binary gateway streams.

Only plain ``http`` URLs are supported and reads are performed in bounded
chunks so that a collection session can be cancelled promptly between reads.
The transport never parses the payload; framing is owned by
:mod:`metrology.ingress.binary`.
"""

import http.client
import urllib.parse
from ..errors import InputError


class CollectionCancelled(Exception):
    """Raised inside a session when its cancellation flag is observed."""


def split_url(url):
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme != "http" or not parsed.hostname or not parsed.port:
        raise InputError("gateway URL must be an explicit http host:port URL")
    if parsed.username is not None or parsed.password is not None:
        raise InputError("gateway URL must not carry credentials")
    return parsed


def iter_chunks(url, *, chunk_size=512, timeout=10, cancelled=None):
    """Yield bounded byte chunks from a gateway URL until the body ends."""
    if not isinstance(chunk_size, int) or not 1 <= chunk_size <= 65536:
        raise InputError("chunk size must be between one and 65536 bytes")
    parsed = split_url(url)
    target = parsed.path or "/"
    if parsed.query:
        target += "?" + parsed.query
    connection = http.client.HTTPConnection(
        parsed.hostname, parsed.port, timeout=timeout
    )
    try:
        connection.request("GET", target)
        response = connection.getresponse()
        if response.status != 200:
            raise InputError(
                "gateway responded with an error status",
                details={"status": response.status, "reason": response.reason},
            )
        while True:
            if cancelled is not None and cancelled():
                raise CollectionCancelled("gateway collection cancelled")
            chunk = response.read(chunk_size)
            if not chunk:
                break
            yield chunk
    finally:
        connection.close()
