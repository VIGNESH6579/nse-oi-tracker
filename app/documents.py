import ipaddress
import logging
from urllib.parse import urlparse

import requests

from .config import settings

log = logging.getLogger(__name__)

ALLOWED_HOSTS = {
    "nsearchives.nseindia.com",
    "www.nseindia.com",
    "nseindia.com",
    "www.bseindia.com",
    "bseindia.com",
    "beta.bseindia.com",
}
MAX_BYTES = 12 * 1024 * 1024
ALLOWED_TYPES = (
    "text/html",
    "application/xhtml+xml",
    "application/xml",
    "text/xml",
    "application/pdf",
    "application/octet-stream",
    "text/plain",
)


def _host_allowed(hostname: str) -> bool:
    host = (hostname or "").strip().lower().rstrip(".")
    if not host or host in {"localhost"}:
        return False
    try:
        ip = ipaddress.ip_address(host)
        return ip.is_global and not (ip.is_private or ip.is_loopback or ip.is_link_local)
    except ValueError:
        return host in ALLOWED_HOSTS


def is_official_document_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
    except Exception:
        return False
    if parsed.scheme not in {"http", "https"}:
        return False
    if parsed.username or parsed.password:
        return False
    return _host_allowed(parsed.hostname or "")


def fetch_official_document(url: str) -> tuple[bytes, str]:
    """Download an official NSE/BSE filing. Raises ValueError on policy failures."""
    if not is_official_document_url(url):
        raise ValueError("Document URL is not an allowed NSE/BSE host")
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml,application/pdf;q=0.9,*/*;q=0.8",
    }
    host = (urlparse(url).hostname or "").lower()
    if "bseindia.com" in host:
        headers["Referer"] = "https://www.bseindia.com/"
        headers["Origin"] = "https://www.bseindia.com"
    else:
        headers["Referer"] = "https://www.nseindia.com/"
        headers["Origin"] = "https://www.nseindia.com"
    with requests.Session() as session:
        session.max_redirects = 3
        response = session.get(url, headers=headers, timeout=settings.nse_timeout, stream=True)
        response.raise_for_status()
        final_host = urlparse(response.url).hostname or ""
        if not _host_allowed(final_host):
            raise ValueError("Redirected away from official exchange hosts")
        content_type = (response.headers.get("Content-Type") or "").split(";")[0].strip().lower()
        if content_type and content_type not in ALLOWED_TYPES:
            raise ValueError(f"Unsupported document content type: {content_type}")
        chunks = []
        total = 0
        for chunk in response.iter_content(chunk_size=65536):
            if not chunk:
                continue
            total += len(chunk)
            if total > MAX_BYTES:
                raise ValueError("Document exceeded size limit")
            chunks.append(chunk)
        return b"".join(chunks), content_type or "application/octet-stream"
