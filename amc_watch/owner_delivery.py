"""Safe, dependency-free delivery for owner-only Discord incidents."""

from __future__ import annotations

import json
import re
import socket
import urllib.error
import urllib.request
from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping
from urllib.parse import urlsplit


MAX_DISCORD_CONTENT = 1_900
_CONTROL_CHARACTERS = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_URL = re.compile(r"(?i)\b(?:https?|socks5?)://[^\s<>]+")
_SECRET_ASSIGNMENT = re.compile(
    r"(?i)\b(token|password|passwd|secret|api[_-]?key|authorization)"
    r"\s*[:=]\s*([^\s,;]+)"
)
_DISCORD_MENTION = re.compile(r"@(everyone|here)|<@([!&]?\d+)>", re.IGNORECASE)


class DeliveryDisposition(str, Enum):
    """Whether an outbox item is complete, retryable, or permanently invalid."""

    DELIVERED = "delivered"
    RETRY = "retry"
    PERMANENT = "permanent"


@dataclass(frozen=True)
class DeliveryResult:
    disposition: DeliveryDisposition
    retry_after_seconds: float | None = None
    reason: str = ""


def sanitize_owner_text(value: object, *, limit: int = MAX_DISCORD_CONTENT) -> str:
    """Redact common secret/URL shapes and make Discord mentions inert."""

    text = _CONTROL_CHARACTERS.sub("", str(value or ""))
    text = _URL.sub("[redacted-url]", text)
    text = _SECRET_ASSIGNMENT.sub(lambda match: f"{match.group(1)}=[redacted]", text)
    text = _DISCORD_MENTION.sub("[mention removed]", text)
    text = text.strip()
    if len(text) > limit:
        text = text[: max(0, limit - 1)].rstrip() + "…"
    return text


def _retry_after(headers: Mapping[str, Any] | None, body: bytes) -> float | None:
    value: Any = None
    if headers:
        value = headers.get("Retry-After") or headers.get("retry-after")
    if value is None and body:
        try:
            payload = json.loads(body.decode("utf-8", errors="replace"))
            value = payload.get("retry_after") if isinstance(payload, dict) else None
        except (ValueError, TypeError):
            value = None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed < 0:
        return None
    return min(max(parsed, 1.0), 3_600.0)


class DiscordOwnerWebhook:
    """Send sanitized owner alerts and return a durable delivery decision."""

    def __init__(
        self,
        webhook_url: str,
        *,
        timeout_seconds: float = 10.0,
        opener: Any | None = None,
    ) -> None:
        parsed = urlsplit(webhook_url.strip())
        if (
            parsed.scheme != "https"
            or parsed.hostname != "discord.com"
            or not parsed.path.startswith("/api/webhooks/")
            or parsed.username
            or parsed.password
            or parsed.fragment
        ):
            raise ValueError("OWNER_DISCORD_WEBHOOK must be a discord.com HTTPS webhook")
        self._webhook_url = webhook_url.strip()
        self._timeout_seconds = timeout_seconds
        self._opener = opener or urllib.request.build_opener()

    def send(self, content: str) -> DeliveryResult:
        payload = json.dumps(
            {
                "content": sanitize_owner_text(content),
                "allowed_mentions": {
                    "parse": [],
                    "users": [],
                    "roles": [],
                    "replied_user": False,
                },
            },
            separators=(",", ":"),
        ).encode("utf-8")
        request = urllib.request.Request(
            self._webhook_url,
            data=payload,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "User-Agent": "AMC-Seat-Watch-Owner-Notifier/1.0",
            },
        )
        try:
            with self._opener.open(request, timeout=self._timeout_seconds) as response:
                status = int(response.getcode())
            if 200 <= status < 300:
                return DeliveryResult(DeliveryDisposition.DELIVERED)
            if status >= 500:
                return DeliveryResult(DeliveryDisposition.RETRY, reason=f"discord_http_{status}")
            return DeliveryResult(DeliveryDisposition.PERMANENT, reason=f"discord_http_{status}")
        except urllib.error.HTTPError as exc:
            # The response body is used only for Discord's retry hint and is never
            # surfaced in logs or stored in the incident ledger.
            body = exc.read(4_096)
            if exc.code == 429:
                return DeliveryResult(
                    DeliveryDisposition.RETRY,
                    retry_after_seconds=_retry_after(exc.headers, body),
                    reason="discord_http_429",
                )
            if exc.code in {408, 425, 500, 502, 503, 504}:
                return DeliveryResult(
                    DeliveryDisposition.RETRY,
                    retry_after_seconds=_retry_after(exc.headers, b""),
                    reason=f"discord_http_{exc.code}",
                )
            return DeliveryResult(
                DeliveryDisposition.PERMANENT,
                reason=f"discord_http_{exc.code}",
            )
        except (urllib.error.URLError, TimeoutError, socket.timeout, OSError):
            return DeliveryResult(DeliveryDisposition.RETRY, reason="transport_error")
