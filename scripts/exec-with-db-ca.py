#!/usr/bin/env python3
"""Pin an injected PostgreSQL CA before executing a service."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


def main() -> int:
    if len(sys.argv) < 2:
        print("database TLS wrapper requires a command", file=sys.stderr)
        return 2
    runtime = os.environ.get("RUNTIME_DIRECTORY", "").strip()
    certificate = os.environ.get("AMC_DATABASE_CA_CERT", "")
    database_url = os.environ.get("AMC_DATABASE_URL", "").strip()
    if not runtime or not Path(runtime).is_absolute():
        print("database TLS wrapper requires an absolute runtime directory", file=sys.stderr)
        return 1
    if (
        "-----BEGIN CERTIFICATE-----" not in certificate
        or "-----END CERTIFICATE-----" not in certificate
        or "\x00" in certificate
        or len(certificate) > 1_000_000
    ):
        print("database CA certificate is missing or malformed", file=sys.stderr)
        return 1
    parsed = urlsplit(database_url)
    if parsed.scheme not in {
        "postgres",
        "postgresql",
        "postgres+psycopg",
        "postgresql+psycopg",
    }:
        print("AMC_DATABASE_URL must be PostgreSQL", file=sys.stderr)
        return 1

    certificate_path = Path(runtime) / "postgres-ca.crt"
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(certificate_path, flags, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        os.write(descriptor, certificate.rstrip().encode("utf-8") + b"\n")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)

    query = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if key.lower() not in {"sslmode", "sslrootcert"}
    ]
    query.extend(
        (
            ("sslmode", "verify-full"),
            ("sslrootcert", str(certificate_path)),
        )
    )
    secured_url = urlunsplit(
        (parsed.scheme, parsed.netloc, parsed.path, urlencode(query), parsed.fragment)
    )

    environment = os.environ.copy()
    environment["AMC_DATABASE_URL"] = secured_url
    environment["PGSSLMODE"] = "verify-full"
    environment["PGSSLROOTCERT"] = str(certificate_path)
    environment.pop("AMC_DATABASE_CA_CERT", None)
    os.execvpe(sys.argv[1], sys.argv[1:], environment)
    return 127  # pragma: no cover - os.execvpe never returns on success


if __name__ == "__main__":
    raise SystemExit(main())
