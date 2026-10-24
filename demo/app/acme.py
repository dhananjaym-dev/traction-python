"""A thin HTTP client for the Acme vendor simulator (the companion repo's acme-vendor). Every
failure is thrown as one exception per status family, chosen by status; it knows nothing about
Traction."""

from __future__ import annotations

import httpx

CONTACTS = "/contacts"
COMPANIES = "/companies"

TIMEOUT_S = 2.0


class AcmeError(Exception):
    def __init__(self, status: int, body: str):
        super().__init__(f"{status}: {body}")
        self.status = status
        self.body = body


class AuthError(AcmeError):
    pass


class ContractError(AcmeError):
    pass


class BadPayloadError(AcmeError):
    pass


class RateLimitedError(AcmeError):
    pass


class VendorUnavailableError(AcmeError):
    pass


def error_for(status: int, body: str) -> Exception:
    """The exception for an error status (status 0 = transport failure)."""
    if status == 401:
        return AuthError(status, body)
    if status == 422:
        return ContractError(status, body)
    if status == 400:
        return BadPayloadError(status, body)
    if status == 429:
        return RateLimitedError(status, body)
    if status >= 500 or status == 0:
        return VendorUnavailableError(status, body)
    return RuntimeError(f"unexpected Acme response {status} {body}")


class AcmeClient:
    def __init__(self, base_url: str):
        self._http = httpx.AsyncClient(base_url=base_url, timeout=TIMEOUT_S)

    async def upsert(self, path: str, record: dict) -> None:
        """Creates or updates one record at ``path``; idempotent on the record's id."""
        try:
            response = await self._http.post(path, json=record)
        except httpx.HTTPError as e:
            raise VendorUnavailableError(0, str(e)) from e  # timeout, connection refused
        if response.is_error:
            raise error_for(response.status_code, response.text)

    async def aclose(self) -> None:
        await self._http.aclose()
