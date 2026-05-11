import base64
from time import time
from typing import Dict
from urllib.parse import quote, urlencode, urlparse, urlunparse

from cryptography.hazmat.primitives.asymmetric import ed25519

from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


class NobitexAuth(AuthBase):
    def __init__(self, api_key: str, secret_key: str, ws_auth_token: str):
        self.api_key = api_key
        self.secret_key = secret_key
        self.ws_auth_token = ws_auth_token
        try:
            self.secret_key = base64.urlsafe_b64decode(self.secret_key)
        except Exception as e:
            raise ValueError(f"Invalid secret key base64 encoding: {e}")
        if len(self.secret_key) != 32:
            raise ValueError(f"Private seed must be 32 bytes, got {len(self.secret_key)} bytes")
        self.secret_key = ed25519.Ed25519PrivateKey.from_private_bytes(self.secret_key)

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        headers = {}
        if request.headers is not None:
            headers.update(request.headers)
        headers.update(self.header_for_authentication(request))
        request.headers = headers
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        """
        This method is intended to configure a websocket request to be authenticated. Binance does not use this
        functionality
        """
        return request  # pass-through

    def header_for_authentication(self, request: RESTRequest) -> Dict[str, str]:
        now = int(time())
        return {
            "Nobitex-Key": self.api_key,
            "Nobitex-Timestamp": str(now),
            "Nobitex-Signature": self._generate_signature(now, request)
        }

    def _generate_signature(self, now: int, request: RESTRequest) -> str:
        raw = f"{now}{str(request.method)}{self._extract_endpoint(request)}"
        if request.data is not None:
            raw += str(request.data)
        signature = self.secret_key.sign(raw.encode("utf-8"))
        return base64.urlsafe_b64encode(signature).decode("utf-8")

    @staticmethod
    def _extract_endpoint(request: RESTRequest) -> str:
        val = ""
        if request.endpoint_url:
            val = request.endpoint_url
        elif not request.url:
            val = "/"
        else:
            val = request.url.strip()
            if not val.startswith(('http://', 'https://')):
                parsed = urlparse('http://' + val)
            else:
                parsed = urlparse(val)
            val = urlunparse(('', '', parsed.path, parsed.params, parsed.query, parsed.fragment))
        if not val.startswith('/'):
            val = '/' + val
        val = quote(val)
        if request.params:
            val += "?" + urlencode(request.params)
        return val
