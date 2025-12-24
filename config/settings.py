import os

def _env(name, default=None, required=False):
    value = os.getenv(name, default)
    if required and not value:
        raise RuntimeError(f"{name} environment variable is required")
    return value

# Core DB
DB_URL = _env("DB_URL", required=True)

# Polymarket API creds (optional in some scripts, required in others)
POLY_API_KEY = _env("POLY_API_KEY")
POLY_API_SECRET = _env("POLY_API_SECRET")
POLY_API_PASSPHRASE = _env("POLY_API_PASSPHRASE")

# HTTP endpoints (default to the standard ones you already use)
POLY_MARKETS_HTTP_BASE = _env(
    "POLY_MARKETS_HTTP_BASE",
    "https://gamma-api.polymarket.com",
)
POLY_CLOB_HTTP_BASE = _env(
    "POLY_CLOB_HTTP_BASE",
    "https://clob.polymarket.com",
)
POLY_TRADES_HTTP_BASE = _env(
    "POLY_TRADES_HTTP_BASE",
    "https://data-api.polymarket.com",
)

# WebSocket endpoints
POLY_TRADES_WSS = _env(
    "POLY_TRADES_WSS",
    "wss://clob-ws.polymarket.com",
)
POLY_WSS_BASE = _env(
    "POLY_WSS_BASE",
    "wss://ws-subscriptions-clob.polymarket.com",
)
