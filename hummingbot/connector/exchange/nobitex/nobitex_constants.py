from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

REST_URL = "https://apiv2.nobitex.ir"
WSS_URL = "wss://ws.nobitex.ir/connection/websocket"

HBOT_ORDER_ID_PREFIX = "x-HUM"
MAX_ORDER_ID_LEN = 32

WS_AUTH_PATH_URL = "/auth/ws/token/"
ORDER_ADD_PATH_URL = "/market/orders/add"
ORDER_UPDATE_PATH_URL = "/market/orders/update-status"
ORDER_STATUS_PATH_URL = "/market/orders/status"
ACCOUNTS_PATH_URL = "/users/wallets/list"
SERVER_OPTIONS_PATH = "/v2/options"

SIDE_BUY = "buy"
SIDE_SELL = "sell"

ONE_MINUTE = 60

ORDER_STATE = {
    "Active": OrderState.OPEN,
    "Inactive": OrderState.PENDING_CREATE,
    "Done": OrderState.FILLED,
    "Canceled": OrderState.CANCELED,
}

RATE_LIMITS = [
    RateLimit(limit_id=WS_AUTH_PATH_URL, limit=30, time_interval=10 * ONE_MINUTE),
    RateLimit(limit_id=ORDER_ADD_PATH_URL, limit=300, time_interval=10 * ONE_MINUTE),  # shared between spot and perp TODO: if perp added, put half of this limit for perp
    RateLimit(limit_id=ORDER_STATUS_PATH_URL, limit=300, time_interval=ONE_MINUTE),
    RateLimit(limit_id=ORDER_UPDATE_PATH_URL, limit=90, time_interval=ONE_MINUTE),
    RateLimit(limit_id=ACCOUNTS_PATH_URL, limit=20, time_interval=2 * ONE_MINUTE),
    RateLimit(limit_id=SERVER_OPTIONS_PATH, limit=300, time_interval=ONE_MINUTE),
]


ORDER_NOT_EXIST_ERROR_CODE = "No Order matches the given query."
UNKNOWN_ORDER_ERROR_CODE = "No Order matches the given query."
