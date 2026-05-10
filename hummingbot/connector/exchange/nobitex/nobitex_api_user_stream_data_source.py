import asyncio
import time
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Dict, List, Optional

from hummingbot.connector.exchange.nobitex import nobitex_constants as CONSTANTS
from hummingbot.connector.exchange.nobitex.nobitex_auth import NobitexAuth
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import OrderState
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.nobitex.nobitex_exchange import NobitexExchange


class NobitexAPIUserStreamDataSource(UserStreamTrackerDataSource):

    # TODO: check if this is correct
    # LISTEN_KEY_KEEP_ALIVE_INTERVAL = 1800  # Recommended to Ping/Update listen key to keep connection alive
    HEARTBEAT_TIME_INTERVAL = 25.0
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 6.0

    _logger: Optional[HummingbotLogger] = None

    def __init__(
        self,
        auth: NobitexAuth,
        trading_pairs: List[str],
        connector: "NobitexExchange",
        api_factory: WebAssistantsFactory,
    ):
        super().__init__()
        self._auth: NobitexAuth = auth
        # self._current_listen_key = None
        self._domain = "ir"
        self._api_factory = api_factory

        self._connector = connector
        self._trading_pairs = trading_pairs

        self._last_recv_time = 0

        self._last_poll_timestamp = 0

        # self._listen_key_initialized_event: asyncio.Event = asyncio.Event()
        # self._last_listen_key_ping_ts = 0

    @property
    def last_recv_time(self) -> float:
        """
        Returns the time of the last received message

        :return: the timestamp of the last received message in seconds
        """
        # if self._ws_assistant:
        return self._last_recv_time
        # return 0

    async def _send_ping(self, websocket_assistant: WSAssistant):
        await self._sleep(1.0)
        self._last_recv_time = self._time() * 1e3

    async def _gather_private_orders(self) -> List[Dict]:
        params = {
            "page": 1,
            "limit": 100,
            "status": "all",
            "tradingType": "spot",
            "details": 2,
        }
        page = 1
        has_next = True

        orders = []

        while has_next:
            params["page"] = page
            response = await self._connector._api_get(
                path_url=CONSTANTS.PRIVATE_ORDERS_PATH, is_auth_required=True, params=params
            )
            if response is not None and response.get("status") == "ok":

                orders.extend(response.get("orders", []))

            page += 1
            has_next = response.get("hasNext")

        return orders

    async def _gather_private_trades(self) -> List[Dict]:
        params = {
            "page": 1,
            "limit": 100,
        }
        page = 1
        has_next = True

        trades = []

        while has_next:
            params["page"] = page
            response = await self._connector._api_get(
                path_url=CONSTANTS.MY_TRADES_PATH_URL, is_auth_required=True, params=params
            )
            if response is not None and response.get("status") == "ok":

                trades.extend(response.get("trades", []))

            page += 1
            has_next = response.get("hasNext")

        return trades

    def filter_trades_by_order_id(self, all_trades: List[Dict], order: Dict) -> List[Dict]:
        _all_trades = [trade for trade in all_trades if trade.get("orderId") == order.get("id")]

        trades = []
        for trade in _all_trades:
            commission_asset = order.get("commission_asset")
            commission_amount = Decimal(trade.get("fee"))
            if commission_asset == "IRT":
                commission_amount = commission_amount / 10

            quote = order.get("quote")
            if quote == "IRT":
                price = Decimal(trade.get("price")) / 10
                total = Decimal(trade.get("total")) / 10
            else:
                price = Decimal(trade.get("price"))
                total = Decimal(trade.get("total"))

            trades.append(
                {
                    "id": trade["id"],
                    "order_exchange_id": order.get("id"),
                    "client_order_id": order.get("client_order_id"),
                    "timestamp": datetime.fromtimestamp(trade.get("timestamp")).timestamp() * 1e3,
                    "price": price,
                    "amount": Decimal(trade.get("amount")),
                    "total": total,
                    "commission": commission_amount,
                    "commission_asset": commission_asset,
                }
            )

        return trades

    async def _update_private_orders(self, queue: asyncio.Queue):
        orders = await self._gather_private_orders()
        all_trades = await self._gather_private_trades()
        if orders is not None and len(orders) > 0:
            for order in orders:
                order_id = str(order.get("id"))

                order_side = TradeType.BUY if order.get("type") == "buy" else TradeType.SELL
                execution = str(order.get("execution")).lower()
                order_type = (
                    OrderType.LIMIT
                    if execution == "limit"
                    else (OrderType.MARKET if execution == "market" else "unknown")
                )
                order_matched_amount = Decimal(order.get("matchedAmount"))
                created_at = datetime.fromisoformat(order.get("created_at")).timestamp() * 1e3

                order_average_price = Decimal(order.get("averagePrice"))

                order_status = CONSTANTS.ORDER_STATE[order.get("status")]
                if order_matched_amount > 0 and order_status == OrderState.OPEN:
                    order_status = OrderState.PARTIALLY_FILLED

                base = self._connector._convert_trading_pair_naming_mapping(order.get("srcCurrency")).replace(
                    "RLS", "IRT"
                )
                quote = self._connector._convert_trading_pair_naming_mapping(order.get("dstCurrency")).replace(
                    "RLS", "IRT"
                )

                trading_pair = combine_to_hb_trading_pair(base=base, quote=quote)

                price = order.get("price")

                order_price = self._connector.nobitex_convert_received_rls_to_irt(
                    trading_pair=trading_pair, value=Decimal(price if price != "market" else order_average_price)
                )

                order_average_price = self._connector.nobitex_convert_received_rls_to_irt(
                    trading_pair=trading_pair, value=order_average_price
                )

                order_quantity = Decimal(order.get("amount"))
                client_order_id = order.get("clientOrderId")

                if order_side == TradeType.BUY:
                    commission_asset = base
                else:
                    commission_asset = quote

                commission_amount = Decimal(order.get("fee"))

                if commission_asset == "IRT":
                    commission_amount = commission_amount / 10

                _order = {
                    "id": order_id,
                    "order_type": order_type,
                    "order_side": order_side,
                    "client_order_id": client_order_id,
                    "exchange_order_id": order_id,
                    "trading_pair": trading_pair,
                    "quote": quote,
                    "base": base,
                    "order_status": order_status,
                    "order_price": order_price,
                    "order_quantity": order_quantity,
                    "commission_asset": commission_asset,
                    "commission_amount": commission_amount,
                    "order_average_price": order_average_price,
                    "order_matched_amount": order_matched_amount,
                    "created_at": created_at,
                    "event_type": CONSTANTS.EVENT_TYPE_ORDER_CHANGE,
                }

                trades = self.filter_trades_by_order_id(all_trades, _order)

                _order["trades"] = trades

                await self._process_event_message(event_message=_order, queue=queue)

    async def _update_private_balances(self, _: asyncio.Queue):
        await self._connector._update_balances()

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant, queue: asyncio.Queue):
        while True:
            current_timestamp = time.time()

            if current_timestamp - self._last_poll_timestamp > self.UPDATE_ORDER_STATUS_MIN_INTERVAL:
                self._last_poll_timestamp = current_timestamp

                self._last_recv_time = current_timestamp * 1e3

                try:
                    # Gather all private data requests concurrently
                    responses = await safe_gather(
                        self._update_private_orders(queue), self._update_private_balances(queue), return_exceptions=True
                    )

                    # Process each response
                    for response in responses:
                        if isinstance(response, Exception):
                            self.logger().error(f"Error fetching private data: {str(response)}", exc_info=True)
                            continue

                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self.logger().error(f"Unexpected error in websocket message processing: {str(e)}", exc_info=True)
                    raise
            else:
                await self._sleep(0.5)

    async def listen_for_user_stream(self, output: asyncio.Queue):
        """
        Connects to the user private channel in the exchange using a websocket connection. With the established
        connection listens to all balance events and order updates provided by the exchange, and stores them in the
        output queue

        :param output: the queue to use to store the received messages
        """
        while True:
            await self._sleep(1.0)
            try:
                self._ws_assistant = await self._connected_websocket_assistant()
                await self._subscribe_channels(websocket_assistant=self._ws_assistant)
                await self._send_ping(websocket_assistant=self._ws_assistant)  # to update last_recv_timestamp
                await self._process_websocket_messages(websocket_assistant=self._ws_assistant, queue=output)
            except asyncio.CancelledError:
                raise
            except ConnectionError as connection_exception:
                self.logger().warning(f"The websocket connection was closed ({connection_exception})")
            except Exception:
                self.logger().exception("Unexpected error while listening to user stream. Retrying after 5 seconds...")
                await self._sleep(1.0)
            finally:
                await self._on_user_stream_interruption(websocket_assistant=self._ws_assistant)
                self._ws_assistant = None

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """
        Creates an instance of WSAssistant connected to the exchange
        """
        # self._manage_listen_key_task = safe_ensure_future(self._manage_listen_key_task_loop())
        # await self._listen_key_initialized_event.wait()

        # ws: WSAssistant = await self._get_ws_assistant()
        # url = f"{CONSTANTS.WSS_URL.format(self._domain)}/{self._current_listen_key}"
        # await ws.connect(ws_url=url, ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
        # return ws
        return None

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        """
        Subscribes to the trade events and diff orders events through the provided websocket connection.

        Nobitex does not require any channel subscription.

        :param websocket_assistant: the websocket assistant used to connect to the exchange
        """
        pass

    async def _get_ws_assistant(self) -> WSAssistant:
        # if self._ws_assistant is None:
        # self._ws_assistant = await self._api_factory.get_ws_assistant()
        # return self._ws_assistant
        return None

    async def _on_user_stream_interruption(self, websocket_assistant: Optional[WSAssistant]):
        # await super()._on_user_stream_interruption(websocket_assistant=websocket_assistant)
        # self._manage_listen_key_task and self._manage_listen_key_task.cancel()
        # self._current_listen_key = None
        # self._listen_key_initialized_event.clear()
        await self._sleep(5)
