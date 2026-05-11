import asyncio
import time
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.nobitex import nobitex_constants as CONSTANTS, nobitex_web_utils as web_utils
from hummingbot.connector.exchange.nobitex.nobitex_order_book import NobitexOrderBook
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.nobitex.nobitex_exchange import NobitexExchange


class NobitexAPIOrderBookDataSource(OrderBookTrackerDataSource):

    FULL_ORDER_BOOK_RESET_DELTA_SECONDS = 60
    TRADE_UPDATE_INTERVAL = 10  # TODO: change this by the actual value

    HEARTBEAT_TIME_INTERVAL = 25.0
    # TRADE_STREAM_ID = 1
    # DIFF_STREAM_ID = 2
    ONE_HOUR = 60 * 60

    _logger: Optional[HummingbotLogger] = None

    def __init__(
        self,
        trading_pairs: List[str],
        connector: "NobitexExchange",
        api_factory: WebAssistantsFactory,
    ):
        super().__init__(trading_pairs)
        self._connector = connector
        # self._trade_messages_queue_key = CONSTANTS.TRADE_EVENT_TYPE
        # self._diff_messages_queue_key = CONSTANTS.DIFF_EVENT_TYPE

        self._trade_messages_queue_key = "trade"
        self._diff_messages_queue_key = "order_book_diff"
        self._snapshot_messages_queue_key = "order_book_snapshot"

        self._domain = "ir"
        self._api_factory = api_factory
        self._ws_id = 1

    # TODO: First check done
    async def get_last_traded_prices(self, trading_pairs: List[str], domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    # TODO: First check done
    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """
        Retrieves a copy of the full order book from the exchange, for a particular trading pair.

        :param trading_pair: the trading pair for which the order book will be retrieved

        :return: the response from the exchange (JSON dictionary)
        """
        symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

        rest_assistant = await self._api_factory.get_rest_assistant()
        data = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(path_url=CONSTANTS.ORDER_BOOK_PATH + symbol, domain=self._domain),
            params=None,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.ORDER_BOOK_PATH,
        )

        return data

    async def _trade(self, trading_pair: str) -> Dict[str, Any]:
        """
        Requests the trades from the exchange. Retrieves a copy of last trades for a particular trading pair.

        :param trading_pair: the trading pair for which the trades will be retrieved

        :return: the response from the exchange (JSON dictionary)
        """
        symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

        rest_assistant = await self._api_factory.get_rest_assistant()
        data = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(path_url=CONSTANTS.TRADE_PATH + symbol, domain=self._domain),
            params=None,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.TRADE_PATH,
        )

        if data["status"] == "ok":
            return data["trades"]
        return []

    async def _request_trades(self, output: asyncio.Queue):
        """
        Requests the trades from the exchange. Retrieves a copy of last trades for all trading pairs.

        :param output: a queue to add the created trade messages
        """
        for trading_pair in self._trading_pairs:
            try:
                trades = await self._trade(trading_pair=trading_pair)

                for trade in trades:
                    t = OrderBookMessage(
                        timestamp=trade["time"],
                        message_type=OrderBookMessageType.TRADE,
                        content={
                            "trade_id": trade["time"],
                            "trading_pair": trading_pair,
                            "price": self._connector.nobitex_convert_received_rls_to_irt(
                                trading_pair=trading_pair, value=Decimal(trade["price"])
                            ),
                            "amount": Decimal(trade["volume"]),
                            "trade_type": (
                                float(TradeType.SELL.value) if trade["type"] == "sell" else float(TradeType.BUY.value)
                            ),
                        },
                    )
                    output.put_nowait(t)
            except Exception:
                self.logger().exception(f"Unexpected error fetching trade for {trading_pair}.")
                raise

    # TODO: First check done
    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Subscribes to the trade events and diff orders events through the provided websocket connection.
        :param ws: the websocket assistant used to connect to the exchange
        """
        try:
            # self._ws_id is used to identify the request

            for trading_pair in self._trading_pairs:
                symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                payload = {"subscribe": {"channel": f"public:orderbook-{symbol}"}, "id": self._ws_id}
                self._ws_id += 1
                subscribe_orderbook_request: WSJSONRequest = WSJSONRequest(payload=payload)
                await ws.send(subscribe_orderbook_request)

            self.logger().info("Subscribed to public order book channels...")

        except asyncio.CancelledError:
            raise
        except Exception:

            self.logger().error(
                "Unexpected error occurred subscribing to order book trading and delta streams...", exc_info=True
            )
            raise

    # TODO: First check done
    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(ws_url=CONSTANTS.WSS_URL, ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)

        # send connect request after connecting to the websocket based on Nobitex documentation
        payload = {"connect": {}, "id": 1}
        connect_request: WSJSONRequest = WSJSONRequest(payload=payload)
        await ws.send(connect_request)

        # reset the ws_id to 2 because the first id is used for the connect request
        self._ws_id = 2

        return ws

    # TODO: First check done
    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair)
        snapshot_timestamp: float = time.time()

        if snapshot["status"] == "ok":
            snapshot["bids"] = map(
                lambda x: [
                    self._connector.nobitex_convert_received_rls_to_irt(trading_pair=trading_pair, value=Decimal(x[0])),
                    Decimal(x[1]),
                ],
                snapshot["bids"],
            )

            snapshot["asks"] = map(
                lambda x: [
                    self._connector.nobitex_convert_received_rls_to_irt(trading_pair=trading_pair, value=Decimal(x[0])),
                    Decimal(x[1]),
                ],
                snapshot["asks"],
            )

        snapshot_msg: OrderBookMessage = NobitexOrderBook.snapshot_message_from_exchange(
            snapshot, snapshot_timestamp, metadata={"trading_pair": trading_pair}
        )
        return snapshot_msg

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """
        Create an instance of OrderBookMessage of type OrderBookMessageType.TRADE

        :param raw_message: the JSON dictionary of the public trade event
        :param message_queue: queue where the parsed messages should be stored in

        Note: this exchange does not support trades through the websocket

        in binance, the trades are handled through the websocket like this:

        if "result" not in raw_message:
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=raw_message["s"])
            trade_message = NobitexOrderBook.trade_message_from_exchange(
                raw_message, {"trading_pair": trading_pair})
            message_queue.put_nowait(trade_message)
        """
        raise NotImplementedError

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """
        Create an instance of OrderBookMessage of type OrderBookMessageType.DIFF

        :param raw_message: the JSON dictionary of the public trade event
        :param message_queue: queue where the parsed messages should be stored in

        Note: this exchange does not support order diffs at all

        in binance, the order book diffs are handled through the websocket like this:

        if "result" not in raw_message:
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=raw_message["s"])
            order_book_message: OrderBookMessage = NobitexOrderBook.diff_message_from_exchange(
                raw_message, time.time(), {"trading_pair": trading_pair})
            message_queue.put_nowait(order_book_message)
        """
        raise NotImplementedError

    # TODO: implement this based on nobitex documentation, and put order book snapshot in the queue
    async def _parse_order_book_snapshot_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """
        Create an instance of OrderBookMessage of type OrderBookMessageType.SNAPSHOT

        :param raw_message: the JSON dictionary of the public trade event
        :param message_queue: queue where the parsed messages should be stored in
        """
        data = raw_message.get("push", {}).get("pub", {}).get("data")
        symbol = raw_message.get("push", {}).get("channel", "public:orderbook-").split("-")[1]

        if "bids" in data and "asks" in data and symbol != "":
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=symbol)

            data["bids"] = map(
                lambda x: [
                    self._connector.nobitex_convert_received_rls_to_irt(trading_pair=trading_pair, value=Decimal(x[0])),
                    Decimal(x[1]),
                ],
                data["bids"],
            )

            data["asks"] = map(
                lambda x: [
                    self._connector.nobitex_convert_received_rls_to_irt(trading_pair=trading_pair, value=Decimal(x[0])),
                    Decimal(x[1]),
                ],
                data["asks"],
            )

            order_book_message: OrderBookMessage = NobitexOrderBook.snapshot_message_from_exchange(
                data, time.time(), {"trading_pair": trading_pair}
            )
            message_queue.put_nowait(order_book_message)

    # TODO: First check done
    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        channel = event_message.get("push", {}).get("channel", "") if event_message is not None else ""
        # check if the channel is for the order book snapshot
        if str(channel).startswith("public:orderbook-"):
            return self._snapshot_messages_queue_key
        # other channels are not supported yet

        return channel

    # TODO: First check done
    async def _process_websocket_messages(self, websocket_assistant: WSAssistant):
        ws = websocket_assistant

        pong_response = WSJSONRequest(payload={})

        async for ws_response in websocket_assistant.iter_messages():
            data = ws_response.data if ws_response is not None else None

            if data == {}:
                # this is how we send a pong response to the server
                await ws.send(pong_response)
                pass
            elif data is not None:
                channel: str = self._channel_originating_message(event_message=data)
                valid_channels = self._get_messages_queue_keys()
                if channel in valid_channels:
                    self._message_queue[channel].put_nowait(data)
                else:
                    await self._process_message_for_unknown_channel(
                        event_message=ws_response, websocket_assistant=websocket_assistant
                    )

    # TODO: First check done
    async def listen_for_subscriptions(self):
        """
        Connects to the trade events and order diffs websocket endpoints and listens to the messages sent by the
        exchange. Each message is stored in its own queue.
        """
        # if we need custom ping implementation, we can do it here
        # if we need to implement custom subscription logic, we can do it here

        ws: Optional[WSAssistant] = None
        while True:
            try:
                ws: WSAssistant = await self._connected_websocket_assistant()
                self._ws_assistant = ws
                await self._subscribe_channels(ws)
                await self._process_websocket_messages(websocket_assistant=ws)
            except asyncio.CancelledError:
                raise
            except ConnectionError as connection_exception:
                self.logger().warning(f"The websocket connection was closed ({connection_exception})")
            except Exception:
                self.logger().exception(
                    "Unexpected error occurred when listening to order book streams. Retrying in 5 seconds...",
                )
                await self._sleep(1.0)
            finally:
                self._ws_assistant = None
                await self._on_order_stream_interruption(websocket_assistant=ws)

    # Note: this exchange does not support order diffs
    async def listen_for_order_book_diffs(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """
        Reads the order diffs events queue. For each event creates a diff message instance and adds it to the
        output queue

        :param ev_loop: the event loop the method will run in
        :param output: a queue to add the created diff messages
        """
        message_queue = self._message_queue[self._diff_messages_queue_key]
        while True:
            try:
                diff_event = await message_queue.get()
                await self._parse_order_book_diff_message(raw_message=diff_event, message_queue=output)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error when processing order book diffs from exchange")

    # TODO: First check done
    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """
        Reads the order snapshot events queue. For each event it creates a snapshot message instance and adds it to the
        output queue.
        This method also request the full order book content from the exchange using HTTP requests if it does not
        receive events during one hour.

        :param ev_loop: the event loop the method will run in
        :param output: a queue to add the created snapshot messages
        """
        message_queue = self._message_queue[self._snapshot_messages_queue_key]
        while True:
            try:
                try:
                    snapshot_event = await asyncio.wait_for(
                        message_queue.get(), timeout=self.FULL_ORDER_BOOK_RESET_DELTA_SECONDS
                    )
                    await self._parse_order_book_snapshot_message(raw_message=snapshot_event, message_queue=output)
                except asyncio.TimeoutError:
                    await self._request_order_book_snapshots(output=output)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error when processing public order book snapshots from exchange")
                await self._sleep(1.0)

    # TODO: First check done
    async def listen_for_trades(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """
        Reads the trade events queue. For each event creates a trade message instance and adds it to the output queue

        :param ev_loop: the event loop the method will run in
        :param output: a queue to add the created trade messages
        """
        message_queue = self._message_queue[self._trade_messages_queue_key]

        while True:
            try:
                try:
                    trade_event = await asyncio.wait_for(message_queue.get(), timeout=self.TRADE_UPDATE_INTERVAL)

                    await self._parse_trade_message(raw_message=trade_event, message_queue=output)
                except asyncio.TimeoutError:
                    await self._request_trades(output=output)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error when processing public trades from exchange")
                await self._sleep(1.0)

    async def subscribe_to_trading_pair(self, trading_pair: str) -> bool:
        if self._ws_assistant is None:
            self.logger().warning(f"Cannot subscribe to {trading_pair}: WebSocket not connected")
            return False

        try:
            symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
            payload = {"subscribe": {"channel": f"public:orderbook-{symbol}"}, "id": self._ws_id}
            self._ws_id += 1
            subscribe_request: WSJSONRequest = WSJSONRequest(payload=payload)
            await self._ws_assistant.send(subscribe_request)

            self.add_trading_pair(trading_pair)
            self.logger().info(f"Subscribed to {trading_pair} order book channel")
            return True

        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception(f"Unexpected error subscribing to {trading_pair} channel")
            return False

    async def unsubscribe_from_trading_pair(self, trading_pair: str) -> bool:
        if self._ws_assistant is None:
            self.logger().warning(f"Cannot unsubscribe from {trading_pair}: WebSocket not connected")
            return False

        try:
            symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
            payload = {"unsubscribe": {"channel": f"public:orderbook-{symbol}"}, "id": self._ws_id}
            self._ws_id += 1
            unsubscribe_request: WSJSONRequest = WSJSONRequest(payload=payload)
            await self._ws_assistant.send(unsubscribe_request)

            self.remove_trading_pair(trading_pair)
            self.logger().info(f"Unsubscribed from {trading_pair} order book channel")
            return True

        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception(f"Unexpected error unsubscribing from {trading_pair} channel")
            return False
