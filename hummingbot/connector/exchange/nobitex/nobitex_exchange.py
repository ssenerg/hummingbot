import asyncio
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.nobitex import nobitex_constants as CONSTANTS, nobitex_web_utils as web_utils
from hummingbot.connector.exchange.nobitex.nobitex_api_order_book_data_source import NobitexAPIOrderBookDataSource
from hummingbot.connector.exchange.nobitex.nobitex_api_user_stream_data_source import NobitexAPIUserStreamDataSource
from hummingbot.connector.exchange.nobitex.nobitex_auth import NobitexAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair, split_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class NobitexExchange(ExchangePyBase):
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 5.0

    web_utils = web_utils

    def __init__(self,
                 nobitex_api_key: str,
                 nobitex_api_secret: str,
                 nobitex_ws_auth_token: str,
                 balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
                 rate_limits_share_pct: Decimal = Decimal("100"),
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 ):
        self.api_key = nobitex_api_key
        self.api_secret = nobitex_api_secret
        self.ws_auth_token = nobitex_ws_auth_token
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._last_trades_poll_nobitex_timestamp = 1.0
        super().__init__(balance_asset_limit, rate_limits_share_pct)

    @staticmethod
    def nobitex_order_type(order_type: OrderType) -> str:
        if order_type == OrderType.LIMIT:
            return "limit"
        elif order_type == OrderType.MARKET:
            return "market"
        else:
            raise ValueError(f"Invalid order type: {order_type}")

    @staticmethod
    def to_hb_order_type(nobitex_type: str) -> OrderType:
        if nobitex_type == "Limit":
            return OrderType.LIMIT
        elif nobitex_type == "Market":
            return OrderType.MARKET
        else:
            raise ValueError(f"Invalid order type: {nobitex_type}")

    @property
    def authenticator(self):
        return NobitexAuth(self.api_key, self.api_secret, self.ws_auth_token)

    @property
    def name(self) -> str:
        return "nobitex"

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self):
        return "ir"

    @property
    def client_order_id_max_length(self) -> int:
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self) -> str:
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self):
        return CONSTANTS.SERVER_OPTIONS_PATH

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.SERVER_OPTIONS_PATH

    @property
    def check_network_request_path(self):
        return CONSTANTS.SERVER_OPTIONS_PATH

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def supported_order_types(self) -> List[OrderType]:
        return [OrderType.MARKET, OrderType.LIMIT]

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        return False

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return CONSTANTS.UNKNOWN_ORDER_ERROR_CODE in str(status_update_exception)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return CONSTANTS.UNKNOWN_ORDER_ERROR_CODE in str(cancelation_exception)

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        api_params = {
            "clientOrderId": tracked_order.client_order_id,
            "status": "canceled",
        }
        if tracked_order.exchange_order_id is not None:
            try:
                api_params["order"] = int(tracked_order.exchange_order_id)
            except Exception:
                ...
        cancel_result = await self._api_post(
            path_url=CONSTANTS.ORDER_UPDATE_PATH_URL,
            data=api_params,
            is_auth_required=True,
        )
        return cancel_result.get("status") == "ok"

    @staticmethod
    def _convert_asset(asset: str) -> str:
        asset = asset.lower()
        if asset == "irt":
            return "rls"
        return asset

    async def _place_order(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           trade_type: TradeType,
                           order_type: OrderType,
                           price: Decimal,
                           **kwargs,
                           ) -> Tuple[str, float]:
        type_str = self.nobitex_order_type(order_type)
        side_str = CONSTANTS.SIDE_BUY if trade_type is TradeType.BUY else CONSTANTS.SIDE_SELL

        base, quote = split_hb_trading_pair(trading_pair=trading_pair)
        base, quote = self._convert_asset(base), self._convert_asset(quote)

        if base == "rls":
            amount_str = f"{amount * 10:f}"
        else:
            amount_str = f"{amount:f}"

        api_params = {
            "type": side_str,
            "execution": type_str,
            "srcCurrency": base,
            "dstCurrency": quote,
            "amount": amount_str,
            "clientOrderId": order_id,
        }

        if order_type is OrderType.LIMIT:
            if quote == "rls":
                api_params["price"] = f"{price * 10:f}"
            else:
                api_params["price"] = f"{price:f}"

        try:
            order_result = await self._api_post(
                path_url=CONSTANTS.ORDER_ADD_PATH_URL,
                data=api_params,
                is_auth_required=True,
            )
        except IOError:
            return "UNKNOWN", self._time_synchronizer.time()
        if order_result.get("status", None) == "ok":
            inner_order = order_result.get("order", {})
            o_id = str(inner_order.get("id", "UNKNOWN"))
            created_at = inner_order.get("created_at")
            if created_at is None:
                transact_time = self._time_synchronizer.time()
            else:
                transact_time = datetime.fromisoformat(created_at).timestamp()
            return o_id, transact_time
        else:
            raise Exception(f"Error placing order: {order_result.get('message')}")

    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 amount: Decimal,
                 price: Decimal = s_decimal_NaN,
                 is_maker: Optional[bool] = None) -> TradeFeeBase:
        return DeductedFromReturnsTradeFee(percent=self.estimate_fee_pct(False))

    async def _update_trading_fees(self):
        """
        Update fees information from the exchange
        """
        pass

    # TODO: Double-check later
    async def _user_stream_event_listener(self):
        """
        This functions runs in background continuously processing the events received from the exchange by the user
        stream data source. It keeps reading events from the queue until the task is interrupted.
        The events received are balance updates, order updates and trade events.
        """
        async for event_message in self._iter_user_event_queue():
            try:
                event_type = event_message.get("event_type")

                if event_type == CONSTANTS.EVENT_TYPE_ORDER_CHANGE:
                    order = event_message
                    trades = order.get("trades", [])
                    client_order_id = order.get("client_order_id")
                    exchange_order_id = order.get("exchange_order_id")
                    trading_pair = order.get("trading_pair")
                    tracked_order = self._order_tracker.all_fillable_orders.get(client_order_id)

                    if tracked_order is not None:
                        for trade in trades:
                            trade_id = trade.get("id")

                            fee = TradeFeeBase.new_spot_fee(
                                fee_schema=self.trade_fee_schema(),
                                trade_type=tracked_order.trade_type,
                                percent_token=trade.get("commission_asset"),
                                flat_fees=[
                                    TokenAmount(amount=trade.get("commission"), token=trade.get("commission_asset"))
                                ],
                            )
                            trade_update = TradeUpdate(
                                trade_id=trade_id,
                                client_order_id=client_order_id,
                                exchange_order_id=exchange_order_id,
                                trading_pair=trading_pair,
                                fee=fee,
                                fill_base_amount=Decimal(trade.get("amount")),
                                fill_quote_amount=Decimal(trade.get("total")),
                                fill_price=Decimal(trade.get("price")),
                                fill_timestamp=trade.get("timestamp") * 1e-3,
                            )
                            self._order_tracker.process_trade_update(trade_update)
                        update_timestamp = max(
                            max(trade.get("timestamp") for trade in trades) if trades else 0, order.get("created_at")
                        )
                        order_update = OrderUpdate(
                            trading_pair=trading_pair,
                            update_timestamp=update_timestamp * 1e-3,
                            new_state=order.get("order_status"),
                            client_order_id=client_order_id,
                            exchange_order_id=exchange_order_id,
                        )
                        self._order_tracker.process_order_update(order_update=order_update)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in user stream listener loop.", exc_info=True)
                await self._sleep(5.0)

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        rules = []

        info = exchange_info_dict.get("nobitex", {})
        min_orders = info.get("minOrders", {})
        amount_precisions = info.get("amountPrecisions", {})
        price_precisions = info.get("pricePrecisions", {})

        all_pairs = set(). \
            union(set(amount_precisions.keys())). \
            union(set(price_precisions.keys()))
        raw_rules = {str(k).upper(): {} for k in all_pairs}

        for pair, rule in raw_rules.items():
            amount_precision = amount_precisions.get(pair)
            price_precision = price_precisions.get(pair)
            if pair.endswith("USDT"):
                base = pair[:-4]
                quote = "USDT"
                min_order = min_orders.get("usdt")
            elif pair.endswith("IRT"):
                base = pair[:-3]
                quote = "IRT"
                min_order = min_orders.get("rls")
            else:
                self.logger().warning(f"Error parsing the trading pair rule {pair}, does not end with USDT or IRT. Skipping.")
                continue

            if amount_precision is not None:
                try:
                    amount_precision = Decimal(amount_precision)
                    if base == "IRT":
                        amount_precision = amount_precision / 10
                except Exception:
                    amount_precision = None
            if price_precision is not None:
                try:
                    price_precision = Decimal(price_precision)
                    if quote == "IRT":
                        price_precision = price_precision / 10
                except Exception:
                    price_precision = None

            if min_order is not None:
                try:
                    min_order = Decimal(min_order)
                    if quote == "IRT":
                        min_order = min_order / 10
                except Exception:
                    min_order = None

            if amount_precision is not None:
                rule["min_order_size"] = amount_precision
                rule["min_base_amount_increment"] = amount_precision
            if price_precision is not None:
                rule["min_price_increment"] = price_precision
                rule["min_quote_amount_increment"] = price_precision
            if min_order is not None:
                rule["min_notional_size"] = min_order

            rules.append(TradingRule(
                combine_to_hb_trading_pair(base=base, quote=quote),
                **rule,
            ))

        return rules

    async def _update_balances(self):
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()

        account_info = await self._api_get(
            path_url=CONSTANTS.ACCOUNTS_PATH_URL,
            is_auth_required=True,
        )
        balances = account_info.get("wallets")
        if account_info.get("status") != "ok" or balances is None:
            return

        for i, balance in enumerate(balances):
            asset_name = balance.get("currency")
            if asset_name is None:
                self.logger().warning(f"record {i} of wallets does not have currency key")
                continue
            asset_name = str(asset_name).upper()

            free_balance = balance.get("activeBalance")
            if free_balance is None:
                self.logger().warning(f"wallets asset {asset_name} does not have activeBalance key")
                continue
            try:
                free_balance = Decimal(free_balance)
            except Exception:
                self.logger().warning(f"wallets asset {asset_name} activeBalance is not monetary")
                continue

            total_balance = balance.get("balance")
            if total_balance is None:
                self.logger().warning(f"wallets asset {asset_name} does not have balance key")
                continue
            try:
                total_balance = Decimal(total_balance)
            except Exception:
                self.logger().warning(f"wallets asset {asset_name} balance is not monetary")
                continue

            if asset_name == "IRT":
                free_balance = free_balance / 10
                total_balance = total_balance / 10

            self._account_available_balances[asset_name] = free_balance
            self._account_balances[asset_name] = total_balance
            remote_asset_names.add(asset_name)

        asset_names_to_remove = local_asset_names.difference(remote_asset_names)
        for asset_name in asset_names_to_remove:
            del self._account_available_balances[asset_name]
            del self._account_balances[asset_name]

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        raise NotImplementedError("Nobitex does not have this functionality")

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        api_params = {
            "clientOrderId": tracked_order.client_order_id,
        }
        if tracked_order.exchange_order_id is not None:
            try:
                api_params["id"] = int(tracked_order.exchange_order_id)
            except Exception:
                ...

        updated_order_data = await self._api_post(
            path_url=CONSTANTS.ORDER_STATUS_PATH_URL,
            data=api_params,
            is_auth_required=True,
        )
        if updated_order_data.get("status") != "ok":
            raise Exception(f"Request Failed for order {tracked_order.client_order_id}: {updated_order_data}")
        payload = updated_order_data.get("order", {})
        if not payload:
            raise Exception(f"Empty payload of order {tracked_order.client_order_id}: {updated_order_data}")

        order_id = payload.get("id")
        if order_id is None or not isinstance(order_id, int):
            raise Exception(f"Invalid id of order {tracked_order.client_order_id}: {order_id}")

        try:
            new_state = CONSTANTS.ORDER_STATE[payload.get("status")]
        except KeyError:
            raise Exception(f"Invalid status of order {tracked_order.client_order_id}: {payload.get('status')}")

        partial = payload.get("partial", False)
        if not isinstance(partial, bool):
            partial = False

        if new_state == OrderState.OPEN and partial:
            new_state = OrderState.PARTIALLY_FILLED

        order_update = OrderUpdate(
            trading_pair=tracked_order.trading_pair,
            update_timestamp=self._time_synchronizer.time(),
            new_state=new_state,
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(order_id),
        )

        return order_update

    # TODO: Double-check later
    async def _update_orders_fills(self, orders: List[InFlightOrder]):
        """
        This function overrides the default implementation in the base class
        because Nobitex's get trades endpoint returns data page by page

        This method in the base ExchangePyBase, makes an API call for each order.
        Given the rate limit of the API method and the breadth of info provided by the method
        the mitigation proposal is to collect all orders in one shot, then parse them
        Note that this is limited to 100 orders, but we got orders page by page
        """
        if len(orders) == 0:
            return

        # fist get data page by page
        page = 0
        all_fills_response = []
        while page >= 0:
            params = {"pageSize": 100}

            if page > 0:
                params["page"] = page

            fills_response = await self._api_get(
                path_url=CONSTANTS.MY_TRADES_PATH_URL, params=params, is_auth_required=True
            )

            hasnext = fills_response.get("hasNext")
            page = (page + 1) if hasnext else -1
            all_fills_response.extend(fills_response.get("trades", []))

        for order in orders:
            try:
                trade_updates = await self._nobitex_all_trades_updates_for_order(
                    order=order, all_orders_fills_response=all_fills_response
                )
                for trade_update in trade_updates:
                    self._order_tracker.process_trade_update(trade_update)
            except asyncio.CancelledError:
                raise
            except Exception as request_error:
                self.logger().warning(
                    f"Failed to fetch trade updates for order {order.client_order_id}. Error: {request_error}",
                    exc_info=request_error,
                )

    # TODO: Double-check later
    async def _nobitex_all_trades_updates_for_order(
        self, order: InFlightOrder, all_orders_fills_response: list
    ) -> List[TradeUpdate]:
        trade_updates = []

        if order.exchange_order_id is not None:
            exchange_order_id = int(order.exchange_order_id)
            # trading_pair = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)

            all_fills_response = [x for x in all_orders_fills_response if x["orderId"] == exchange_order_id]

            for trade in all_fills_response:

                if order.trade_type == TradeType.BUY:
                    commission_asset = self._convert_trading_pair_naming_mapping(trade["srcCurrency"])
                else:
                    commission_asset = self._convert_trading_pair_naming_mapping(trade["dstCurrency"])

                commission_amount = Decimal(trade["fee"])

                if commission_asset == "RLS":
                    commission_asset = "IRT"
                    commission_amount = commission_amount / 10

                quote = self._convert_trading_pair_naming_mapping(trade["dstCurrency"])

                if quote == "RLS":
                    quote = "IRT"
                    price = Decimal(trade["price"]) / 10
                else:
                    price = Decimal(trade["price"])

                fee = TradeFeeBase.new_spot_fee(
                    fee_schema=self.trade_fee_schema(),
                    trade_type=order.trade_type,
                    percent_token=commission_asset,
                    flat_fees=[TokenAmount(amount=commission_amount, token=commission_asset)],
                )

                trade_update = TradeUpdate(
                    trade_id=str(trade["id"]),
                    client_order_id=order.client_order_id,
                    exchange_order_id=str(exchange_order_id),
                    trading_pair=order.trading_pair,
                    fee=fee,
                    fill_base_amount=Decimal(trade["amount"]),
                    fill_quote_amount=Decimal(trade["amount"]) * price,
                    fill_price=price,
                    fill_timestamp=self._time_synchronizer.time(),
                )
                trade_updates.append(trade_update)

        return trade_updates

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(throttler=self._throttler, auth=self._auth)

    # TODO: Double-check later
    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return NobitexAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
        )

    # TODO: Double-check later
    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return NobitexAPIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
        )

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        mapping = bidict()
        for i, c in enumerate(exchange_info.get("coins", [])):
            name = c.get("name")
            if name is None:
                self.logger().warning(f"record {i} of coins does not have name key")
                continue
            elif not isinstance(name, str):
                self.logger().warning(f"record {i} of coins name is not string: {type(name)}")
                continue
            elif name == "":
                self.logger().warning(f"record {i} of coins name is empty")
                continue
            std_name = c.get("stdName", name)
            if not isinstance(std_name, str):
                self.logger().warning(f"record {i} of coins stdName is not string: {type(std_name)}")
                continue
            elif std_name == "":
                self.logger().warning(f"record {i} of coins stdName is empty")
                continue

            coin = c.get("coin")
            if coin is None:
                self.logger().warning(f"record {i} of coins does not have coin key")
                continue
            elif not isinstance(coin, str):
                self.logger().warning(f"record {i} of coins coin is not string: {type(coin)}")
                continue
            elif coin == "":
                self.logger().warning(f"record {i} of coins coin is empty")
                continue
            coin = coin.upper()

            mapping[std_name] = coin
        self._naming_dictionary = mapping

        mapping = bidict()

        info = exchange_info.get("nobitex", {})
        amount_precisions = info.get("amountPrecisions", {})
        price_precisions = info.get("pricePrecisions", {})

        all_pairs = set(). \
            union(set(amount_precisions.keys())). \
            union(set(price_precisions.keys()))

        for pair in all_pairs:
            if pair.endswith("USDT"):
                base = pair[:-4]
                quote = "USDT"
            elif pair.endswith("IRT"):
                base = pair[:-3]
                quote = "IRT"
            else:
                self.logger().warning(f"Error parsing the trading pair rule {pair}, does not end with USDT or IRT. Skipping.")
                continue
            mapping[combine_to_hb_trading_pair(base=base, quote=quote)] = combine_to_hb_trading_pair(base=base, quote=quote)

        self._set_trading_pair_symbol_map(mapping)

    def _convert_trading_pair_naming_mapping(self, std_name: str) -> str:
        return self._naming_dictionary.get(std_name)
