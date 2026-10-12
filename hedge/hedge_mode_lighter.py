import asyncio
import json
import signal
import logging
import os
import sys
import time
import requests
import argparse
import traceback
import csv
from decimal import Decimal
from typing import Tuple, Optional, Dict

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import websockets
from datetime import datetime
import pytz
from dotenv import load_dotenv

from helpers.lighter_ws import (lighter_ws_connect_kwargs,
                                build_lighter_base_url_for_deployment,
                                build_lighter_ws_url_for_deployment,
                                create_lighter_signer_client_for_deployment)

load_dotenv()


class HedgeBot:
    """Trading bot that places post-only maker orders on Lighter mainnet and
    hedges with taker (IOC) orders on Lighter on Robinhood Chain (lighter-rh)."""

    # Maker offset from BBO, in basis points: LIGHTER_MAKER_OFFSET_BPS
    # in .env (e.g. 3 = 0.03%). Default 3 bps.
    MAKER_OFFSET_BPS = Decimal((os.getenv('LIGHTER_MAKER_OFFSET_BPS') or '3').strip() or '3') / Decimal('10000')

    def __init__(self, ticker: str, order_quantity: Decimal, fill_timeout: int = 5,
                 iterations: int = 20, sleep_time: int = 0, max_position: Decimal = Decimal('0'),
                 direction: str = 'buy'):
        self.ticker = ticker
        self.order_quantity = order_quantity
        self.fill_timeout = fill_timeout
        self.iterations = iterations
        self.sleep_time = sleep_time
        # Opening direction on the maker deployment: 'buy' builds a long
        # position first, 'sell' builds a short position first.
        self.direction = direction.lower()
        if max_position == Decimal('0'):
            self.max_position = order_quantity
        else:
            self.max_position = max_position

        # Initialize logging to file
        os.makedirs("logs", exist_ok=True)
        self.log_filename = "logs/lighter_mainnet_rh_hedge_mode_log.txt"
        self.csv_filename = "logs/lighter_mainnet_rh_hedge_mode_trades.csv"

        # Initialize CSV file with headers if it doesn't exist
        self._initialize_csv_file()

        # Setup logger
        self.logger = logging.getLogger(f"hedge_bot_lighter_{ticker}")
        self.logger.setLevel(logging.INFO)
        self.logger.handlers.clear()

        # Disable verbose logging from external libraries
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        logging.getLogger('requests').setLevel(logging.WARNING)
        logging.getLogger('websockets').setLevel(logging.WARNING)

        # Create file handler
        file_handler = logging.FileHandler(self.log_filename)
        file_handler.setLevel(logging.INFO)

        # Create console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)

        # Create formatters
        file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')

        file_handler.setFormatter(file_formatter)
        console_handler.setFormatter(console_formatter)

        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        self.logger.propagate = False

        # State management
        self.stop_flag = False
        self.order_counter = 0
        # (deployment, client_order_index) -> (seen filled_base, seen filled_quote);
        # WS updates carry cumulative amounts, this tracks what was already counted
        self._seen_filled = {}

        # Maker deployment (Lighter mainnet) state
        self.maker_client = None
        self.maker_market_index = None
        self.maker_order_book: Dict[str, Dict[Decimal, Decimal]] = {"bids": {}, "asks": {}}
        self.maker_best_bid = None
        self.maker_best_ask = None
        self.maker_order_book_ready = False
        self.maker_order_book_offset = None
        self.maker_snapshot_loaded = False
        self.maker_order_book_lock = asyncio.Lock()
        self.maker_ws_task = None
        # Fill tracking keyed by client_order_index
        self.maker_fill_events: Dict[int, asyncio.Event] = {}
        self.maker_fill_amounts: Dict[int, Decimal] = {}
        self.maker_fill_prices: Dict[int, Decimal] = {}
        self.maker_open_order_indexes: Dict[int, int] = {}
        self.maker_position = Decimal(0)
        self.maker_base_amount_multiplier = None
        self.maker_price_multiplier = None
        self.maker_tick_size = None
        self.maker_base_url = build_lighter_base_url_for_deployment("mainnet")

        # Taker deployment (lighter-rh) state
        self.taker_client = None
        self.taker_market_index = None
        self.taker_order_book: Dict[str, Dict[Decimal, Decimal]] = {"bids": {}, "asks": {}}
        self.taker_best_bid = None
        self.taker_best_ask = None
        self.taker_order_book_ready = False
        self.taker_order_book_offset = None
        self.taker_snapshot_loaded = False
        self.taker_order_book_lock = asyncio.Lock()
        self.taker_ws_task = None
        self.taker_fill_events: Dict[int, asyncio.Event] = {}
        self.taker_fill_amounts: Dict[int, Decimal] = {}
        self.taker_fill_prices: Dict[int, Decimal] = {}
        self.taker_position = Decimal(0)
        self.taker_base_amount_multiplier = None
        self.taker_price_multiplier = None
        self.taker_tick_size = None
        self.taker_base_url = build_lighter_base_url_for_deployment("rh")

        # API configuration: deployment-specific vars first, then shared defaults
        self.maker_account_index = int(os.getenv('LIGHTER_MAINNET_ACCOUNT_INDEX',
                                                 os.getenv('LIGHTER_ACCOUNT_INDEX', '0')))
        self.taker_account_index = int(os.getenv('LIGHTER_RH_ACCOUNT_INDEX',
                                                 os.getenv('LIGHTER_ACCOUNT_INDEX', '0')))
        self.maker_api_key_index = int(os.getenv('LIGHTER_MAINNET_API_KEY_INDEX',
                                                 os.getenv('LIGHTER_API_KEY_INDEX', '0')))
        self.taker_api_key_index = int(os.getenv('LIGHTER_RH_API_KEY_INDEX',
                                                 os.getenv('LIGHTER_API_KEY_INDEX', '0')))

    def shutdown(self, signum=None, frame=None):
        """Graceful shutdown handler."""
        self.stop_flag = True
        self.logger.info("\n🛑 Stopping...")

        if self.maker_ws_task and not self.maker_ws_task.done():
            try:
                self.maker_ws_task.cancel()
                self.logger.info("🔌 Maker WebSocket task cancelled")
            except Exception as e:
                self.logger.error(f"Error cancelling maker WebSocket task: {e}")

        if self.taker_ws_task and not self.taker_ws_task.done():
            try:
                self.taker_ws_task.cancel()
                self.logger.info("🔌 Taker WebSocket task cancelled")
            except Exception as e:
                self.logger.error(f"Error cancelling taker WebSocket task: {e}")

        for handler in self.logger.handlers[:]:
            try:
                handler.close()
                self.logger.removeHandler(handler)
            except Exception:
                pass

    def _initialize_csv_file(self):
        """Initialize CSV file with headers if it doesn't exist."""
        if not os.path.exists(self.csv_filename):
            with open(self.csv_filename, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['exchange', 'timestamp', 'side', 'price', 'quantity'])

    def log_trade_to_csv(self, exchange: str, side: str, price: str, quantity: str):
        """Log trade details to CSV file."""
        timestamp = datetime.now(pytz.UTC).isoformat()
        with open(self.csv_filename, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([exchange, timestamp, side, price, quantity])

    # ------------------------------------------------------------------
    # Generic order book helpers shared by both deployments
    # ------------------------------------------------------------------

    def update_order_book(self, order_book: Dict[str, Dict[Decimal, Decimal]], side: str, levels: list):
        """Update a deployment order book with new levels."""
        for level in levels:
            if isinstance(level, list) and len(level) >= 2:
                price = Decimal(level[0])
                size = Decimal(level[1])
            elif isinstance(level, dict):
                price = Decimal(level.get("price", 0))
                size = Decimal(level.get("size", 0))
            else:
                continue

            if size > 0:
                order_book[side][price] = size
            else:
                order_book[side].pop(price, None)

    def get_best_levels(self, order_book: Dict[str, Dict[Decimal, Decimal]]) -> Tuple[Optional[Tuple[Decimal, Decimal]], Optional[Tuple[Decimal, Decimal]]]:
        """Get best bid and ask levels from an order book."""
        best_bid = None
        best_ask = None

        if order_book["bids"]:
            best_bid_price = max(order_book["bids"].keys())
            best_bid_size = order_book["bids"][best_bid_price]
            best_bid = (best_bid_price, best_bid_size)

        if order_book["asks"]:
            best_ask_price = min(order_book["asks"].keys())
            best_ask_size = order_book["asks"][best_ask_price]
            best_ask = (best_ask_price, best_ask_size)

        return best_bid, best_ask

    def handle_order_result(self, order_data: dict, deployment: str):
        """Handle an order update from either deployment's WebSocket.

        filled_base_amount / filled_quote_amount are cumulative across updates
        for the same order, so only the newly filled delta is applied to
        positions and logs; repeated updates for the same fill are ignored.
        """
        try:
            filled_base_total = Decimal(order_data["filled_base_amount"])
            filled_quote_total = Decimal(order_data["filled_quote_amount"])
            client_order_index = int(order_data.get("client_order_index",
                                                    order_data.get("client_order_id", 0)))

            # Delta since the last update we saw for this order
            seen_base, seen_quote = self._seen_filled.get(
                (deployment, client_order_index), (Decimal(0), Decimal(0)))
            filled_base = filled_base_total - seen_base
            filled_quote = filled_quote_total - seen_quote
            if filled_base < 0 or filled_quote < 0:
                # Should not happen (amounts only grow); reset defensively
                filled_base = filled_base_total
                filled_quote = filled_quote_total
            self._seen_filled[(deployment, client_order_index)] = (filled_base_total, filled_quote_total)

            if filled_base <= 0:
                # No new fill on this update (e.g. a plain cancel) - still
                # wake the waiting waiter so it can stop waiting
                events = self.maker_fill_events if deployment == "mainnet" else self.taker_fill_events
                event = events.get(client_order_index)
                if event is not None:
                    event.set()
                return

            avg_price = filled_quote / filled_base
            overall_avg_price = (filled_quote_total / filled_base_total
                                 if filled_base_total > 0 else Decimal(0))
            side = "SELL" if order_data["is_ask"] else "BUY"

            self.logger.info(f"[{client_order_index}] [{deployment}] [FILLED]: "
                             f"{filled_base} @ {avg_price}")

            self.log_trade_to_csv(
                exchange=deployment,
                side=side,
                price=str(avg_price),
                quantity=str(filled_base)
            )

            # Accumulate into the order's recorded fill totals for waiters
            if deployment == "mainnet":
                if order_data["is_ask"]:
                    self.maker_position -= filled_base
                else:
                    self.maker_position += filled_base
                self.maker_fill_amounts[client_order_index] = (
                    self.maker_fill_amounts.get(client_order_index, Decimal(0)) + filled_base)
                self.maker_fill_prices[client_order_index] = overall_avg_price
                event = self.maker_fill_events.get(client_order_index)
                if event is not None:
                    event.set()
            else:  # taker (lighter-rh)
                if order_data["is_ask"]:
                    self.taker_position -= filled_base
                else:
                    self.taker_position += filled_base
                self.taker_fill_amounts[client_order_index] = (
                    self.taker_fill_amounts.get(client_order_index, Decimal(0)) + filled_base)
                self.taker_fill_prices[client_order_index] = overall_avg_price
                event = self.taker_fill_events.get(client_order_index)
                if event is not None:
                    event.set()

        except Exception as e:
            self.logger.error(f"Error handling order result: {e}")

    async def handle_ws(self, deployment: str):
        """Handle WebSocket connection and messages for a deployment."""
        if deployment == "mainnet":
            ws_url = build_lighter_ws_url_for_deployment("mainnet")
            market_index = self.maker_market_index
            order_book = self.maker_order_book
            order_book_lock = self.maker_order_book_lock
            client = self.maker_client
            account_index = self.maker_account_index
            api_key_index = self.maker_api_key_index
        else:
            ws_url = build_lighter_ws_url_for_deployment("rh")
            market_index = self.taker_market_index
            order_book = self.taker_order_book
            order_book_lock = self.taker_order_book_lock
            client = self.taker_client
            account_index = self.taker_account_index
            api_key_index = self.taker_api_key_index

        if client is None:
            self.logger.error(f"⚠️ [{deployment}] Client not initialized, websocket handler exiting")
            return

        while not self.stop_flag:
            try:
                async with websockets.connect(ws_url, **lighter_ws_connect_kwargs()) as ws:
                    # Subscribe to order book updates
                    await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{market_index}"}))

                    # Subscribe to account orders updates
                    account_orders_channel = f"account_orders/{market_index}/{account_index}"

                    try:
                        auth_token, err = client.create_auth_token_with_expiry(api_key_index=api_key_index)
                        if err is None:
                            auth_message = {
                                "type": "subscribe",
                                "channel": account_orders_channel,
                                "auth": auth_token
                            }
                            await ws.send(json.dumps(auth_message))
                            self.logger.info(f"✅ [{deployment}] Subscribed to account orders with auth token")
                    except Exception as e:
                        self.logger.warning(f"⚠️ [{deployment}] Error creating auth token: {e}")

                    while not self.stop_flag:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=1)

                            try:
                                data = json.loads(msg)
                            except json.JSONDecodeError:
                                continue

                            async with order_book_lock:
                                if data.get("type") == "subscribed/order_book":
                                    order_book["bids"].clear()
                                    order_book["asks"].clear()

                                    ob = data.get("order_book", {})
                                    if ob and "offset" in ob:
                                        if deployment == "mainnet":
                                            self.maker_order_book_offset = ob["offset"]
                                        else:
                                            self.taker_order_book_offset = ob["offset"]

                                    self.update_order_book(order_book, "bids", ob.get("bids", []))
                                    self.update_order_book(order_book, "asks", ob.get("asks", []))
                                    if deployment == "mainnet":
                                        self.maker_snapshot_loaded = True
                                        self.maker_order_book_ready = True
                                    else:
                                        self.taker_snapshot_loaded = True
                                        self.taker_order_book_ready = True

                                    self.logger.info(f"✅ [{deployment}] Order book snapshot loaded")

                                elif data.get("type") == "update/order_book":
                                    if deployment == "mainnet" and not self.maker_snapshot_loaded:
                                        continue
                                    if deployment == "rh" and not self.taker_snapshot_loaded:
                                        continue

                                    ob = data.get("order_book", {})
                                    if not ob or "offset" not in ob:
                                        continue

                                    self.update_order_book(order_book, "bids", ob.get("bids", []))
                                    self.update_order_book(order_book, "asks", ob.get("asks", []))

                                    best_bid, best_ask = self.get_best_levels(order_book)
                                    if deployment == "mainnet":
                                        if best_bid is not None:
                                            self.maker_best_bid = best_bid[0]
                                        if best_ask is not None:
                                            self.maker_best_ask = best_ask[0]
                                    else:
                                        if best_bid is not None:
                                            self.taker_best_bid = best_bid[0]
                                        if best_ask is not None:
                                            self.taker_best_ask = best_ask[0]

                                elif data.get("type") == "ping":
                                    await ws.send(json.dumps({"type": "pong"}))

                                elif data.get("type") == "update/account_orders":
                                    orders = data.get("orders", {}).get(str(market_index), [])
                                    for order in orders:
                                        # Every order update flows through
                                        # handle_order_result; zero-fill updates
                                        # (plain cancels) only wake waiters
                                        self.handle_order_result(order, deployment)

                        except asyncio.TimeoutError:
                            continue
                        except websockets.exceptions.ConnectionClosed:
                            break
                        except Exception as e:
                            self.logger.error(f"⚠️ [{deployment}] Error in websocket: {e}")
                            break

            except Exception as e:
                self.logger.error(f"⚠️ [{deployment}] Failed to connect to websocket: {e}")

            await asyncio.sleep(2)

    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    # ------------------------------------------------------------------
    # Client initialization
    # ------------------------------------------------------------------

    def _read_deployment_key(self, deployment: str) -> str:
        """Read the API private key for a deployment.

        Precedence: deployment-specific vars (LIGHTER_MAINNET_API_KEY_PRIVATE_KEY /
        LIGHTER_RH_API_KEY_PRIVATE_KEY) first, then the shared API_KEY_PRIVATE_KEY.
        """
        prefix = "MAINNET" if deployment == "mainnet" else "RH"
        specific = os.getenv(f'LIGHTER_{prefix}_API_KEY_PRIVATE_KEY')
        if specific and specific.strip():
            return specific.strip()
        shared = os.getenv('API_KEY_PRIVATE_KEY')
        if shared and shared.strip():
            return shared.strip()
        raise Exception(f"API private key for Lighter {deployment} not set "
                        f"(set LIGHTER_{prefix}_API_KEY_PRIVATE_KEY or API_KEY_PRIVATE_KEY)")

    def initialize_clients(self):
        """Initialize both Lighter SignerClients (maker on mainnet, taker on rh)."""
        if self.maker_client is None:
            self.maker_client = create_lighter_signer_client_for_deployment(
                "mainnet",
                account_index=self.maker_account_index,
                api_private_keys={self.maker_api_key_index: self._read_deployment_key("mainnet")},
            )
            err = self.maker_client.check_client()
            if err is not None:
                raise Exception(f"Maker (mainnet) CheckClient error: {err}")

            self.logger.info("✅ Lighter mainnet client initialized successfully")

        if self.taker_client is None:
            self.taker_client = create_lighter_signer_client_for_deployment(
                "rh",
                account_index=self.taker_account_index,
                api_private_keys={self.taker_api_key_index: self._read_deployment_key("rh")},
            )
            err = self.taker_client.check_client()
            if err is not None:
                raise Exception(f"Taker (rh) CheckClient error: {err}")

            self.logger.info("✅ Lighter rh client initialized successfully")

    def get_market_config(self, deployment: str) -> Tuple[int, int, int, Decimal]:
        """Get market configuration (market_id, size multiplier, price multiplier, tick size)."""
        base_url = self.maker_base_url if deployment == "mainnet" else self.taker_base_url
        url = f"{base_url}/api/v1/orderBooks"
        headers = {"accept": "application/json"}

        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()

            data = response.json()

            for market in data["order_books"]:
                if market["symbol"] == self.ticker:
                    price_multiplier = pow(10, market["supported_price_decimals"])
                    return (market["market_id"],
                            pow(10, market["supported_size_decimals"]),
                            price_multiplier,
                            Decimal("1") / (Decimal("10") ** market["supported_price_decimals"]))
            raise Exception(f"Ticker {self.ticker} not found on Lighter {deployment}")

        except Exception as e:
            self.logger.error(f"⚠️ [{deployment}] Error getting market config: {e}")
            raise

    def get_position(self, deployment: str) -> Decimal:
        """Get position via REST API for a deployment (signed: positive long, negative short)."""
        base_url = self.maker_base_url if deployment == "mainnet" else self.taker_base_url
        account_index = self.maker_account_index if deployment == "mainnet" else self.taker_account_index
        url = f"{base_url}/api/v1/account"
        headers = {"accept": "application/json"}
        parameters = {"by": "index", "value": account_index}

        try:
            response = requests.get(url, headers=headers, params=parameters, timeout=10)
            response.raise_for_status()

            data = response.json()

            positions = data['accounts'][0].get('positions', [])
            for position in positions:
                if position.get('symbol') == self.ticker:
                    return Decimal(position['position']) * position['sign']

            return Decimal(0)

        except Exception as e:
            self.logger.error(f"⚠️ [{deployment}] Error getting position: {e}")
            return self.maker_position if deployment == "mainnet" else self.taker_position

    def round_to_tick(self, price: Decimal) -> Decimal:
        """Round price to maker tick size."""
        if self.maker_tick_size is None:
            return price
        return (price / self.maker_tick_size).quantize(Decimal('1')) * self.maker_tick_size

    # ------------------------------------------------------------------
    # Order placement
    # ------------------------------------------------------------------

    async def place_maker_post_only_order(self, side: str, quantity: Decimal) -> Tuple[Decimal, Decimal]:
        """Place a post-only maker order on Lighter mainnet with a maker offset
        (LIGHTER_MAKER_OFFSET_BPS, default 3 bps) from BBO.

        Returns (filled_size, filled_price); filled_size == 0 means no fill.
        """
        if not self.maker_client:
            raise Exception("Maker client not initialized")
        maker_client = self.maker_client

        self.logger.info(f"[Maker] [{side}] Placing POST-ONLY order with {self.MAKER_OFFSET_BPS * 10000} bps offset from BBO")

        # Get BBO prices from the maker order book
        best_bid, best_ask = self.get_best_levels(self.maker_order_book)
        if best_bid is None or best_ask is None:
            self.logger.warning(f"[Maker] [{side}] Order book not ready, retrying")
            return Decimal(0), Decimal(0)

        # Calculate price with maker offset from BBO
        if side.lower() == 'buy':
            order_price = best_ask[0] * (Decimal('1') - self.MAKER_OFFSET_BPS)
        else:
            order_price = best_bid[0] * (Decimal('1') + self.MAKER_OFFSET_BPS)

        order_price = self.round_to_tick(order_price)

        self.logger.info(f"[Maker] [{side}] BBO: {best_bid[0]}/{best_ask[0]}, Order price: {order_price}")

        # Generate unique client order index
        client_order_index = int(time.time() * 1000)
        fill_event = asyncio.Event()
        self.maker_fill_events[client_order_index] = fill_event
        self.maker_fill_amounts[client_order_index] = Decimal(0)
        self.maker_fill_prices[client_order_index] = Decimal(0)

        is_ask = side.lower() == 'sell'
        assert self.maker_base_amount_multiplier is not None
        assert self.maker_price_multiplier is not None

        try:
            tx, tx_hash, error = await maker_client.create_order(
                market_index=self.maker_market_index,
                client_order_index=client_order_index,
                base_amount=int(quantity * self.maker_base_amount_multiplier),
                price=int(order_price * self.maker_price_multiplier),
                is_ask=is_ask,
                order_type=maker_client.ORDER_TYPE_LIMIT,
                time_in_force=maker_client.ORDER_TIME_IN_FORCE_POST_ONLY,
                reduce_only=False,
                trigger_price=0,
            )
            if error is not None:
                raise Exception(f"Error placing maker order: {error}")

            self.logger.info(f"[Maker] [{side}] Order placed: {client_order_index} @ {order_price}")

            # Wait for fill event with timeout
            start_time = time.time()
            while not fill_event.is_set() and not self.stop_flag:
                if time.time() - start_time > self.fill_timeout:
                    # Timed out - cancel the order; whatever filled races in
                    # is picked up by cancel_maker_order's settle window
                    self.logger.info(f"[Maker] [{side}] Fill timeout, canceling order {client_order_index}")
                    await self.cancel_maker_order(client_order_index, fill_event)
                    break
                await asyncio.sleep(0.1)

            return self.maker_fill_amounts[client_order_index], self.maker_fill_prices[client_order_index]

        finally:
            self.maker_fill_events.pop(client_order_index, None)
            self.maker_fill_amounts.pop(client_order_index, None)
            self.maker_fill_prices.pop(client_order_index, None)
            self._seen_filled.pop(("mainnet", client_order_index), None)

    async def cancel_maker_order(self, client_order_index: int, fill_event: Optional[asyncio.Event] = None):
        """Cancel a maker order by client_order_index.

        Looks the order up on the REST active-orders endpoint, cancels it, then
        waits briefly so a racing fill update from the WebSocket is counted
        (exits early if the fill event fires during the settle window).
        """
        if not self.maker_client:
            return
        maker_client = self.maker_client

        order_index = await self.find_order_index_by_client_id(client_order_index)
        if order_index is None:
            self.logger.info(f"[Maker] Order {client_order_index} not found (already filled or cancelled)")
            return

        try:
            cancel_tx, cancel_hash, error = await maker_client.cancel_order(
                market_index=self.maker_market_index,
                order_index=order_index,
            )
            if error is not None:
                raise Exception(f"Error cancelling maker order: {error}")

            self.logger.info(f"[Maker] Cancelled order {client_order_index} (order_index {order_index})")
        except Exception as e:
            self.logger.error(f"❌ [Maker] Error cancelling order {client_order_index}: {e}")

        # Give the WebSocket a moment to deliver any fill that raced the cancel
        start_time = time.time()
        while time.time() - start_time < 2 and not self.stop_flag:
            if fill_event is not None and fill_event.is_set():
                break
            await asyncio.sleep(0.1)

    async def find_order_index_by_client_id(self, client_order_index: int) -> Optional[int]:
        """Find an order's order_index via REST active-orders lookup."""
        if not self.maker_client:
            return None

        try:
            auth_token, err = self.maker_client.create_auth_token_with_expiry(
                api_key_index=self.maker_api_key_index)
            if err is not None:
                self.logger.error(f"⚠️ [Maker] Error creating auth token: {err}")
                return None

            url = f"{self.maker_base_url}/api/v1/accountActiveOrders"
            headers = {"accept": "application/json"}
            parameters = {
                "account_index": self.maker_account_index,
                "market_id": self.maker_market_index,
                "auth": auth_token,
            }

            response = requests.get(url, headers=headers, params=parameters, timeout=10)
            response.raise_for_status()
            data = response.json()
            for order in data.get("orders", []):
                if order.get("client_order_index") == client_order_index:
                    return order["order_index"]
            return None
        except Exception as e:
            self.logger.error(f"⚠️ [Maker] Error finding order index: {e}")
            return None

    async def place_taker_ioc_order(self, side: str, quantity: Decimal) -> Tuple[Decimal, Decimal]:
        """Place a taker IOC order on lighter-rh, crossing the spread.

        Returns (filled_size, filled_price); filled_size == 0 means no fill.
        """
        if not self.taker_client:
            raise Exception("Taker client not initialized")
        taker_client = self.taker_client

        best_bid, best_ask = self.get_best_levels(self.taker_order_book)
        if best_bid is None or best_ask is None:
            self.logger.warning(f"[Taker] [{side}] Order book not ready, retrying")
            return Decimal(0), Decimal(0)

        # Cross the spread with a small buffer so the IOC fills at BBO
        if side.lower() == 'buy':
            price = best_ask[0] * Decimal('1.002')
        else:
            price = best_bid[0] * Decimal('0.998')

        client_order_index = int(time.time() * 1000) + 1  # avoid collision with maker's ID
        fill_event = asyncio.Event()
        self.taker_fill_events[client_order_index] = fill_event
        self.taker_fill_amounts[client_order_index] = Decimal(0)
        self.taker_fill_prices[client_order_index] = Decimal(0)

        assert self.taker_base_amount_multiplier is not None
        assert self.taker_price_multiplier is not None

        try:
            tx, tx_hash, error = await taker_client.create_order(
                market_index=self.taker_market_index,
                client_order_index=client_order_index,
                base_amount=int(quantity * self.taker_base_amount_multiplier),
                price=int(price * self.taker_price_multiplier),
                is_ask=(side.lower() == 'sell'),
                order_type=taker_client.ORDER_TYPE_MARKET,
                time_in_force=taker_client.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
                reduce_only=False,
                trigger_price=0,
            )
            if error is not None:
                raise Exception(f"Error placing taker order: {error}")

            self.logger.info(f"[Taker] [{side}] IOC order placed: {client_order_index} @ {price}")

            # Wait for the fill via the WebSocket
            start_time = time.time()
            while not fill_event.is_set() and not self.stop_flag:
                if time.time() - start_time > 10:
                    self.logger.warning(f"[Taker] [{side}] Timeout waiting for fill")
                    break
                await asyncio.sleep(0.1)

            return self.taker_fill_amounts[client_order_index], self.taker_fill_prices[client_order_index]

        finally:
            self.taker_fill_events.pop(client_order_index, None)
            self.taker_fill_amounts.pop(client_order_index, None)
            self.taker_fill_prices.pop(client_order_index, None)
            self._seen_filled.pop(("rh", client_order_index), None)

    # ------------------------------------------------------------------
    # Main trading loop
    # ------------------------------------------------------------------

    async def trading_loop(self):
        """Main trading loop implementing the hedge strategy."""
        self.logger.info(f"🚀 Starting Lighter mainnet(maker) + rh(taker) hedge bot for {self.ticker}")

        # Initialize clients and fetch market configs
        try:
            self.initialize_clients()

            (self.maker_market_index,
             self.maker_base_amount_multiplier,
             self.maker_price_multiplier,
             self.maker_tick_size) = self.get_market_config("mainnet")

            (self.taker_market_index,
             self.taker_base_amount_multiplier,
             self.taker_price_multiplier,
             self.taker_tick_size) = self.get_market_config("rh")

            self.logger.info(f"Market info loaded - Maker (mainnet): {self.maker_market_index}, "
                             f"Taker (rh): {self.taker_market_index}")

        except Exception as e:
            self.logger.error(f"❌ Failed to initialize: {e}")
            self.logger.error(f"❌ Full traceback: {traceback.format_exc()}")
            return

        # Start both WebSocket handlers
        try:
            self.maker_ws_task = asyncio.create_task(self.handle_ws("mainnet"))
            self.logger.info("✅ Maker WebSocket task started")

            self.taker_ws_task = asyncio.create_task(self.handle_ws("rh"))
            self.logger.info("✅ Taker WebSocket task started")

            # Wait for both order books to be ready
            timeout = 15
            start_time = time.time()
            while not (self.maker_order_book_ready and self.taker_order_book_ready) and not self.stop_flag:
                if time.time() - start_time > timeout:
                    self.logger.warning("⚠️ Timeout waiting for order books")
                    break
                await asyncio.sleep(0.5)

        except Exception as e:
            self.logger.error(f"❌ Failed to setup websockets: {e}")
            return

        await asyncio.sleep(2)

        iterations = 0
        self.maker_position = self.get_position("mainnet")
        self.taker_position = self.get_position("rh")

        while iterations < self.iterations and not self.stop_flag:
            iterations += 1
            self.logger.info("-----------------------------------------------")
            self.logger.info(f"🔄 Trading loop iteration {iterations}")
            self.logger.info("-----------------------------------------------")

            # OPEN phase: build position in self.direction on mainnet (maker),
            # hedge opposite on rh (taker)
            open_side = self.direction
            while ((self.maker_position < self.max_position) if open_side == 'buy'
                   else (self.maker_position > -1 * self.max_position)) and not self.stop_flag:
                self.maker_position = self.get_position("mainnet")
                self.taker_position = self.get_position("rh")
                self.logger.info(f"{open_side.capitalize()}ing up to "
                                 f"{'-' if open_side == 'sell' else ''}{self.max_position} | "
                                 f"Maker pos: {self.maker_position} | Taker pos: {self.taker_position}")

                if abs(self.maker_position + self.taker_position) > self.order_quantity * 2:
                    self.logger.error(f"❌ Position diff too large: {self.maker_position + self.taker_position}")
                    sys.exit(1)

                try:
                    filled_size, filled_price = await self.place_maker_post_only_order(open_side, self.order_quantity)
                except Exception as e:
                    self.logger.error(f"⚠️ Error placing maker order: {e}")
                    self.logger.error(f"⚠️ Full traceback: {traceback.format_exc()}")
                    continue

                if filled_size == 0:
                    self.logger.info(f"[Maker] [{open_side}] [CANCELLED/NO FILL]")
                    continue

                self.logger.info(f"[Maker] [{open_side}] [FILLED]: {filled_size} @ {filled_price}")

                # Hedge on rh with a taker order in the opposite direction
                hedge_side = 'sell' if open_side == 'buy' else 'buy'
                await self.place_taker_ioc_order(hedge_side, abs(filled_size))

            if self.sleep_time > 0:
                self.logger.info(f"💤 Sleeping {self.sleep_time} seconds ...")
                await asyncio.sleep(self.sleep_time)

            # CLOSE phase: unwind the opened position on mainnet (maker),
            # hedge opposite on rh (taker)
            close_side = 'sell' if open_side == 'buy' else 'buy'
            exit_after_next_trade = False
            while ((self.maker_position > -1 * self.max_position) if close_side == 'sell'
                   else (self.maker_position < self.max_position)) and not self.stop_flag:
                self.maker_position = self.get_position("mainnet")
                self.taker_position = self.get_position("rh")
                self.logger.info(f"{close_side.capitalize()}ing back from "
                                 f"{'-' if close_side == 'sell' else ''}{self.max_position} | "
                                 f"Maker pos: {self.maker_position} | Taker pos: {self.taker_position}")

                if abs(self.maker_position + self.taker_position) > self.order_quantity * 2:
                    self.logger.error(f"❌ Position diff too large: {self.maker_position + self.taker_position}")
                    sys.exit(1)

                if iterations == self.iterations:
                    # Final iteration: unwind whatever remains so we end flat
                    if close_side == 'sell' and 0 < self.maker_position <= self.order_quantity:
                        exit_after_next_trade = True
                    elif close_side == 'buy' and -self.order_quantity <= self.maker_position < 0:
                        exit_after_next_trade = True

                if self.maker_position == 0:
                    break

                try:
                    if exit_after_next_trade:
                        filled_size, filled_price = await self.place_maker_post_only_order(close_side, abs(self.maker_position))
                    else:
                        filled_size, filled_price = await self.place_maker_post_only_order(close_side, self.order_quantity)
                except Exception as e:
                    self.logger.error(f"⚠️ Error placing maker order: {e}")
                    self.logger.error(f"⚠️ Full traceback: {traceback.format_exc()}")
                    continue

                if filled_size == 0:
                    self.logger.info(f"[Maker] [{close_side}] [CANCELLED/NO FILL]")
                    continue

                self.logger.info(f"[Maker] [{close_side}] [FILLED]: {filled_size} @ {filled_price}")

                # Hedge on rh with a taker order in the opposite direction
                hedge_side = 'sell' if close_side == 'buy' else 'buy'
                await self.place_taker_ioc_order(hedge_side, abs(filled_size))

                if exit_after_next_trade:
                    self.logger.info("Position back to zero. Done! Exiting...")
                    break

    async def cleanup_on_exit(self):
        """Cancel any resting maker orders and close clients on exit."""
        if self.maker_client:
            maker_client = self.maker_client
            try:
                auth_token, err = maker_client.create_auth_token_with_expiry(
                    api_key_index=self.maker_api_key_index)
                if err is None:
                    url = f"{self.maker_base_url}/api/v1/accountActiveOrders"
                    headers = {"accept": "application/json"}
                    parameters = {
                        "account_index": self.maker_account_index,
                        "market_id": self.maker_market_index,
                        "auth": auth_token,
                    }
                    response = requests.get(url, headers=headers, params=parameters, timeout=10)
                    response.raise_for_status()
                    orders = response.json().get("orders", [])
                    if orders:
                        self.logger.info(f"🔌 Cancelling {len(orders)} open maker order(s) on exit")
                        for order in orders:
                            try:
                                _, _, error = await maker_client.cancel_order(
                                    market_index=self.maker_market_index,
                                    order_index=order["order_index"],
                                )
                                if error is not None:
                                    self.logger.error(f"Error cancelling order {order['order_index']}: {error}")
                            except Exception as e:
                                self.logger.error(f"Error cancelling order {order['order_index']}: {e}")
            except Exception as e:
                self.logger.error(f"Error cancelling open maker orders on exit: {e}")

        for client, name in ((self.maker_client, "maker"), (self.taker_client, "taker")):
            if client:
                try:
                    await client.close()
                    self.logger.info(f"🔌 {name.capitalize()} client closed")
                except Exception as e:
                    self.logger.error(f"Error closing {name} client: {e}")

    async def run(self):
        """Run the hedge bot."""
        self.setup_signal_handlers()

        try:
            await self.trading_loop()
        except KeyboardInterrupt:
            self.logger.info("\n🛑 Received interrupt signal...")
        finally:
            self.logger.info("🔄 Cleaning up...")
            await self.cleanup_on_exit()
            self.logger.info("✅ Cleanup done")


async def main():
    """Entry point for standalone execution."""
    parser = argparse.ArgumentParser(description='Lighter mainnet maker + lighter-rh taker hedge mode')
    parser.add_argument('--ticker', type=str, default='BTC',
                        help='Ticker symbol (default: BTC)')
    parser.add_argument('--size', type=str, required=True,
                        help='Number of tokens to buy/sell per order')
    parser.add_argument('--iter', type=int, default=20,
                        help='Number of iterations to run (default: 20)')
    parser.add_argument('--fill-timeout', type=int, default=5,
                        help='Timeout in seconds for maker order fills (default: 5)')
    parser.add_argument('--sleep', type=int, default=0,
                        help='Sleep time in seconds after each phase (default: 0)')
    parser.add_argument('--env-file', type=str, default=".env",
                        help='.env file path (default: .env)')
    parser.add_argument('--max-position', type=Decimal, default=Decimal('0'),
                        help='Maximum position to hold (default: 0)')
    parser.add_argument('--direction', type=str, default='buy', choices=['buy', 'sell'],
                        help='Opening direction on the maker deployment: buy (long first) or sell (short first) (default: buy)')
    args = parser.parse_args()

    from dotenv import load_dotenv
    load_dotenv(args.env_file)

    bot = HedgeBot(
        ticker=args.ticker.upper(),
        order_quantity=Decimal(args.size),
        fill_timeout=args.fill_timeout,
        iterations=args.iter,
        sleep_time=args.sleep,
        max_position=args.max_position,
        direction=args.direction,
    )
    await bot.run()


if __name__ == "__main__":
    asyncio.run(main())
