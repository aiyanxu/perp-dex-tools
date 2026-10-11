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
from typing import Tuple

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from exchanges.arcus import ArcusClient
import websockets
from datetime import datetime
import pytz
from dotenv import load_dotenv
from helpers.lighter_ws import (build_lighter_ws_url, build_lighter_base_url,
                                create_lighter_signer_client_for_env,
                                lighter_ws_connect_kwargs)

load_dotenv()


class Config:
    """Simple config class to wrap dictionary for Arcus client."""

    def __init__(self, config_dict):
        for key, value in config_dict.items():
            setattr(self, key, value)


class HedgeBot:
    """Trading bot that places post-only orders on Arcus and hedges with market orders on Lighter."""

    # Arcus offset: 3 basis points (bps) from mark price
    ARCUS_OFFSET_BPS = Decimal('0.0003')  # 3 bps = 0.03%

    # Dead man's switch: refresh every 45s, deadline 60s out (must be 5s-5min ahead)
    DMS_REFRESH_INTERVAL = 45
    DMS_DEADLINE_SECONDS = 60

    def __init__(self, ticker: str, order_quantity: Decimal, fill_timeout: int = 5,
                 iterations: int = 20, sleep_time: int = 0, max_position: Decimal = Decimal('0'),
                 direction: str = 'buy'):
        self.ticker = ticker
        self.order_quantity = order_quantity
        self.fill_timeout = fill_timeout
        self.iterations = iterations
        self.sleep_time = sleep_time
        # Opening direction on the primary exchange: 'buy' builds a long
        # position first, 'sell' builds a short position first.
        self.direction = direction.lower()
        if max_position == Decimal('0'):
            self.max_position = order_quantity
        else:
            self.max_position = max_position

        # Initialize logging to file
        os.makedirs("logs", exist_ok=True)
        self.log_filename = f"logs/arcus_{ticker}_hedge_mode_log.txt"
        self.csv_filename = f"logs/arcus_{ticker}_hedge_mode_trades.csv"

        # Initialize CSV file with headers if it doesn't exist
        self._initialize_csv_file()

        # Setup logger
        self.logger = logging.getLogger(f"hedge_bot_arcus_{ticker}")
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

        # Arcus state
        self.arcus_client: ArcusClient = None  # type: ignore[assignment]
        self.arcus_contract_id = None
        self.arcus_tick_size = None
        self.arcus_order_status = None
        self.arcus_position = Decimal(0)

        # Arcus order book state
        self.arcus_best_bid = None
        self.arcus_best_ask = None
        self.arcus_order_book_ready = False

        # Lighter order book state
        self.lighter_client = None
        self.lighter_order_book = {"bids": {}, "asks": {}}
        self.lighter_best_bid = None
        self.lighter_best_ask = None
        self.lighter_order_book_ready = False
        self.lighter_order_book_offset = 0
        self.lighter_order_book_sequence_gap = False
        self.lighter_snapshot_loaded = False
        self.lighter_order_book_lock = asyncio.Lock()

        # Lighter WebSocket state
        self.lighter_ws_task = None
        self.lighter_order_filled = False

        # Lighter order management
        self.lighter_order_status = None
        self.lighter_order_price = None
        self.lighter_order_side = None
        self.lighter_order_size = None

        # Lighter position
        self.lighter_position = Decimal(0)

        # Strategy state
        self.order_execution_complete = False

        # Dead man's switch task
        self.dms_task = None
        self._last_dms_refresh = 0.0

        # Lighter API configuration
        self.lighter_base_url = build_lighter_base_url()
        self.account_index = int(os.getenv('LIGHTER_ACCOUNT_INDEX', '0'))
        self.api_key_index = int(os.getenv('LIGHTER_API_KEY_INDEX', '0'))

    def shutdown(self, signum=None, frame=None):
        """Graceful shutdown handler."""
        self.stop_flag = True
        self.logger.info("\n🛑 Stopping...")

        if self.arcus_client:
            try:
                self.logger.info("🔌 Arcus client will be disconnected")
            except Exception as e:
                self.logger.error(f"Error disconnecting Arcus client: {e}")

        if self.lighter_ws_task and not self.lighter_ws_task.done():
            try:
                self.lighter_ws_task.cancel()
                self.logger.info("🔌 Lighter WebSocket task cancelled")
            except Exception as e:
                self.logger.error(f"Error cancelling Lighter WebSocket task: {e}")

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

    def handle_lighter_order_result(self, order_data):
        """Handle Lighter order result from WebSocket."""
        try:
            order_data["avg_filled_price"] = (Decimal(order_data["filled_quote_amount"]) /
                                              Decimal(order_data["filled_base_amount"]))
            if order_data["is_ask"]:
                order_data["side"] = "SHORT"
                order_type = "OPEN"
                self.lighter_position -= Decimal(order_data["filled_base_amount"])
            else:
                order_data["side"] = "LONG"
                order_type = "CLOSE"
                self.lighter_position += Decimal(order_data["filled_base_amount"])

            client_order_index = order_data["client_order_id"]

            self.logger.info(f"[{client_order_index}] [{order_type}] [Lighter] [FILLED]: "
                             f"{order_data['filled_base_amount']} @ {order_data['avg_filled_price']}")

            self.log_trade_to_csv(
                exchange='Lighter',
                side=order_data['side'],
                price=str(order_data['avg_filled_price']),
                quantity=str(order_data['filled_base_amount'])
            )

            self.lighter_order_filled = True
            self.order_execution_complete = True

        except Exception as e:
            self.logger.error(f"Error handling Lighter order result: {e}")

    async def reset_lighter_order_book(self):
        """Reset Lighter order book state."""
        async with self.lighter_order_book_lock:
            self.lighter_order_book["bids"].clear()
            self.lighter_order_book["asks"].clear()
            self.lighter_order_book_offset = 0
            self.lighter_order_book_sequence_gap = False
            self.lighter_snapshot_loaded = False
            self.lighter_best_bid = None
            self.lighter_best_ask = None

    def update_lighter_order_book(self, side: str, levels: list):
        """Update Lighter order book with new levels."""
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
                self.lighter_order_book[side][price] = size
            else:
                self.lighter_order_book[side].pop(price, None)

    def get_lighter_best_levels(self) -> Tuple[Tuple[Decimal, Decimal], Tuple[Decimal, Decimal]]:
        """Get best bid and ask levels from Lighter order book."""
        best_bid = None
        best_ask = None

        if self.lighter_order_book["bids"]:
            best_bid_price = max(self.lighter_order_book["bids"].keys())
            best_bid_size = self.lighter_order_book["bids"][best_bid_price]
            best_bid = (best_bid_price, best_bid_size)

        if self.lighter_order_book["asks"]:
            best_ask_price = min(self.lighter_order_book["asks"].keys())
            best_ask_size = self.lighter_order_book["asks"][best_ask_price]
            best_ask = (best_ask_price, best_ask_size)

        return best_bid, best_ask

    async def handle_lighter_ws(self):
        """Handle Lighter WebSocket connection and messages."""
        url = build_lighter_ws_url()

        while not self.stop_flag:
            timeout_count = 0
            try:
                await self.reset_lighter_order_book()

                async with websockets.connect(url, **lighter_ws_connect_kwargs()) as ws:
                    # Subscribe to order book updates
                    await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{self.lighter_market_index}"}))

                    # Subscribe to account orders updates
                    account_orders_channel = f"account_orders/{self.lighter_market_index}/{self.account_index}"

                    try:
                        auth_token, err = self.lighter_client.create_auth_token_with_expiry(api_key_index=self.api_key_index)
                        if err is None:
                            auth_message = {
                                "type": "subscribe",
                                "channel": account_orders_channel,
                                "auth": auth_token
                            }
                            await ws.send(json.dumps(auth_message))
                            self.logger.info("✅ Subscribed to account orders with auth token")
                    except Exception as e:
                        self.logger.warning(f"⚠️ Error creating auth token: {e}")

                    while not self.stop_flag:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=1)

                            try:
                                data = json.loads(msg)
                            except json.JSONDecodeError:
                                continue

                            timeout_count = 0

                            async with self.lighter_order_book_lock:
                                if data.get("type") == "subscribed/order_book":
                                    self.lighter_order_book["bids"].clear()
                                    self.lighter_order_book["asks"].clear()

                                    order_book = data.get("order_book", {})
                                    if order_book and "offset" in order_book:
                                        self.lighter_order_book_offset = order_book["offset"]

                                    self.update_lighter_order_book("bids", order_book.get("bids", []))
                                    self.update_lighter_order_book("asks", order_book.get("asks", []))
                                    self.lighter_snapshot_loaded = True
                                    self.lighter_order_book_ready = True

                                    self.logger.info(f"✅ Lighter order book snapshot loaded")

                                elif data.get("type") == "update/order_book" and self.lighter_snapshot_loaded:
                                    order_book = data.get("order_book", {})
                                    if not order_book or "offset" not in order_book:
                                        continue

                                    self.update_lighter_order_book("bids", order_book.get("bids", []))
                                    self.update_lighter_order_book("asks", order_book.get("asks", []))

                                    best_bid, best_ask = self.get_lighter_best_levels()
                                    if best_bid is not None:
                                        self.lighter_best_bid = best_bid[0]
                                    if best_ask is not None:
                                        self.lighter_best_ask = best_ask[0]

                                elif data.get("type") == "ping":
                                    await ws.send(json.dumps({"type": "pong"}))

                                elif data.get("type") == "update/account_orders":
                                    orders = data.get("orders", {}).get(str(self.lighter_market_index), [])
                                    for order in orders:
                                        if order.get("status") == "filled":
                                            self.handle_lighter_order_result(order)

                        except asyncio.TimeoutError:
                            timeout_count += 1
                            continue
                        except websockets.exceptions.ConnectionClosed:
                            break
                        except Exception as e:
                            self.logger.error(f"⚠️ Error in Lighter websocket: {e}")
                            break

            except Exception as e:
                self.logger.error(f"⚠️ Failed to connect to Lighter websocket: {e}")

            await asyncio.sleep(2)

    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    async def refresh_dead_mans_switch(self):
        """Keep the Arcus dead man's switch armed.

        Arcus orders rest until goodTilTime (>= 1 month). If this bot dies with
        orders on the book, they would stay there unhedged. The switch fires
        cancelAllOrders when we stop refreshing it.
        """
        while not self.stop_flag:
            try:
                deadline_us = (time.time_ns() // 1000
                               + self.DMS_DEADLINE_SECONDS * 1_000_000)
                await self.arcus_client.schedule_cancel(
                    deadline_us=deadline_us, market_id=self.arcus_client.market_id)
                self._last_dms_refresh = time.time()
            except Exception as e:
                # Retry sooner on failure; if it stays armed from a previous
                # refresh we are still covered until its deadline passes.
                self.logger.warning(f"⚠️ Dead man's switch refresh failed: {e}")
            await asyncio.sleep(self.DMS_REFRESH_INTERVAL)

    async def cleanup_on_exit(self):
        """Disarm the dead man's switch and cancel any resting Arcus orders."""
        if not self.arcus_client:
            return
        try:
            if self.arcus_contract_id:
                open_orders = await self.arcus_client.get_active_orders(self.arcus_contract_id)
                if open_orders:
                    self.logger.info(f"🔌 Cancelling {len(open_orders)} open Arcus order(s) on exit")
                    await self.arcus_client.cancel_all_orders()
        except Exception as e:
            self.logger.error(f"Error cancelling open Arcus orders on exit: {e}")
        try:
            await self.arcus_client.schedule_cancel()  # disarm
            self.logger.info("✅ Arcus dead man's switch disarmed")
        except Exception as e:
            self.logger.error(f"Error disarming Arcus dead man's switch: {e}")
        try:
            await self.arcus_client.disconnect()
            self.logger.info("🔌 Arcus client disconnected")
        except Exception as e:
            self.logger.error(f"Error disconnecting Arcus client: {e}")

    def initialize_lighter_client(self):
        """Initialize the Lighter client."""
        if self.lighter_client is None:
            api_key_private_key = os.getenv('API_KEY_PRIVATE_KEY')
            if not api_key_private_key:
                raise Exception("API_KEY_PRIVATE_KEY environment variable not set")

            self.lighter_client = create_lighter_signer_client_for_env(
                account_index=self.account_index,
                api_private_keys={self.api_key_index: api_key_private_key}
            )

            err = self.lighter_client.check_client()
            if err is not None:
                raise Exception(f"CheckClient error: {err}")

            self.logger.info("✅ Lighter client initialized successfully")
        return self.lighter_client

    def initialize_arcus_client(self):
        """Initialize the Arcus client."""
        config_dict = {
            'ticker': self.ticker,
            'contract_id': '',
            'quantity': self.order_quantity,
            'tick_size': Decimal('0.01'),
            'close_order_side': 'sell'
        }

        config = Config(config_dict)
        self.arcus_client = ArcusClient(config)
        self.logger.info("✅ Arcus client initialized successfully")
        return self.arcus_client

    def get_lighter_market_config(self) -> Tuple[int, int, int, Decimal]:
        """Get Lighter market configuration."""
        url = f"{self.lighter_base_url}/api/v1/orderBooks"
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
            raise Exception(f"Ticker {self.ticker} not found")

        except Exception as e:
            self.logger.error(f"⚠️ Error getting market config: {e}")
            raise

    async def get_arcus_contract_info(self) -> Tuple[str, Decimal]:
        """Get Arcus contract ID and tick size."""
        if not self.arcus_client:
            raise Exception("Arcus client not initialized")

        contract_id, tick_size = await self.arcus_client.get_contract_attributes()
        return contract_id, tick_size

    async def get_arcus_position(self) -> Decimal:
        """Get Arcus position (signed: positive long, negative short)."""
        if not self.arcus_client:
            raise Exception("Arcus client not initialized")

        return await self.arcus_client.get_account_positions()

    async def fetch_arcus_bbo_prices(self) -> Tuple[Decimal, Decimal]:
        """Fetch best bid/ask prices from Arcus."""
        if not self.arcus_client:
            raise Exception("Arcus client not initialized")

        best_bid, best_ask = await self.arcus_client.fetch_bbo_prices(self.arcus_contract_id)
        return best_bid, best_ask

    async def fetch_arcus_mark_price(self) -> Decimal:
        """Fetch mark price from Arcus."""
        if not self.arcus_client:
            raise Exception("Arcus client not initialized")

        mark_price = await self.arcus_client.fetch_mark_price(self.arcus_contract_id)
        return mark_price

    def round_to_tick(self, price: Decimal) -> Decimal:
        """Round price to tick size."""
        if self.arcus_tick_size is None:
            return price
        return (price / self.arcus_tick_size).quantize(Decimal('1')) * self.arcus_tick_size

    async def place_arcus_post_only_order(self, side: str, quantity: Decimal) -> Tuple[Decimal, Decimal]:
        """
        Place a post-only (ALO) order on Arcus with 3 bps offset from mark price.
        Returns (filled_size, order_price); filled_size == 0 means no fill.
        """
        if not self.arcus_client:
            raise Exception("Arcus client not initialized")

        self.arcus_order_status = None
        self.logger.info(f"[Arcus] [{side}] Placing POST-ONLY order with 3 bps offset from mark price")

        # Get mark price and BBO prices
        mark_price = await self.fetch_arcus_mark_price()
        best_bid, best_ask = await self.fetch_arcus_bbo_prices()

        # Calculate price with 3 bps offset from mark price
        if side.lower() == 'buy':
            order_price = mark_price * (Decimal('1') - self.ARCUS_OFFSET_BPS)
        else:
            order_price = mark_price * (Decimal('1') + self.ARCUS_OFFSET_BPS)

        order_price = self.arcus_client.round_to_tick(order_price)

        self.logger.info(f"[Arcus] [{side}] Mark: {mark_price}, BBO: {best_bid}/{best_ask}, Order price: {order_price}")

        # Place the order (ALO = add-liquidity-only / post-only)
        response = await self.arcus_client.place_order(
            side=side, quantity=quantity, price=order_price,
            order_type="LIMIT", time_in_force="ALO",
        )
        order_id = response.get('orderId')
        if not order_id:
            raise Exception(f"Order placement failed: {response}")

        self.logger.info(f"[Arcus] [{side}] Order placed: {order_id} @ {order_price} (status={response.get('status')})")

        # Poll for order completion: terminal = filled / canceled / rejected
        start_time = time.time()
        not_found_count = 0
        while not self.stop_flag:
            try:
                order_info = await self.arcus_client.get_order_info(order_id)
            except Exception as e:
                # Transient REST failure - keep polling until the fill timeout
                self.logger.warning(f"⚠️ Error polling order status: {e}")
                order_info = None

            if order_info is not None and order_info.status in ('FILLED', 'CANCELED', 'REJECTED'):
                self.arcus_order_status = order_info.status
                return order_info.filled_size, order_info.price

            if order_info is None:
                # Async placement: a fresh order 404s briefly. If it stays
                # invisible past the timeout, it never reached the book
                # (e.g. POST_ONLY_WOULD_CROSS) - bail out and let the
                # caller retry at a fresh price instead of waiting.
                if time.time() - start_time > self.fill_timeout:
                    self.logger.info(f"[Arcus] [{side}] Order never appeared on the book, retrying")
                    self.arcus_order_status = 'REJECTED'
                    return Decimal(0), Decimal(0)
                await asyncio.sleep(0.4)
                continue

            if time.time() - start_time > self.fill_timeout:
                # Price moved - cancel and report whatever filled
                new_mark_price = await self.fetch_arcus_mark_price()
                if side.lower() == 'buy':
                    current_price = new_mark_price * (Decimal('1') - self.ARCUS_OFFSET_BPS)
                else:
                    current_price = new_mark_price * (Decimal('1') + self.ARCUS_OFFSET_BPS)
                current_price = self.arcus_client.round_to_tick(current_price)

                if order_price != current_price:
                    self.logger.info(f"[Arcus] [{side}] [CANCELING] Price changed, canceling order")
                    try:
                        await self.arcus_client.cancel_order(order_id)
                        # Poll until the cancel reaches a terminal state
                        # (cancel is async too - a 202 ACK is not a done deal)
                        for _ in range(10):
                            await asyncio.sleep(0.3)
                            try:
                                order_info = await self.arcus_client.get_order_info(order_id)
                            except Exception as poll_error:
                                self.logger.warning(f"⚠️ Error polling canceled order: {poll_error}")
                                continue
                            if order_info is not None and order_info.status in ('FILLED', 'CANCELED', 'REJECTED'):
                                self.arcus_order_status = order_info.status
                                return order_info.filled_size, order_info.price
                        return Decimal(0), Decimal(0)
                    except Exception as e:
                        self.logger.error(f"❌ Error canceling Arcus order: {e}")
                        return Decimal(0), Decimal(0)
                else:
                    # Price hasn't changed, keep waiting
                    start_time = time.time()

            await asyncio.sleep(0.4)

        return Decimal(0), Decimal(0)

    async def place_lighter_market_order(self, lighter_side: str, quantity: Decimal):
        """Place a market order on Lighter for hedging."""
        if not self.lighter_client:
            self.initialize_lighter_client()

        best_bid, best_ask = self.get_lighter_best_levels()

        if lighter_side.lower() == 'buy':
            order_type = "CLOSE"
            is_ask = False
            price = best_ask[0] * Decimal('1.002')
        else:
            order_type = "OPEN"
            is_ask = True
            price = best_bid[0] * Decimal('0.998')

        self.lighter_order_filled = False
        self.lighter_order_price = price
        self.lighter_order_side = lighter_side
        self.lighter_order_size = quantity

        try:
            client_order_index = int(time.time() * 1000)
            tx, tx_hash, error = await self.lighter_client.create_order(
                market_index=self.lighter_market_index,
                client_order_index=client_order_index,
                base_amount=int(quantity * self.base_amount_multiplier),
                price=int(price * self.price_multiplier),
                is_ask=is_ask,
                order_type=self.lighter_client.ORDER_TYPE_LIMIT,
                time_in_force=self.lighter_client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
                reduce_only=False,
                trigger_price=0,
            )
            if error is not None:
                raise Exception(f"Error placing Lighter order: {error}")

            self.logger.info(f"[Lighter] [{order_type}]: {quantity}")

            await self.monitor_lighter_order(client_order_index)

            return tx_hash
        except Exception as e:
            self.logger.error(f"❌ Error placing Lighter order: {e}")
            return None

    async def monitor_lighter_order(self, client_order_index: int):
        """Monitor Lighter order for fill."""
        start_time = time.time()
        while not self.lighter_order_filled and not self.stop_flag:
            if time.time() - start_time > 30:
                self.logger.warning("⚠️ Timeout waiting for Lighter order fill")
                self.lighter_order_filled = True
                self.order_execution_complete = True
                break

            await asyncio.sleep(0.1)

    def get_lighter_position(self) -> Decimal:
        """Get Lighter position via REST API."""
        url = f"{self.lighter_base_url}/api/v1/account"
        headers = {"accept": "application/json"}
        parameters = {"by": "index", "value": self.account_index}

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
            self.logger.error(f"⚠️ Error getting Lighter position: {e}")
            return self.lighter_position

    async def trading_loop(self):
        """Main trading loop implementing the hedge strategy."""
        self.logger.info(f"🚀 Starting Arcus hedge bot for {self.ticker}")

        # Initialize clients
        try:
            self.initialize_lighter_client()
            self.initialize_arcus_client()

            # Get contract info
            self.arcus_contract_id, self.arcus_tick_size = await self.get_arcus_contract_info()
            self.lighter_market_index, self.base_amount_multiplier, self.price_multiplier, self.tick_size = self.get_lighter_market_config()

            self.logger.info(f"Contract info loaded - Arcus: {self.arcus_contract_id} (market {self.arcus_client.market_id}), "
                             f"Lighter: {self.lighter_market_index}")

        except Exception as e:
            self.logger.error(f"❌ Failed to initialize: {e}")
            self.logger.error(f"❌ Full traceback: {traceback.format_exc()}")
            return

        # Connect to Arcus (WS market data + orders channel)
        try:
            await self.arcus_client.connect()
            self.logger.info("✅ Arcus client connected")

            # Fetch initial BBO prices
            best_bid, best_ask = await self.fetch_arcus_bbo_prices()
            if best_bid > 0 and best_ask > 0:
                self.arcus_best_bid = best_bid
                self.arcus_best_ask = best_ask
                self.arcus_order_book_ready = True
                self.logger.info(f"✅ Arcus order book ready - Best bid: {best_bid}, Best ask: {best_ask}")

        except Exception as e:
            self.logger.error(f"❌ Failed to connect to Arcus: {e}")
            self.logger.error(f"❌ Full traceback: {traceback.format_exc()}")
            return

        # Setup Lighter websocket
        try:
            self.lighter_ws_task = asyncio.create_task(self.handle_lighter_ws())
            self.logger.info("✅ Lighter WebSocket task started")

            # Wait for initial Lighter order book data
            timeout = 10
            start_time = time.time()
            while not self.lighter_order_book_ready and not self.stop_flag:
                if time.time() - start_time > timeout:
                    self.logger.warning(f"⚠️ Timeout waiting for Lighter WebSocket")
                    break
                await asyncio.sleep(0.5)

        except Exception as e:
            self.logger.error(f"❌ Failed to setup Lighter websocket: {e}")
            return

        # Arm the dead man's switch (auto cancel-all if this bot dies)
        try:
            deadline_us = time.time_ns() // 1000 + self.DMS_DEADLINE_SECONDS * 1_000_000
            await self.arcus_client.schedule_cancel(
                deadline_us=deadline_us, market_id=self.arcus_client.market_id)
            self.dms_task = asyncio.create_task(self.refresh_dead_mans_switch())
            self.logger.info("✅ Arcus dead man's switch armed (60s deadline, refreshed every 45s)")
        except Exception as e:
            self.logger.warning(f"⚠️ Failed to arm dead man's switch: {e}")

        await asyncio.sleep(5)

        iterations = 0
        self.lighter_position = self.get_lighter_position()
        self.arcus_position = await self.get_arcus_position()

        while iterations < self.iterations and not self.stop_flag:
            iterations += 1
            self.logger.info("-----------------------------------------------")
            self.logger.info(f"🔄 Trading loop iteration {iterations}")
            self.logger.info("-----------------------------------------------")

            # OPEN phase: build position in self.direction on Arcus, hedge opposite on Lighter
            open_side = self.direction  # 'buy' -> long on Arcus, 'sell' -> short on Arcus
            while ((self.arcus_position < self.max_position) if open_side == 'buy'
                   else (self.arcus_position > -1 * self.max_position)) and not self.stop_flag:
                self.lighter_position = self.get_lighter_position()
                self.arcus_position = await self.get_arcus_position()
                self.logger.info(f"{open_side.capitalize()}ing up to "
                                 f"{'-' if open_side == 'sell' else ''}{self.max_position} | "
                                 f"Arcus pos: {self.arcus_position} | Lighter pos: {self.lighter_position}")

                if abs(self.arcus_position + self.lighter_position) > self.order_quantity * 2:
                    self.logger.error(f"❌ Position diff too large: {self.arcus_position + self.lighter_position}")
                    sys.exit(1)

                self.order_execution_complete = False

                try:
                    filled_size, filled_price = await self.place_arcus_post_only_order(open_side, self.order_quantity)
                    if filled_size == 0:
                        self.logger.info(f"[Arcus] [{open_side}] [CANCELLED]")
                        continue
                except Exception as e:
                    self.logger.error(f"⚠️ Error placing Arcus post only order: {e}")
                    self.logger.error(f"⚠️ Full traceback: {traceback.format_exc()}")
                    continue

                self.logger.info(f"[Arcus] [{open_side}] [FILLED]: {filled_size} @ {filled_price}")
                self.log_trade_to_csv(
                    exchange='Arcus',
                    side=open_side,
                    price=str(filled_price),
                    quantity=str(filled_size)
                )

                if open_side == 'buy':
                    self.arcus_position += filled_size
                    # Hedge on Lighter
                    await self.place_lighter_market_order('sell', abs(filled_size))
                else:
                    self.arcus_position -= abs(filled_size)
                    # Hedge on Lighter
                    await self.place_lighter_market_order('buy', abs(filled_size))

            if self.sleep_time > 0:
                self.logger.info(f"💤 Sleeping {self.sleep_time} seconds ...")
                await asyncio.sleep(self.sleep_time)

            # CLOSE phase: unwind the opened position on Arcus, hedge opposite on Lighter
            close_side = 'sell' if open_side == 'buy' else 'buy'
            exit_after_next_trade = False
            while ((self.arcus_position > -1 * self.max_position) if close_side == 'sell'
                   else (self.arcus_position < self.max_position)) and not self.stop_flag:
                self.lighter_position = self.get_lighter_position()
                self.arcus_position = await self.get_arcus_position()
                self.logger.info(f"{close_side.capitalize()}ing back from "
                                 f"{'-' if close_side == 'sell' else ''}{self.max_position} | "
                                 f"Arcus pos: {self.arcus_position} | Lighter pos: {self.lighter_position}")

                if abs(self.arcus_position + self.lighter_position) > self.order_quantity * 2:
                    self.logger.error(f"❌ Position diff too large: {self.arcus_position + self.lighter_position}")
                    sys.exit(1)

                if iterations == self.iterations:
                    # Final iteration: unwind whatever remains so we end flat
                    if close_side == 'sell' and 0 < self.arcus_position <= self.order_quantity:
                        exit_after_next_trade = True
                    elif close_side == 'buy' and -self.order_quantity <= self.arcus_position < 0:
                        exit_after_next_trade = True

                try:
                    if exit_after_next_trade:
                        filled_size, filled_price = await self.place_arcus_post_only_order(close_side, abs(self.arcus_position))
                    else:
                        filled_size, filled_price = await self.place_arcus_post_only_order(close_side, self.order_quantity)
                    if filled_size == 0:
                        self.logger.info(f"[Arcus] [{close_side}] [CANCELLED]")
                        continue
                except Exception as e:
                    self.logger.error(f"⚠️ Error placing Arcus post only order: {e}")
                    self.logger.error(f"⚠️ Full traceback: {traceback.format_exc()}")
                    continue

                self.logger.info(f"[Arcus] [{close_side}] [FILLED]: {filled_size} @ {filled_price}")
                self.log_trade_to_csv(
                    exchange='Arcus',
                    side=close_side,
                    price=str(filled_price),
                    quantity=str(filled_size)
                )

                if close_side == 'sell':
                    self.arcus_position -= abs(filled_size)
                    # Hedge on Lighter
                    await self.place_lighter_market_order('buy', abs(filled_size))
                else:
                    self.arcus_position += filled_size
                    # Hedge on Lighter
                    await self.place_lighter_market_order('sell', abs(filled_size))

                if exit_after_next_trade:
                    self.logger.info("Position back to zero. Done! Exiting...")
                    break

    async def run(self):
        """Run the hedge bot."""
        self.setup_signal_handlers()

        try:
            await self.trading_loop()
        except KeyboardInterrupt:
            self.logger.info("\n🛑 Received interrupt signal...")
        finally:
            self.logger.info("🔄 Cleaning up...")
            self.stop_flag = True
            if self.dms_task and not self.dms_task.done():
                self.dms_task.cancel()
                try:
                    await self.dms_task
                except asyncio.CancelledError:
                    pass
            await self.cleanup_on_exit()
            self.shutdown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Arcus Hedge Mode Bot')
    parser.add_argument('--ticker', type=str, required=True, help='Trading pair (e.g. BTC)')
    parser.add_argument('--quantity', type=str, required=True, help='Order quantity')
    parser.add_argument('--fill-timeout', type=int, default=5, help='Fill timeout in seconds')
    parser.add_argument('--iterations', type=int, default=20, help='Number of trading iterations')
    parser.add_argument('--sleep-time', type=int, default=0, help='Sleep time between iterations')
    parser.add_argument('--max-position', type=str, default='0', help='Maximum position size')
    parser.add_argument('--direction', type=str, default='buy', choices=['buy', 'sell'],
                        help='Opening direction on Arcus: buy (long first) or sell (short first) (default: buy)')

    args = parser.parse_args()

    bot = HedgeBot(
        ticker=args.ticker,
        order_quantity=Decimal(args.quantity),
        fill_timeout=args.fill_timeout,
        iterations=args.iterations,
        sleep_time=args.sleep_time,
        max_position=Decimal(args.max_position),
        direction=args.direction
    )

    asyncio.run(bot.run())
