"""
Aster exchange client implementation.

Authentication follows the Aster Futures V3 "API wallet / agent" model
(https://github.com/asterdex/api-docs/blob/master/Aster%20API%20Overview.md):

- ``ASTER_ACCOUNT_ADDRESS``  master account (login) wallet address -> request ``user``
- ``ASTER_PRIVATE_KEY``      API wallet private key; the derived address is the
                            request ``signer`` and signs every request (EIP-712,
                            keccak over the urlencoded parameter string).

Signed endpoints live under ``/fapi/v3/*`` on ``https://fapi.asterdex.com``.
Public market data endpoints do not require a signature.
"""

import os
import re
import asyncio
import json
import time
import sys
import threading
from decimal import Decimal
from typing import Dict, Any, List, Optional, Tuple
from urllib.parse import urlencode

import aiohttp
import websockets

from eth_account import Account
from eth_account.messages import encode_typed_data
from eth_utils import to_checksum_address

from .base import BaseExchangeClient, OrderResult, OrderInfo, query_retry
from helpers.logger import TradingLogger


ASTER_BASE_URL = 'https://fapi.asterdex.com'
ASTER_WS_URL = 'wss://fstream.asterdex.com'
ASTER_CHAIN_ID = 1666  # Aster L1 mainnet chain id used in the EIP-712 domain

_ADDRESS_RE = re.compile(r'^0x[0-9a-fA-F]{40}$')


def _build_signing_message(payload: str) -> Dict[str, Any]:
    """Build the EIP-712 typed-data envelope Aster V3 signs over ``payload``."""
    return {
        'types': {
            'EIP712Domain': [
                {'name': 'name', 'type': 'string'},
                {'name': 'version', 'type': 'string'},
                {'name': 'chainId', 'type': 'uint256'},
                {'name': 'verifyingContract', 'type': 'address'},
            ],
            'Message': [{'name': 'msg', 'type': 'string'}],
        },
        'primaryType': 'Message',
        'domain': {
            'name': 'AsterSignTransaction',
            'version': '1',
            'chainId': ASTER_CHAIN_ID,
            'verifyingContract': '0x0000000000000000000000000000000000000000',
        },
        'message': {'msg': payload},
    }


class AsterSigner:
    """Signs Aster Futures V3 requests with the API wallet private key.

    ``account_address`` is the master account wallet (the ``user`` parameter);
    the ``signer`` parameter is derived from ``private_key`` (the API wallet),
    matching Aster's model where the private key corresponds to the signer.
    """

    def __init__(self, account_address: str, private_key: str) -> None:
        account_address = (account_address or '').strip()
        private_key = (private_key or '').strip()

        if not _ADDRESS_RE.match(account_address):
            raise ValueError(
                f"ASTER_ACCOUNT_ADDRESS is not a valid address: {account_address!r}"
            )
        try:
            self.account = Account.from_key(private_key)
        except Exception as e:
            raise ValueError(
                "ASTER_PRIVATE_KEY is not a valid private key"
            ) from e

        self.user = to_checksum_address(account_address)
        self.signer = self.account.address
        self.private_key = private_key

        self._last_nonce = 0
        self._nonce_lock = threading.Lock()

    def _next_nonce(self) -> int:
        """Monotonic microsecond timestamp, unique per signer.

        The server accepts nonces within a 60 s window around its own time and
        rejects duplicates (tracked per agent/signer address).
        """
        with self._nonce_lock:
            nonce = int(time.time() * 1_000_000)
            if nonce <= self._last_nonce:
                nonce = self._last_nonce + 1
            self._last_nonce = nonce
            return nonce

    def sign(self, params: Dict[str, Any]) -> Tuple[str, str]:
        """Attach nonce/user/signer to ``params`` and ECDSA-sign the encoding.

        Returns ``(query_string, signature_hex)`` where ``query_string`` is the
        urlencoded parameter string that was signed (signature excluded).
        """
        payload = dict(params)
        payload['nonce'] = str(self._next_nonce())
        payload['user'] = self.user
        payload['signer'] = self.signer

        query_string = urlencode(payload)
        message = encode_typed_data(full_message=_build_signing_message(query_string))
        signature = Account.sign_message(message, private_key=self.private_key).signature.hex()
        return query_string, signature


class AsterWebSocketManager:
    """WebSocket manager for Aster order updates."""

    def __init__(self, config: Dict[str, Any], signer: AsterSigner, order_update_callback):
        self.signer = signer
        self.order_update_callback = order_update_callback
        self.websocket = None
        self.running = False
        self.base_url = ASTER_BASE_URL
        self.ws_url = ASTER_WS_URL
        self.listen_key = None
        self.logger = None
        self._keepalive_task = None
        self._last_ping_time = None
        self._last_keepalive_time = None
        self.config = config

    async def _request_listen_key(self, method: str, path: str) -> Dict[str, Any]:
        """Send a signed listen-key request (USER_STREAM endpoint)."""
        query_string, signature = self.signer.sign({})
        body = f"{query_string}&signature={signature}"
        url = f"{self.base_url}{path}"
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent': 'PythonApp/1.0'
        }

        async with aiohttp.ClientSession() as session:
            async with session.request(method, url, data=body, headers=headers) as response:
                text = await response.text()
                try:
                    result = json.loads(text)
                except (json.JSONDecodeError, ValueError):
                    result = text
                if response.status != 200:
                    raise Exception(f"Failed to {method} {path}: {response.status} {text}")
                return result

    async def _get_listen_key(self) -> str:
        """Get listen key for user data stream."""
        result = await self._request_listen_key('POST', '/fapi/v3/listenKey')
        return result.get('listenKey')

    async def _keepalive_listen_key(self) -> bool:
        """Keep alive the listen key to prevent timeout."""
        try:
            if not self.listen_key:
                return False

            await self._request_listen_key('PUT', '/fapi/v3/listenKey')
            self._last_keepalive_time = time.time()
            if self.logger:
                self.logger.log("Listen key keepalive successful", "DEBUG")
            return True
        except Exception as e:
            if self.logger:
                self.logger.log(f"Error keeping alive listen key: {e}", "ERROR")
            return False

    async def _check_connection_health(self) -> bool:
        """Check if the WebSocket connection is healthy based on message timing."""
        if not self._last_ping_time:
            return True  # No messages received yet, assume healthy

        # If we haven't received anything in 10 minutes the connection is stale
        # (the server pushes pings periodically, so a silent socket is unhealthy).
        time_since_last_ping = time.time() - self._last_ping_time
        if time_since_last_ping > 10 * 60:  # 10 minutes
            if self.logger:
                self.logger.log(
                    f"No message received for {time_since_last_ping/60:.1f} minutes, "
                    "connection may be unhealthy", "WARNING"
                )
            return False

        return True

    async def _start_keepalive_task(self):
        """Keep the listen key alive and monitor connection health."""
        while self.running:
            try:
                # Check connection health every 5 minutes
                await asyncio.sleep(5 * 60)

                if not self.running:
                    break

                # Check if connection is healthy
                if not await self._check_connection_health():
                    if self.logger:
                        self.logger.log("Connection health check failed, reconnecting...", "WARNING")
                    # Try to reconnect
                    try:
                        await self.connect()
                    except Exception as e:
                        if self.logger:
                            self.logger.log(f"Reconnection failed: {e}", "ERROR")
                        # Wait before retrying
                        await asyncio.sleep(30)
                    continue

                # Keepalive the listen key every 30 minutes (server closes it after 60)
                if not self._last_keepalive_time or time.time() - self._last_keepalive_time >= 30 * 60:
                    success = await self._keepalive_listen_key()
                    if not success:
                        if self.logger:
                            self.logger.log("Listen key keepalive failed, reconnecting...", "WARNING")
                        # Try to reconnect
                        try:
                            await self.connect()
                        except Exception as e:
                            if self.logger:
                                self.logger.log(f"Reconnection failed: {e}", "ERROR")
                            # Wait before retrying
                            await asyncio.sleep(30)

            except Exception as e:
                if self.logger:
                    self.logger.log(f"Error in keepalive task: {e}", "ERROR")
                # Wait a bit before retrying
                await asyncio.sleep(60)

    async def connect(self):
        """Connect to Aster WebSocket."""
        try:
            # Get listen key
            self.listen_key = await self._get_listen_key()
            if not self.listen_key:
                raise Exception("Failed to get listen key")

            # Connect to WebSocket
            ws_url = f"{self.ws_url}/ws/{self.listen_key}"
            self.websocket = await websockets.connect(ws_url)
            self.running = True
            self._last_ping_time = None
            self._last_keepalive_time = None

            if self.logger:
                self.logger.log("Connected to Aster WebSocket with listen key", "INFO")

            # Start keepalive task
            self._keepalive_task = asyncio.create_task(self._start_keepalive_task())

            # Start listening for messages
            await self._listen()

        except Exception as e:
            if self.logger:
                self.logger.log(f"WebSocket connection error: {e}", "ERROR")
            raise

    async def _listen(self):
        """Listen for WebSocket messages."""
        try:
            async for message in self.websocket:
                if not self.running:
                    break

                # Track any received message as a sign of life
                self._last_ping_time = time.time()

                try:
                    data = json.loads(message)
                    await self._handle_message(data)
                except json.JSONDecodeError as e:
                    if self.logger:
                        self.logger.log(f"Failed to parse WebSocket message: {e}", "ERROR")
                except Exception as e:
                    if self.logger:
                        self.logger.log(f"Error handling WebSocket message: {e}", "ERROR")

        except websockets.exceptions.ConnectionClosed:
            if self.logger:
                self.logger.log("WebSocket connection closed", "WARNING")
        except Exception as e:
            if self.logger:
                self.logger.log(f"WebSocket listen error: {e}", "ERROR")

    async def _handle_message(self, data: Dict[str, Any]):
        """Handle incoming WebSocket messages."""
        try:
            event_type = data.get('e', '')

            if event_type == 'ORDER_TRADE_UPDATE':
                await self._handle_order_update(data)
            elif event_type == 'listenKeyExpired':
                if self.logger:
                    self.logger.log("Listen key expired, reconnecting...", "WARNING")
                # Reconnect with new listen key
                await self.connect()
            else:
                if self.logger:
                    self.logger.log(f"Unknown WebSocket message: {data}", "DEBUG")

        except Exception as e:
            if self.logger:
                self.logger.log(f"Error handling WebSocket message: {e}", "ERROR")

    async def _handle_order_update(self, order_data: Dict[str, Any]):
        """Handle order update messages."""
        try:
            order_info = order_data.get('o', {})

            order_id = order_info.get('i', '')
            symbol = order_info.get('s', '')
            side = order_info.get('S', '')
            quantity = order_info.get('q', '0')
            price = order_info.get('p', '0')
            executed_qty = order_info.get('z', '0')
            status = order_info.get('X', '')

            # Map status
            status_map = {
                'NEW': 'OPEN',
                'PARTIALLY_FILLED': 'PARTIALLY_FILLED',
                'FILLED': 'FILLED',
                'CANCELED': 'CANCELED',
                'REJECTED': 'REJECTED',
                'EXPIRED': 'EXPIRED'
            }
            mapped_status = status_map.get(status, status)

            # Call the order update callback if it exists
            if hasattr(self, 'order_update_callback') and self.order_update_callback:
                if side.lower() == self.config.close_order_side:
                    order_type = "CLOSE"
                else:
                    order_type = "OPEN"

                await self.order_update_callback({
                    'order_id': order_id,
                    'side': side.lower(),
                    'order_type': order_type,
                    'status': mapped_status,
                    'size': quantity,
                    'price': price,
                    'contract_id': symbol,
                    'filled_size': executed_qty
                })

        except Exception as e:
            if self.logger:
                self.logger.log(f"Error handling order update: {e}", "ERROR")

    async def disconnect(self):
        """Disconnect from WebSocket."""
        self.running = False

        # Cancel keepalive task
        if self._keepalive_task and not self._keepalive_task.done():
            self._keepalive_task.cancel()
            try:
                await self._keepalive_task
            except asyncio.CancelledError:
                pass

        if self.websocket:
            await self.websocket.close()
            if self.logger:
                self.logger.log("WebSocket disconnected", "INFO")

    def set_logger(self, logger):
        """Set the logger instance."""
        self.logger = logger


class AsterClient(BaseExchangeClient):
    """Aster exchange client implementation."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize Aster client."""
        super().__init__(config)

        # Aster V3 credentials from environment
        self.signer = AsterSigner(
            os.getenv('ASTER_ACCOUNT_ADDRESS', ''),
            os.getenv('ASTER_PRIVATE_KEY', '')
        )
        self.base_url = ASTER_BASE_URL

        # Initialize logger early
        self.logger = TradingLogger(exchange="aster", ticker=self.config.ticker, log_to_console=False)
        self._order_update_handler = None

    def _validate_config(self) -> None:
        """Validate Aster configuration."""
        required_env_vars = ['ASTER_ACCOUNT_ADDRESS', 'ASTER_PRIVATE_KEY']
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {missing_vars}")

    async def _public_request(
        self, method: str, endpoint: str, params: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """Make an unsigned request to a public (market data) Aster V3 endpoint."""
        url = f"{self.base_url}{endpoint}"
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent': 'PythonApp/1.0'
        }

        async with aiohttp.ClientSession() as session:
            async with session.request(method.upper(), url, params=params, headers=headers) as response:
                text = await response.text()
                try:
                    result = json.loads(text)
                except (json.JSONDecodeError, ValueError):
                    result = text
                if response.status != 200:
                    raise Exception(f"API request failed: {result}")
                return result

    async def _make_request(
        self, method: str, endpoint: str, params: Dict[str, Any] = None, data: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """Make an ECDSA-signed request to an authenticated Aster V3 endpoint.

        GET parameters travel in the query string; POST/PUT/DELETE parameters
        travel in the urlencoded request body. In both cases ``signature`` is
        appended last and is never part of the signed payload.
        """
        method = method.upper()
        payload = dict(params) if params else {}
        if data:
            payload.update(data)

        query_string, signature = self.signer.sign(payload)

        url = f"{self.base_url}{endpoint}"
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent': 'PythonApp/1.0'
        }

        async with aiohttp.ClientSession() as session:
            if method == 'GET':
                url = f"{url}?{query_string}&signature={signature}"
                request_params = {'headers': headers}
            else:
                request_params = {'data': f"{query_string}&signature={signature}", 'headers': headers}

            async with session.request(method, url, **request_params) as response:
                text = await response.text()
                try:
                    result = json.loads(text)
                except (json.JSONDecodeError, ValueError):
                    result = text
                if response.status != 200:
                    raise Exception(f"API request failed: {result}")
                return result

    async def connect(self) -> None:
        """Connect to Aster WebSocket."""
        # Initialize WebSocket manager
        self.ws_manager = AsterWebSocketManager(
            config=self.config,
            signer=self.signer,
            order_update_callback=self._handle_websocket_order_update
        )

        # Set logger for WebSocket manager
        self.ws_manager.set_logger(self.logger)

        try:
            # Start WebSocket connection in background task
            asyncio.create_task(self.ws_manager.connect())
            # Wait a moment for connection to establish
            await asyncio.sleep(2)
        except Exception as e:
            self.logger.log(f"Error connecting to Aster WebSocket: {e}", "ERROR")
            raise

    async def disconnect(self) -> None:
        """Disconnect from Aster."""
        try:
            if hasattr(self, 'ws_manager') and self.ws_manager:
                await self.ws_manager.disconnect()
        except Exception as e:
            self.logger.log(f"Error during Aster disconnect: {e}", "ERROR")

    def get_exchange_name(self) -> str:
        """Get the exchange name."""
        return "aster"

    def setup_order_update_handler(self, handler) -> None:
        """Setup order update handler for WebSocket."""
        self._order_update_handler = handler

    async def _handle_websocket_order_update(self, order_data: Dict[str, Any]):
        """Handle order updates from WebSocket."""
        try:
            if self._order_update_handler:
                self._order_update_handler(order_data)
        except Exception as e:
            self.logger.log(f"Error handling WebSocket order update: {e}", "ERROR")

    @query_retry(default_return=(0, 0))
    async def fetch_bbo_prices(self, contract_id: str) -> Tuple[Decimal, Decimal]:
        """Fetch best bid and ask prices from Aster."""
        result = await self._public_request('GET', '/fapi/v3/ticker/bookTicker', {'symbol': contract_id})

        best_bid = Decimal(result.get('bidPrice', 0))
        best_ask = Decimal(result.get('askPrice', 0))

        return best_bid, best_ask

    async def get_order_price(self, direction: str) -> Decimal:
        """Get the price of an order with Aster using official SDK."""
        best_bid, best_ask = await self.fetch_bbo_prices(self.config.contract_id)
        if best_bid <= 0 or best_ask <= 0:
            self.logger.log("Invalid bid/ask prices", "ERROR")
            raise ValueError("Invalid bid/ask prices")

        if direction == 'buy':
            # For buy orders, place slightly below best ask to ensure execution
            order_price = best_ask - self.config.tick_size
        else:
            # For sell orders, place slightly above best bid to ensure execution
            order_price = best_bid + self.config.tick_size
        return order_price

    async def place_open_order(self, contract_id: str, quantity: Decimal, direction: str) -> OrderResult:
        """Place an open order with Aster."""
        attempt = 0
        while True:
            attempt += 1
            if attempt % 5 == 0:
                self.logger.log(f"[OPEN] Attempt {attempt} to place order", "INFO")
                active_orders = await self.get_active_orders(contract_id)
                active_open_orders = 0
                for order in active_orders:
                    if order.side == self.config.direction:
                        active_open_orders += 1
                if active_open_orders > 1:
                    self.logger.log(f"[OPEN] ERROR: Active open orders abnormal: {active_open_orders}", "ERROR")
                    raise Exception(f"[OPEN] ERROR: Active open orders abnormal: {active_open_orders}")

            best_bid, best_ask = await self.fetch_bbo_prices(contract_id)

            if best_bid <= 0 or best_ask <= 0:
                return OrderResult(success=False, error_message='Invalid bid/ask prices')

            # Determine order side and price
            if direction == 'buy':
                # For buy orders, place slightly below best ask to ensure execution
                price = best_ask - self.config.tick_size
            elif direction == 'sell':
                # For sell orders, place slightly above best bid to ensure execution
                price = best_bid + self.config.tick_size
            else:
                raise Exception(f"[OPEN] Invalid direction: {direction}")

            # Place the order
            order_data = {
                'symbol': contract_id,
                'side': direction.upper(),
                'type': 'LIMIT',
                'quantity': str(quantity),
                'price': str(price),
                'timeInForce': 'GTX'  # GTX is Good Till Crossing (Post Only)
            }

            result = await self._make_request('POST', '/fapi/v3/order', data=order_data)
            order_status = result.get('status', '')
            order_id = result.get('orderId', '')

            start_time = time.time()
            while order_status == 'NEW' and time.time() - start_time < 2:
                await asyncio.sleep(0.1)
                order_info = await self.get_order_info(order_id)
                if order_info is not None:
                    order_status = order_info.status

            if order_status in ['NEW', 'PARTIALLY_FILLED']:
                return OrderResult(success=True, order_id=order_id, side=direction, size=quantity, price=price, status='OPEN')
            elif order_status == 'FILLED':
                return OrderResult(success=True, order_id=order_id, side=direction, size=quantity, price=price, status='FILLED')
            elif order_status == 'EXPIRED':
                continue
            else:
                return OrderResult(success=False, error_message='Unknown order status: ' + order_status)

    async def _get_active_close_orders(self, contract_id: str) -> int:
        """Get active close orders for a contract using official SDK."""
        active_orders = await self.get_active_orders(contract_id)
        active_close_orders = 0
        for order in active_orders:
            if order.side == self.config.close_order_side:
                active_close_orders += 1
        return active_close_orders

    async def place_close_order(self, contract_id: str, quantity: Decimal, price: Decimal, side: str) -> OrderResult:
        """Place a close order with Aster."""
        attempt = 0
        active_close_orders = await self._get_active_close_orders(contract_id)
        while True:
            attempt += 1
            if attempt % 5 == 0:
                self.logger.log(f"[CLOSE] Attempt {attempt} to place order", "INFO")
                current_close_orders = await self._get_active_close_orders(contract_id)

                if current_close_orders - active_close_orders > 1:
                    self.logger.log(f"[CLOSE] ERROR: Active close orders abnormal: "
                                    f"{active_close_orders}, {current_close_orders}", "ERROR")
                    raise Exception(f"[CLOSE] ERROR: Active close orders abnormal: "
                                    f"{active_close_orders}, {current_close_orders}")
                else:
                    active_close_orders = current_close_orders
            # Get current market prices to adjust order price if needed
            best_bid, best_ask = await self.fetch_bbo_prices(contract_id)

            if best_bid <= 0 or best_ask <= 0:
                return OrderResult(success=False, error_message='No bid/ask data available')

            # Adjust order price based on market conditions and side
            adjusted_price = price
            if side.lower() == 'sell':
                order_side = 'SELL'
                # For sell orders, ensure price is above best bid to be a maker order
                if price <= best_bid:
                    adjusted_price = best_bid + self.config.tick_size
            elif side.lower() == 'buy':
                order_side = 'BUY'
                # For buy orders, ensure price is below best ask to be a maker order
                if price >= best_ask:
                    adjusted_price = best_ask - self.config.tick_size

            adjusted_price = self.round_to_tick(adjusted_price)

            # Place the order
            order_data = {
                'symbol': contract_id,
                'side': order_side,
                'type': 'LIMIT',
                'quantity': str(quantity),
                'price': str(adjusted_price),
                'timeInForce': 'GTX'  # GTX is Good Till Crossing (Post Only)
            }

            result = await self._make_request('POST', '/fapi/v3/order', data=order_data)
            order_status = result.get('status', '')
            order_id = result.get('orderId', '')

            start_time = time.time()
            while order_status == 'NEW' and time.time() - start_time < 2:
                await asyncio.sleep(0.1)
                order_info = await self.get_order_info(order_id)
                if order_info is not None:
                    order_status = order_info.status

            if order_status in ['NEW', 'PARTIALLY_FILLED']:
                return OrderResult(success=True, order_id=order_id, side=order_side.lower(),
                                   size=quantity, price=adjusted_price, status='OPEN')
            elif order_status == 'FILLED':
                return OrderResult(success=True, order_id=order_id, side=order_side.lower(),
                                   size=quantity, price=adjusted_price, status='FILLED')
            elif order_status == 'EXPIRED':
                continue
            else:
                return OrderResult(success=False, error_message='Unknown order status: ' + order_status)

    async def place_market_order(self, contract_id: str, quantity: Decimal, direction: str) -> OrderResult:
        """Place a market order with Aster."""
        # Validate direction
        if direction.lower() not in ['buy', 'sell']:
            return OrderResult(success=False, error_message=f'Invalid direction: {direction}')

        # Place the market order
        order_data = {
            'symbol': contract_id,
            'side': direction.upper(),
            'type': 'MARKET',
            'quantity': str(quantity)
        }

        result = await self._make_request('POST', '/fapi/v3/order', data=order_data)
        order_status = result.get('status', '')
        order_id = result.get('orderId', '')

        start_time = time.time()
        while order_status != 'FILLED' and time.time() - start_time < 10:
            await asyncio.sleep(0.2)
            order_info = await self.get_order_info(order_id)
            if order_info is not None:
                order_status = order_info.status

        if order_status != 'FILLED':
            self.logger.log(f"Market order failed with status: {order_status}", "ERROR")
            sys.exit(1)
        # For market orders, we expect them to be filled immediately
        else:
            return OrderResult(
                success=True,
                order_id=order_id,
                side=direction.lower(),
                size=quantity,
                price=order_info.price,
                status='FILLED'
            )

    async def cancel_order(self, order_id: str) -> OrderResult:
        """Cancel an order with Aster."""
        try:
            result = await self._make_request('DELETE', '/fapi/v3/order', {
                'symbol': self.config.contract_id,
                'orderId': order_id
            })

            if 'orderId' in result:
                return OrderResult(success=True, filled_size=Decimal(result.get('executedQty', 0)))
            else:
                return OrderResult(success=False, error_message=result.get('msg', 'Unknown error'))

        except Exception as e:
            return OrderResult(success=False, error_message=str(e))

    @query_retry()
    async def get_order_info(self, order_id: str) -> Optional[OrderInfo]:
        """Get order information from Aster."""
        result = await self._make_request('GET', '/fapi/v3/order', {
            'symbol': self.config.contract_id,
            'orderId': order_id
        })

        order_type = result.get('type', '')
        if order_type == 'MARKET':
            price = Decimal(result.get('avgPrice', 0))
        else:
            price = Decimal(result.get('price', 0))

        if 'orderId' in result:
            return OrderInfo(
                order_id=str(result['orderId']),
                side=result.get('side', '').lower(),
                size=Decimal(result.get('origQty', 0)),
                price=price,
                status=result.get('status', ''),
                filled_size=Decimal(result.get('executedQty', 0)),
                remaining_size=Decimal(result.get('origQty', 0)) - Decimal(result.get('executedQty', 0))
            )
        return None

    @query_retry(default_return=[])
    async def get_active_orders(self, contract_id: str) -> List[OrderInfo]:
        """Get active orders for a contract from Aster."""
        result = await self._make_request('GET', '/fapi/v3/openOrders', {'symbol': contract_id})

        orders = []
        for order in result:
            orders.append(OrderInfo(
                order_id=str(order['orderId']),
                side=order.get('side', '').lower(),
                size=Decimal(order.get('origQty', 0)) - Decimal(order.get('executedQty', 0)),
                price=Decimal(order.get('price', 0)),
                status=order.get('status', ''),
                filled_size=Decimal(order.get('executedQty', 0)),
                remaining_size=Decimal(order.get('origQty', 0)) - Decimal(order.get('executedQty', 0))
            ))

        return orders

    @query_retry(reraise=True)
    async def get_account_positions(self) -> Decimal:
        """Get account positions from Aster."""
        result = await self._make_request('GET', '/fapi/v3/positionRisk', {'symbol': self.config.contract_id})

        for position in result:
            if position.get('symbol') == self.config.contract_id:
                position_amt = abs(Decimal(position.get('positionAmt', 0)))
                return position_amt

        return Decimal(0)

    async def get_contract_attributes(self) -> Tuple[str, Decimal]:
        """Get contract ID and tick size for a ticker."""
        ticker = self.config.ticker
        if len(ticker) == 0:
            self.logger.log("Ticker is empty", "ERROR")
            raise ValueError("Ticker is empty")

        try:
            result = await self._public_request('GET', '/fapi/v3/exchangeInfo')

            for symbol_info in result['symbols']:
                if (symbol_info.get('status') == 'TRADING' and
                        symbol_info.get('baseAsset') == ticker and
                        symbol_info.get('quoteAsset') == 'USDT'):

                    self.config.contract_id = symbol_info.get('symbol', '')

                    # Get tick size from filters
                    for filter_info in symbol_info.get('filters', []):
                        if filter_info.get('filterType') == 'PRICE_FILTER':
                            self.config.tick_size = Decimal(filter_info['tickSize'].strip('0'))
                            break

                    # Get minimum quantity
                    min_quantity = Decimal(0)
                    for filter_info in symbol_info.get('filters', []):
                        if filter_info.get('filterType') == 'LOT_SIZE':
                            min_quantity = Decimal(filter_info.get('minQty', 0))
                            break

                    if self.config.quantity < min_quantity:
                        self.logger.log(
                            f"Order quantity is less than min quantity: "
                            f"{self.config.quantity} < {min_quantity}", "ERROR"
                        )
                        raise ValueError(
                            f"Order quantity is less than min quantity: "
                            f"{self.config.quantity} < {min_quantity}"
                        )

                    if self.config.tick_size == 0:
                        self.logger.log("Failed to get tick size for ticker", "ERROR")
                        raise ValueError("Failed to get tick size for ticker")

                    return self.config.contract_id, self.config.tick_size

            self.logger.log("Failed to get contract ID for ticker", "ERROR")
            raise ValueError("Failed to get contract ID for ticker")

        except Exception as e:
            self.logger.log(f"Error getting contract attributes: {e}", "ERROR")
            raise
