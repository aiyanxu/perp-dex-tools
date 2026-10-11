"""
Arcus exchange client implementation.
API docs: https://docs.arcus.xyz/api-reference/introduction

Non-custodial perpetuals exchange (https://app.arcus.xyz). Authentication model:
- An Ed25519 API key is registered against a master Ethereum address (done once
  in the web app or via POST /v1/createApiKey with an EIP-712 signature).
- Order requests (placeOrder/cancelOrder) sign a typed canonical payload with
  the Ed25519 private key; headers X-API-Key / X-Timestamp (unix ns) /
  X-Signature carry the credentials.

Signed payload (place, op=1):
    {"ad":addr,"ai":N,"c":clientId?,"ct":tsNs,"g":goodTilNs,"m":marketId,
     "op":1,"p":priceTicks,"q":sizeQuantums,"r":reduceOnly,"s":side,"t":tif,"v":1}
Signed payload (cancel, op=2):
    {"ad":addr,"ai":N,"c":clientId?,"ct":tsNs,"id":orderId,"m":marketId,"op":2,"v":1}
Both are compact JSON with alphabetically sorted keys, signed with no prefix.
"""

import os
import asyncio
import json
import time
import logging
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from typing import Any, Callable, Dict, List, Optional, Tuple

import aiohttp
import websockets
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from dotenv import load_dotenv

from .base import BaseExchangeClient, OrderResult, OrderInfo, query_retry
from helpers.logger import TradingLogger

load_dotenv()

logger = logging.getLogger(__name__)

ARCUS_MAINNET_REST_URL = "https://api.arcus.xyz"
ARCUS_MAINNET_WS_URL = "wss://api.arcus.xyz/v1/ws"
ARCUS_TESTNET_REST_URL = "https://api.testnet.arcus.xyz"
ARCUS_TESTNET_WS_URL = "wss://api.testnet.arcus.xyz/v1/ws"

# Order signing op codes (see the authentication docs)
OP_PLACE = 1
OP_CANCEL = 2

# orderSide -> signed payload integer
SIDE_INT = {"BUY": 0, "SELL": 1}
# timeInForce -> signed payload integer
TIF_INT = {"GTT": 0, "FOK": 1, "IOC": 2, "ALO": 3}

# goodTilTime must be at least one month ahead on every order type
GOOD_TIL_DAYS = 40

# Map Arcus order statuses to the bot's standard statuses
STATUS_MAP = {
    "OPEN": "OPEN",
    "ACK": "OPEN",
    "CANCEL_ACKNOWLEDGED": "OPEN",
    "CANCEL_ALL_ACKNOWLEDGED": "OPEN",
    "CANCEL_PENDING": "OPEN",
    "PENDING": "OPEN",
    "UNTRIGGERED": "OPEN",
    "PARTIALLY_FILLED": "PARTIALLY_FILLED",
    "FILLED": "FILLED",
    "CANCELED": "CANCELED",
    "MARGIN_CANCELED": "CANCELED",
    "REJECTED": "REJECTED",
    "LIQUIDATED": "CANCELED",
    "ADL": "CANCELED",
}


class ArcusApiError(Exception):
    """Error raised for non-2xx Arcus API responses."""

    def __init__(self, status: int, message: str):
        self.status = status
        super().__init__(f"HTTP {status}: {message}")


def canonical_json(obj: Any) -> str:
    """Compact JSON with sorted keys - the exact bytes Arcus signatures are made over."""
    return json.dumps(obj, separators=(",", ":"), sort_keys=True)


def to_integer_units(value: Decimal, unit: Decimal) -> int:
    """Convert a decimal to integer ticks/quantums (value / unit); must divide evenly."""
    n = Decimal(value) / Decimal(unit)
    integral = n.to_integral_value()
    if n != integral:
        raise ValueError(f"{value} is not a multiple of {unit}")
    return int(integral)


def build_sign_payload(*, address: str, account_index: int, timestamp_ns: int,
                       op: int, market_id: int,
                       good_til_ns: Optional[int] = None,
                       price_ticks: Optional[int] = None,
                       size_quantums: Optional[int] = None,
                       side: Optional[str] = None,
                       time_in_force: Optional[str] = None,
                       reduce_only: bool = False,
                       order_id: Optional[str] = None,
                       client_id: Optional[str] = None) -> str:
    """Build the typed canonical payload that placeOrder/cancelOrder sign."""
    payload: Dict[str, Any] = {
        "ad": address.lower(),
        "ai": account_index,
        "ct": timestamp_ns,
        "m": market_id,
        "op": op,
        "v": 1,
    }
    if client_id:
        payload["c"] = client_id
    if good_til_ns is not None:
        payload["g"] = good_til_ns
    if order_id is not None:
        payload["id"] = order_id
    if op == OP_PLACE:
        payload["p"] = price_ticks
        payload["q"] = size_quantums
        payload["r"] = 1 if reduce_only else 0
        payload["s"] = SIDE_INT[side or "BUY"]
        payload["t"] = TIF_INT[time_in_force or "GTT"]
    return canonical_json(payload)


class ArcusWebSocketManager:
    """WebSocket manager for Arcus: L2 orderbook deltas, mark price and account orders.

    Channels used (all multiplexed on one socket):
    - l2OrderbookUpdates: book snapshot + incremental deltas for the traded market
    - markets: periodic full snapshot; source of the mark price
    - orders: lifecycle updates for our account (fills/rejects/cancels)
    """

    def __init__(self, ws_url: str, market_name: str,
                 account_address: Optional[str] = None, account_index: int = 0,
                 n_levels: int = 50, logger=None):
        self.ws_url = ws_url
        self.market_name = market_name
        self.account_address = account_address.lower() if account_address else None
        self.account_index = account_index
        self.n_levels = n_levels
        self.logger = logger

        self.running = False

        # Order book state (l2OrderbookUpdates)
        self.order_book: Dict[str, Dict[Decimal, Decimal]] = {"bids": {}, "asks": {}}
        self.best_bid = Decimal("0")
        self.best_ask = Decimal("0")
        self.order_book_ready = False
        self._snapshot_loaded = False
        self._last_seq = 0
        self._expect_first_delta = True

        # Mark price state (markets channel)
        self.mark_price = Decimal("0")

        # Account order updates (orders channel)
        self.orders_cache: Dict[str, Dict[str, Any]] = {}
        self.order_update_handler: Optional[Callable[[Dict[str, Any]], None]] = None
    async def start(self) -> None:
        """Connect and keep the socket alive, reconnecting on failure."""
        self.running = True
        while self.running:
            try:
                async with websockets.connect(
                    self.ws_url, ping_interval=20, ping_timeout=60, max_queue=1024
                ) as ws:
                    await self._subscribe_all(ws)
                    await self._listen(ws)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if self.running:
                    self._log(f"Arcus WebSocket error: {e}, reconnecting in 2s", logging.WARNING)
                    await asyncio.sleep(2)

    def stop(self) -> None:
        self.running = False

    async def _subscribe_all(self, ws) -> None:
        self._reset_book()
        await ws.send(json.dumps({
            "type": "subscribe",
            "channel": "l2OrderbookUpdates",
            "id": self.market_name,
            "nLevels": self.n_levels,
        }))
        await ws.send(json.dumps({"type": "subscribe", "channel": "markets"}))
        if self.account_address:
            await ws.send(json.dumps({
                "type": "subscribe",
                "channel": "orders",
                "id": self.account_address,
                "accountIndex": self.account_index,
                "market": self.market_name,
            }))

    def _reset_book(self) -> None:
        self.order_book["bids"].clear()
        self.order_book["asks"].clear()
        self.best_bid = Decimal("0")
        self.best_ask = Decimal("0")
        self.order_book_ready = False
        self._snapshot_loaded = False
        self._last_seq = 0
        self._expect_first_delta = True

    async def _listen(self, ws) -> None:
        while self.running:
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=1)
            except asyncio.TimeoutError:
                continue
            except websockets.exceptions.ConnectionClosed:
                raise
            try:
                data = json.loads(msg)
            except json.JSONDecodeError:
                continue
            await self._handle_message(ws, data)

    async def _handle_message(self, ws, data: Dict[str, Any]) -> None:
        msg_type = data.get("type")
        if msg_type == "error":
            self._log(f"Arcus WebSocket error frame: {data.get('message')}", logging.WARNING)
            return
        channel = data.get("channel")
        if channel == "l2OrderbookUpdates":
            contents = data.get("contents", {})
            if msg_type == "subscribed":
                self._apply_book_snapshot(contents)
            elif msg_type == "channel_data":
                await self._apply_book_delta(ws, contents)
        elif channel == "markets":
            self._apply_markets(data.get("contents", {}))
        elif channel == "orders":
            self._apply_order_update(data.get("contents", {}))

    def _apply_book_snapshot(self, contents: Dict[str, Any]) -> None:
        self._reset_book()
        self._set_levels("bids", contents.get("bids", []))
        self._set_levels("asks", contents.get("asks", []))
        self._last_seq = int(contents.get("lastSequenceId", 0))
        self._snapshot_loaded = True
        self._update_bbo()
        self.order_book_ready = True

    async def _apply_book_delta(self, ws, contents: Dict[str, Any]) -> None:
        seq = int(contents.get("lastSequenceId", 0))
        if not self._snapshot_loaded or seq <= self._last_seq:
            return
        # The first delta after a snapshot may sit a few sequences ahead of the
        # snapshot (boundary gap) - that self-heals, so apply it regardless.
        if not self._expect_first_delta and seq > self._last_seq + 1:
            # Mid-stream gap means a missed delta - resubscribe for a fresh snapshot.
            self._log("Arcus orderbook sequence gap, resubscribing", logging.WARNING)
            self._reset_book()
            await ws.send(json.dumps({
                "type": "unsubscribe",
                "channel": "l2OrderbookUpdates",
                "id": self.market_name,
            }))
            await ws.send(json.dumps({
                "type": "subscribe",
                "channel": "l2OrderbookUpdates",
                "id": self.market_name,
                "nLevels": self.n_levels,
            }))
            return
        self._expect_first_delta = False
        self._set_levels("bids", contents.get("bids", []))
        self._set_levels("asks", contents.get("asks", []))
        self._last_seq = seq
        self._update_bbo()

    def _set_levels(self, side: str, levels: list) -> None:
        book = self.order_book[side]
        for level in levels:
            if not isinstance(level, (list, tuple)) or len(level) < 2:
                continue
            price = Decimal(level[0])
            size = Decimal(level[1])
            if size > 0:
                book[price] = size
            else:
                book.pop(price, None)

    def _update_bbo(self) -> None:
        if self.order_book["bids"]:
            self.best_bid = max(self.order_book["bids"].keys())
        else:
            self.best_bid = Decimal("0")
        if self.order_book["asks"]:
            self.best_ask = min(self.order_book["asks"].keys())
        else:
            self.best_ask = Decimal("0")

    def _apply_markets(self, contents: Dict[str, Any]) -> None:
        markets = contents.get("markets", {})
        market = next((m for m in markets.values()
                       if str(m.get("marketDisplayName", "")).upper() == self.market_name.upper()),
                      None)
        if not market:
            return
        mark_price = market.get("markPrice")
        # "0" means unavailable - do not fall back to the oracle price
        if mark_price and Decimal(mark_price) > 0:
            self.mark_price = Decimal(mark_price)

    def _apply_order_update(self, contents: Dict[str, Any]) -> None:
        order_id = contents.get("orderId")
        if not order_id:
            return
        self.orders_cache[order_id] = contents
        if self.order_update_handler:
            try:
                self.order_update_handler(contents)
            except Exception as e:
                self._log(f"Error in order update handler: {e}", logging.ERROR)

    def get_cached_order(self, order_id: str) -> Optional[Dict[str, Any]]:
        return self.orders_cache.get(order_id)

    def _log(self, message: str, level: int = logging.INFO) -> None:
        if self.logger is not None and hasattr(self.logger, "log"):
            try:
                self.logger.log(message, logging.getLevelName(level))
                return
            except Exception:
                pass
        logging.getLogger("arcus_ws").log(level, message)


class ArcusClient(BaseExchangeClient):
    """Arcus exchange client implementation."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize Arcus client."""
        super().__init__(config)

        self.wallet_address = os.getenv("ARCUS_WALLET_ADDRESS")
        self.api_private_key = os.getenv("ARCUS_API_PRIVATE_KEY")
        self.environment = os.getenv("ARCUS_ENVIRONMENT", "mainnet").lower()
        self.account_index = int(os.getenv("ARCUS_ACCOUNT_INDEX", "0"))

        if not self.wallet_address:
            raise ValueError("ARCUS_WALLET_ADDRESS must be set")

        if self.environment == "testnet":
            self.base_url = ARCUS_TESTNET_REST_URL
            self.ws_url = ARCUS_TESTNET_WS_URL
        else:
            self.base_url = ARCUS_MAINNET_REST_URL
            self.ws_url = ARCUS_MAINNET_WS_URL

        self._ed25519_key = self._load_ed25519_key(self.api_private_key)
        self.api_key = self._ed25519_key.public_key().public_bytes_raw().hex()

        self.logger = TradingLogger(exchange="arcus", ticker=self.config.ticker, log_to_console=False)

        self.http_session: Optional[aiohttp.ClientSession] = None
        self.ws_manager: Optional[ArcusWebSocketManager] = None
        self._ws_task: Optional[asyncio.Task] = None

        # Market metadata (populated by get_contract_attributes)
        self.market_id: Optional[int] = None
        self.tick_size = Decimal("0.01")
        self.step_size = Decimal("0.00000001")
        self.tick_tiers: List[Dict[str, str]] = []
        self.min_order_size = Decimal("0")
        self.min_order_notional = Decimal("0")

        self._order_update_handler = None

    @staticmethod
    def _load_ed25519_key(hex_key: Optional[str]) -> Ed25519PrivateKey:
        if not hex_key:
            raise ValueError("ARCUS_API_PRIVATE_KEY must be set")
        key_bytes = bytes.fromhex(hex_key.strip())
        if len(key_bytes) == 64:  # seed + public key form
            key_bytes = key_bytes[:32]
        if len(key_bytes) != 32:
            raise ValueError("ARCUS_API_PRIVATE_KEY must be a 32-byte Ed25519 private key in hex")
        return Ed25519PrivateKey.from_private_bytes(key_bytes)

    def _validate_config(self) -> None:
        """Validate Arcus configuration."""
        required_env_vars = ["ARCUS_WALLET_ADDRESS", "ARCUS_API_PRIVATE_KEY"]
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {missing_vars}")

    async def connect(self) -> None:
        """Connect to Arcus (HTTP session + market data WebSocket)."""
        if self.http_session is None:
            self.http_session = aiohttp.ClientSession()

        if self.market_id is None:
            await self.get_contract_attributes()

        if self.ws_manager is None:
            self.ws_manager = ArcusWebSocketManager(
                ws_url=self.ws_url,
                market_name=self.config.contract_id,
                account_address=self.wallet_address,
                account_index=self.account_index,
                logger=self.logger,
            )
            self.ws_manager.order_update_handler = self._handle_ws_order_update

        if self._ws_task is None or self._ws_task.done():
            self._ws_task = asyncio.create_task(self.ws_manager.start())
            await asyncio.sleep(2)

    async def disconnect(self) -> None:
        """Disconnect from Arcus."""
        try:
            if self.ws_manager:
                self.ws_manager.stop()
            if self._ws_task and not self._ws_task.done():
                self._ws_task.cancel()
                try:
                    await self._ws_task
                except asyncio.CancelledError:
                    pass
            if self.http_session:
                await self.http_session.close()
                self.http_session = None
        except Exception as e:
            self.logger.log(f"Error during Arcus disconnect: {e}", "ERROR")

    def get_exchange_name(self) -> str:
        """Get the exchange name."""
        return "arcus"

    def setup_order_update_handler(self, handler) -> None:
        """Setup order update handler for the WebSocket orders channel."""
        self._order_update_handler = handler

    def _handle_ws_order_update(self, contents: Dict[str, Any]) -> None:
        """Normalize a raw orders-channel update and forward it to the bot handler."""
        try:
            info = self._order_info_from_dict(contents)
            close_side = (getattr(self.config, "close_order_side", "") or "").lower()
            if self._order_update_handler:
                self._order_update_handler({
                    "order_id": info.order_id,
                    "side": info.side,
                    "order_type": "CLOSE" if info.side == close_side else "OPEN",
                    "status": info.status,
                    "size": str(info.size),
                    "price": str(info.price),
                    "contract_id": contents.get("marketDisplayName", ""),
                    "filled_size": str(info.filled_size),
                })
        except Exception as e:
            self.logger.log(f"Error handling WebSocket order update: {e}", "ERROR")

    # ------------------------------------------------------------------
    # Signing / HTTP plumbing
    # ------------------------------------------------------------------

    def _sign_message(self, message: bytes) -> str:
        return self._ed25519_key.sign(message).hex()

    def _auth_headers(self, timestamp_ns: int, signature: str) -> Dict[str, str]:
        return {
            "Content-Type": "application/json",
            "X-API-Key": self.api_key,
            "X-Timestamp": str(timestamp_ns),
            "X-Signature": signature,
        }

    async def _request(self, method: str, path: str,
                       params: Optional[Dict[str, Any]] = None,
                       body: Optional[Dict[str, Any]] = None,
                       headers: Optional[Dict[str, str]] = None) -> Any:
        if self.http_session is None:
            self.http_session = aiohttp.ClientSession()
        url = f"{self.base_url}{path}"
        async with self.http_session.request(
            method, url, params=params,
            data=canonical_json(body).encode() if body is not None else None,
            headers=headers,
        ) as resp:
            text = await resp.text()
            if resp.status not in (200, 201, 202):
                raise ArcusApiError(resp.status, text)
            return json.loads(text) if text else {}

    # ------------------------------------------------------------------
    # Rounding helpers
    # ------------------------------------------------------------------

    def _effective_tick(self, price: Decimal) -> Decimal:
        """Tick size for the price band containing `price` (tickTiers)."""
        for tier in self.tick_tiers:
            up_to = tier.get("upToPrice")
            if up_to is None or price <= Decimal(up_to):
                return Decimal(tier["tick"])
        return self.tick_size

    def round_to_tick(self, price) -> Decimal:
        """Round price to the applicable tick tier grid."""
        price = Decimal(price)
        tick = self._effective_tick(price)
        return (price / tick).to_integral_value(rounding=ROUND_HALF_UP) * tick

    def round_to_step(self, quantity) -> Decimal:
        """Round quantity down to a whole number of step sizes."""
        quantity = Decimal(quantity)
        if self.step_size > 0:
            return quantity.quantize(self.step_size, rounding=ROUND_DOWN)
        return quantity

    @staticmethod
    def _format_decimal(value: Decimal) -> str:
        """Plain (non-exponent) decimal string, e.g. Decimal('1E-8') -> '0.00000001'."""
        return format(Decimal(value).normalize(), "f")

    # ------------------------------------------------------------------
    # Trading
    # ------------------------------------------------------------------

    async def place_order(self, side: str, quantity: Decimal, price: Decimal,
                          order_type: str = "LIMIT", time_in_force: str = "ALO",
                          reduce_only: bool = False,
                          client_id: Optional[str] = None) -> Dict[str, Any]:
        """Place an order on Arcus. Returns the raw placeOrder response (orderId, status).

        Orders are asynchronous: a 202/ACK response means the order was accepted
        for processing; watch the orders channel / get_order_info for the outcome.
        """
        if self.market_id is None:
            await self.get_contract_attributes()

        assert self.wallet_address is not None and self.market_id is not None
        side = side.upper()
        price = self.round_to_tick(price)
        quantity = self.round_to_step(quantity)

        timestamp_ns = time.time_ns()
        good_til_us = time.time_ns() // 1000 + GOOD_TIL_DAYS * 86_400_000_000

        sign_payload = build_sign_payload(
            address=self.wallet_address,
            account_index=self.account_index,
            timestamp_ns=timestamp_ns,
            op=OP_PLACE,
            market_id=self.market_id,
            good_til_ns=good_til_us * 1000,
            price_ticks=to_integer_units(price, self.tick_size),
            size_quantums=to_integer_units(quantity, self.step_size),
            side=side,
            time_in_force=time_in_force,
            reduce_only=reduce_only,
            client_id=client_id,
        )
        signature = self._sign_message(sign_payload.encode())

        body: Dict[str, Any] = {
            "address": self.wallet_address,
            "accountIndex": self.account_index,
            "marketId": self.market_id,
            "orderSide": side,
            "orderType": order_type,
            "quantity": self._format_decimal(quantity),
            "price": self._format_decimal(price),
            "timeInForce": time_in_force,
            "goodTilTime": str(good_til_us),
            "timestamp": timestamp_ns,
        }
        if client_id:
            body["clientId"] = client_id
        if reduce_only:
            body["reduceOnly"] = True

        return await self._request(
            "POST", "/v1/placeOrder",
            params={"address": self.wallet_address},
            body=body,
            headers=self._auth_headers(timestamp_ns, signature),
        )

    async def _cancel_order_raw(self, order_id: str) -> Dict[str, Any]:
        if self.market_id is None:
            await self.get_contract_attributes()

        assert self.wallet_address is not None and self.market_id is not None
        timestamp_ns = time.time_ns()
        sign_payload = build_sign_payload(
            address=self.wallet_address,
            account_index=self.account_index,
            timestamp_ns=timestamp_ns,
            op=OP_CANCEL,
            market_id=self.market_id,
            order_id=str(order_id),
        )
        signature = self._sign_message(sign_payload.encode())

        body = {
            "address": self.wallet_address,
            "accountIndex": self.account_index,
            "marketId": self.market_id,
            "kind": "orderId",
            "orderId": str(order_id),
            "timestamp": timestamp_ns,
        }
        return await self._request(
            "POST", "/v1/cancelOrder",
            params={"address": self.wallet_address},
            body=body,
            headers=self._auth_headers(timestamp_ns, signature),
        )

    async def schedule_cancel(self, deadline_us: Optional[int] = None,
                              market_id: Optional[int] = None) -> Dict[str, Any]:
        """Arm/refresh (deadline_us set) or disarm (deadline_us None) the dead man's switch.

        Signed with the legacy scheme: ed25519(timestamp + "scheduleCancel" +
        canonicalJSON(body)). If the deadline passes without a refresh, Arcus
        cancels all open orders in scope - the safety net for this bot, since
        Arcus orders rest until goodTilTime (>= 1 month) otherwise.
        """
        if self.market_id is None:
            await self.get_contract_attributes()

        assert self.wallet_address is not None
        timestamp_ns = time.time_ns()

        body: Dict[str, Any] = {
            "address": self.wallet_address,
            "accountIndex": self.account_index,
        }
        if market_id is not None:
            body["marketId"] = market_id
        if deadline_us is not None:
            body["time"] = deadline_us

        message = f"{timestamp_ns}scheduleCancel{canonical_json(body)}"
        signature = self._sign_message(message.encode())

        return await self._request(
            "POST", "/v1/scheduleCancel",
            params={"address": self.wallet_address},
            body=body,
            headers=self._auth_headers(timestamp_ns, signature),
        )

    async def cancel_all_orders(self) -> Dict[str, Any]:
        """Cancel all open orders for the subaccount (market-scoped when marketId set).

        Signed with the legacy scheme, like scheduleCancel.
        """
        if self.market_id is None:
            await self.get_contract_attributes()

        assert self.wallet_address is not None and self.market_id is not None
        timestamp_ns = time.time_ns()

        body: Dict[str, Any] = {
            "address": self.wallet_address,
            "accountIndex": self.account_index,
            "marketId": self.market_id,
        }

        message = f"{timestamp_ns}cancelAllOrders{canonical_json(body)}"
        signature = self._sign_message(message.encode())

        return await self._request(
            "POST", "/v1/cancelAllOrders",
            params={"address": self.wallet_address},
            body=body,
            headers=self._auth_headers(timestamp_ns, signature),
        )

    async def place_open_order(self, contract_id: str, quantity: Decimal, direction: str) -> OrderResult:
        """Place a post-only (ALO) open order just inside the BBO."""
        max_retries = 30
        for _ in range(max_retries):
            best_bid, best_ask = await self.fetch_bbo_prices(contract_id)
            if best_bid <= 0 or best_ask <= 0:
                return OrderResult(success=False, error_message="Invalid bid/ask prices")

            if direction == "buy":
                order_price = best_ask - self.tick_size
                side = "BUY"
            else:
                order_price = best_bid + self.tick_size
                side = "SELL"

            client_id = f"bot{time.time_ns() // 1_000_000}"
            try:
                response = await self.place_order(
                    side, quantity, order_price,
                    time_in_force="ALO", client_id=client_id,
                )
            except ArcusApiError as e:
                self.logger.log(f"[OPEN] Order rejected: {e}", "WARNING")
                continue

            return OrderResult(
                success=True,
                order_id=response.get("orderId"),
                side=direction,
                size=quantity,
                price=order_price,
                status=response.get("status", "ACK"),
            )

        return OrderResult(success=False, error_message="Max retries exceeded")

    async def get_order_price(self, direction: str) -> Decimal:
        """Get the post-only price for an order one tick inside the BBO."""
        best_bid, best_ask = await self.fetch_bbo_prices(self.config.contract_id)
        if best_bid <= 0 or best_ask <= 0:
            raise ValueError("Invalid bid/ask prices")

        if direction == 'buy':
            order_price = best_ask - self.tick_size
        else:
            order_price = best_bid + self.tick_size

        return self.round_to_tick(order_price)

    async def place_market_order(self, contract_id: str, quantity: Decimal, direction: str) -> OrderResult:
        """Place a market (MARKET + IOC) order. The price field is a protective
        slippage bound that must be within 10% of the mark price."""
        mark_price = await self.fetch_mark_price(contract_id)
        if mark_price <= 0:
            return OrderResult(success=False, error_message="No mark price for slippage bound")

        side = "BUY" if direction == 'buy' else "SELL"
        # Cross the book: buy above mark, sell below mark (within the 10% bound)
        slippage = Decimal("1.05") if side == "BUY" else Decimal("0.95")
        price = self.round_to_tick(mark_price * slippage)

        client_id = f"bot{time.time_ns() // 1_000_000}"
        try:
            response = await self.place_order(
                side, quantity, price,
                order_type="MARKET", time_in_force="IOC", client_id=client_id,
            )
        except ArcusApiError as e:
            return OrderResult(success=False, error_message=str(e))

        return OrderResult(
            success=True,
            order_id=response.get("orderId"),
            side=direction,
            size=quantity,
            price=price,
            status=response.get("status", "ACK"),
        )

    async def place_close_order(self, contract_id: str, quantity: Decimal, price: Decimal, side: str) -> OrderResult:
        """Place a post-only (ALO) close order.

        Note: Arcus only allows reduce-only orders with IOC/FOK, so close orders
        are placed as plain limit orders; with an open position a fill reduces it.
        """
        best_bid, best_ask = await self.fetch_bbo_prices(contract_id)

        adjusted_price = price
        if side.lower() == "sell" and price <= best_bid:
            adjusted_price = best_bid + self.tick_size
        elif side.lower() == "buy" and price >= best_ask:
            adjusted_price = best_ask - self.tick_size

        client_id = f"bot{time.time_ns() // 1_000_000}"
        try:
            response = await self.place_order(
                side, quantity, adjusted_price,
                time_in_force="ALO", client_id=client_id,
            )
        except ArcusApiError as e:
            return OrderResult(success=False, error_message=str(e))

        return OrderResult(
            success=True,
            order_id=response.get("orderId"),
            side=side,
            size=quantity,
            price=adjusted_price,
            status=response.get("status", "ACK"),
        )

    async def cancel_order(self, order_id: str) -> OrderResult:
        """Cancel an order with Arcus."""
        try:
            await self._cancel_order_raw(order_id)
            return OrderResult(success=True, order_id=order_id)
        except ArcusApiError as e:
            return OrderResult(success=False, order_id=order_id, error_message=str(e))
        except Exception as e:
            return OrderResult(success=False, order_id=order_id, error_message=str(e))

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def _order_info_from_dict(self, data: Dict[str, Any]) -> OrderInfo:
        status = STATUS_MAP.get(str(data.get("status", "")), "OPEN")
        original = Decimal(str(data.get("originalSize", "0") or "0"))
        remaining = Decimal(str(data.get("remainingSize", "0") or "0"))
        return OrderInfo(
            order_id=str(data.get("orderId", "")),
            side=str(data.get("side", "")).lower(),
            size=original,
            price=Decimal(str(data.get("price", "0") or "0")),
            status=status,
            filled_size=max(original - remaining, Decimal("0")),
            remaining_size=remaining,
        )

    async def get_order_info(self, order_id: str) -> Optional[OrderInfo]:
        """Get order information; prefers the live WebSocket cache, falls back to REST.

        Returns None when the order is not (yet) visible - placement is async, so a
        fresh order may 404 briefly.
        """
        if self.ws_manager:
            cached = self.ws_manager.get_cached_order(order_id)
            if cached:
                return self._order_info_from_dict(cached)
        try:
            data = await self._request(
                "GET", f"/v1/order/{order_id}",
                params={"address": self.wallet_address},
            )
        except ArcusApiError as e:
            if e.status == 404:
                return None
            raise
        return self._order_info_from_dict(data)

    @query_retry(default_return=[])
    async def get_active_orders(self, contract_id: str) -> List[OrderInfo]:
        """Get open orders for a market."""
        data = await self._request(
            "GET", "/v1/openOrders",
            params={"address": self.wallet_address, "market": contract_id},
        )
        return [self._order_info_from_dict(o) for o in data.get("orders", [])]

    @query_retry(default_return=Decimal("0"))
    async def get_account_positions(self) -> Decimal:
        """Get the signed position size for the configured market (positive long)."""
        params: Dict[str, Any] = {"address": self.wallet_address}
        if self.config.contract_id:
            params["market"] = self.config.contract_id
        data = await self._request("GET", "/v1/positions", params=params)
        positions = data.get("positions", {})
        if not positions:
            return Decimal("0")
        if self.market_id is not None and str(self.market_id) in positions:
            return Decimal(str(positions[str(self.market_id)].get("size", "0")))
        contract = (self.config.contract_id or "").upper()
        for position in positions.values():
            if str(position.get("marketDisplayName", "")).upper() == contract:
                return Decimal(str(position.get("size", "0")))
        return Decimal("0")

    @query_retry(default_return=(Decimal("0"), Decimal("0")))
    async def fetch_bbo_prices(self, contract_id: str) -> Tuple[Decimal, Decimal]:
        """Get best bid/ask from the WebSocket book cache or the REST BBO endpoint."""
        if self.ws_manager and self.ws_manager.best_bid > 0 and self.ws_manager.best_ask > 0:
            return self.ws_manager.best_bid, self.ws_manager.best_ask
        data = await self._request("GET", f"/v1/bbo/{contract_id}")
        best_bid = data.get("bestBid") or {}
        best_ask = data.get("bestAsk") or {}
        return Decimal(str(best_bid.get("price", "0"))), Decimal(str(best_ask.get("price", "0")))

    @query_retry(default_return=Decimal("0"))
    async def fetch_mark_price(self, contract_id: str) -> Decimal:
        """Get the mark price from the WebSocket markets cache or REST."""
        if self.ws_manager and self.ws_manager.mark_price > 0:
            return self.ws_manager.mark_price
        data = await self._request("GET", "/v1/markets", params={"market": contract_id})
        markets = data.get("markets", [])
        if not markets:
            return Decimal("0")
        return Decimal(str(markets[0].get("markPrice", "0") or "0"))

    @query_retry()
    async def get_contract_attributes(self) -> Tuple[str, Decimal]:
        """Resolve the ticker to a market and load tick/step sizes."""
        ticker = self.config.ticker
        market_name = ticker if "-" in ticker else f"{ticker}-USD"

        data = await self._request("GET", "/v1/markets", params={"market": market_name})
        markets = data.get("markets", [])
        if not markets:
            raise ValueError(f"Market not found for ticker: {ticker} ({market_name})")
        market = markets[0]
        if market.get("status") != "ONLINE":
            raise ValueError(f"Market {market_name} is not ONLINE: {market.get('status')}")

        self.market_id = int(market["marketId"])
        self.tick_size = Decimal(str(market["tickSize"]))
        self.step_size = Decimal(str(market["stepSize"]))
        self.tick_tiers = market.get("tickTiers") or []
        self.min_order_size = Decimal(str(market.get("minOrderSize", "0") or "0"))
        self.min_order_notional = Decimal(str(market.get("minOrderNotional", "0") or "0"))

        contract_id = market["marketDisplayName"]
        self.config.contract_id = contract_id
        self.config.tick_size = self.tick_size

        # Validate the configured order size against market limits
        quantity = getattr(self.config, "quantity", None)
        if quantity is not None:
            if quantity < self.min_order_size:
                raise ValueError(
                    f"Order quantity {quantity} below min order size {self.min_order_size}")
            mark = Decimal(str(market.get("markPrice", "0") or "0"))
            if mark > 0 and quantity * mark < self.min_order_notional:
                raise ValueError(
                    f"Order notional {quantity * mark} below min order notional "
                    f"{self.min_order_notional}")

        return contract_id, self.tick_size
