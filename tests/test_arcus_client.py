"""Unit tests for the Arcus exchange client signing and rounding logic."""

import os
from decimal import Decimal

import pytest

os.environ.setdefault("ARCUS_WALLET_ADDRESS", "0x" + "12" * 20)
os.environ.setdefault("ARCUS_API_PRIVATE_KEY", "9d" * 32)

from exchanges.arcus import (
    ArcusClient,
    build_sign_payload,
    canonical_json,
    to_integer_units,
    OP_PLACE,
    OP_CANCEL,
)


class FakeConfig:
    """Duck-typed config matching what the hedge bot passes in."""

    ticker = "BTC"
    contract_id = ""
    quantity = Decimal("0.001")
    tick_size = Decimal("0.1")
    close_order_side = "sell"


def _make_client():
    return ArcusClient(FakeConfig())  # type: ignore[arg-type]


# ----------------------------------------------------------------------
# canonical_json / to_integer_units
# ----------------------------------------------------------------------

def test_canonical_json_is_compact_and_sorted():
    assert canonical_json({"b": 1, "a": 2}) == '{"a":2,"b":1}'


def test_to_integer_units_exact():
    assert to_integer_units(Decimal("83000.0"), Decimal("0.1")) == 830000
    assert to_integer_units(Decimal("0.001"), Decimal("0.0001")) == 10


def test_to_integer_units_rejects_remainder():
    with pytest.raises(ValueError):
        to_integer_units(Decimal("83000.05"), Decimal("0.1"))


# ----------------------------------------------------------------------
# build_sign_payload (typed canonical payloads signed by Ed25519)
# ----------------------------------------------------------------------

def test_place_payload_matches_documented_template():
    payload = build_sign_payload(
        address="0xAbCdEf0123456789AbCdEf0123456789AbCdEf01",
        account_index=0,
        timestamp_ns=1_700_000_000_000_000_000,
        op=OP_PLACE,
        market_id=1,
        good_til_ns=1_734_560_000_000_000_000,
        price_ticks=830000,
        size_quantums=1000,
        side="BUY",
        time_in_force="GTT",
        reduce_only=False,
    )
    # Keys sorted alphabetically, no whitespace, address lowercased,
    # c omitted when empty (documented canonical template).
    assert payload == (
        '{"ad":"0xabcdef0123456789abcdef0123456789abcdef01","ai":0,'
        '"ct":1700000000000000000,"g":1734560000000000000,"m":1,"op":1,'
        '"p":830000,"q":1000,"r":0,"s":0,"t":0,"v":1}'
    )


def test_place_payload_sell_alo_reduce_only_and_client_id():
    payload = build_sign_payload(
        address="0xAbCdEf0123456789AbCdEf0123456789AbCdEf01",
        account_index=2,
        timestamp_ns=123,
        op=OP_PLACE,
        market_id=3,
        good_til_ns=456,
        price_ticks=7,
        size_quantums=8,
        side="SELL",
        time_in_force="ALO",
        reduce_only=True,
        client_id="order-42",
    )
    assert '"c":"order-42"' in payload
    assert '"s":1' in payload
    assert '"t":3' in payload
    assert '"r":1' in payload
    assert '"ai":2' in payload


def test_cancel_payload_has_no_order_fields():
    payload = build_sign_payload(
        address="0xabcdef0123456789abcdef0123456789abcdef01",
        account_index=0,
        timestamp_ns=123,
        op=OP_CANCEL,
        market_id=1,
        order_id="ord-1",
    )
    assert payload == (
        '{"ad":"0xabcdef0123456789abcdef0123456789abcdef01","ai":0,'
        '"ct":123,"id":"ord-1","m":1,"op":2,"v":1}'
    )
    # cancel payload must not carry place-only fields
    for key in ('"p":', '"q":', '"s":', '"t":', '"r":', '"g":'):
        assert key not in payload


def test_cancel_by_client_id_payload():
    payload = build_sign_payload(
        address="0xabcdef0123456789abcdef0123456789abcdef01",
        account_index=0,
        timestamp_ns=123,
        op=OP_CANCEL,
        market_id=1,
        client_id="bot42",
    )
    assert payload == (
        '{"ad":"0xabcdef0123456789abcdef0123456789abcdef01","ai":0,'
        '"c":"bot42","ct":123,"m":1,"op":2,"v":1}'
    )


# ----------------------------------------------------------------------
# Ed25519 signature sanity
# ----------------------------------------------------------------------

def test_signature_is_128_hex_chars_over_payload():
    client = _make_client()
    sig = client._sign_message(b"payload-bytes")
    assert len(sig) == 128
    int(sig, 16)  # must be valid hex


def test_api_key_is_64_hex_chars():
    client = _make_client()
    assert len(client.api_key) == 64
    int(client.api_key, 16)


# ----------------------------------------------------------------------
# Legacy-message signing (scheduleCancel / cancelAllOrders)
# ----------------------------------------------------------------------

def test_legacy_sign_message_format(monkeypatch):
    """The legacy scheme signs timestamp + action + canonicalJSON(body) verbatim."""
    client = _make_client()
    client.market_id = 1
    body = {"address": client.wallet_address, "accountIndex": 0, "marketId": 1}

    captured = {}

    def fake_sign(message: bytes) -> str:
        captured["message"] = message.decode()
        return "ab" * 64

    monkeypatch.setattr(client, "_sign_message", fake_sign)

    async def fake_request(*args, **kwargs):
        return {}

    monkeypatch.setattr(client, "_request", fake_request)

    import asyncio
    from exchanges.arcus import canonical_json
    asyncio.run(client.schedule_cancel(deadline_us=1_700_000_000_000_000, market_id=1))

    expected_body = {"address": client.wallet_address, "accountIndex": 0,
                     "marketId": 1, "time": 1_700_000_000_000_000}
    assert captured["message"].endswith(f"scheduleCancel{canonical_json(expected_body)}")
    # message starts with the nanosecond timestamp digits
    assert captured["message"].split("scheduleCancel")[0].isdigit()

    # disarm: no time key in the signed body
    asyncio.run(client.schedule_cancel())
    assert '"time"' not in captured["message"]


# ----------------------------------------------------------------------
# Tick tier rounding
# ----------------------------------------------------------------------

def test_round_to_tick_uses_tier_for_high_prices():
    client = _make_client()
    # BTC-USD-like tiers: 0.1 up to 500k, then 0.2
    client.tick_tiers = [
        {"upToPrice": "500000", "tick": "0.1"},
        {"upToPrice": "1000000", "tick": "0.2"},
        {"tick": "0.5"},
    ]
    assert client.round_to_tick(Decimal("83000.04")) == Decimal("83000.0")
    assert client.round_to_tick(Decimal("600000.11")) == Decimal("600000.2")
    assert client.round_to_tick(Decimal("600000.09")) == Decimal("600000.0")
    assert client.round_to_tick(Decimal("1500000.1")) == Decimal("1500000.0")
    assert client.round_to_tick(Decimal("1500000.26")) == Decimal("1500000.5")


def test_round_to_tick_without_tiers_falls_back_to_tick_size():
    client = _make_client()
    client.tick_tiers = []
    client.tick_size = Decimal("0.1")
    assert client.round_to_tick(Decimal("100.04")) == Decimal("100.0")


def test_round_to_step_rounds_down():
    client = _make_client()
    client.step_size = Decimal("0.00000001")
    assert client.round_to_step(Decimal("0.000999999")) == Decimal("0.00099999")


def test_format_decimal_plain_notation():
    client = _make_client()
    assert client._format_decimal(Decimal("1E-8")) == "0.00000001"
    assert client._format_decimal(Decimal("83000.0")) == "83000"
    assert "e" not in client._format_decimal(Decimal("0.001")).lower()
