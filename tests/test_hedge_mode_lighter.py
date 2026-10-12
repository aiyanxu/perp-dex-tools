"""Unit tests for the Lighter mainnet(maker) + lighter-rh(taker) hedge bot."""

import os
from decimal import Decimal

import pytest

os.environ.setdefault("API_KEY_PRIVATE_KEY", "0x" + "12" * 32)

from hedge.hedge_mode_lighter import HedgeBot


def _make_bot(**kwargs):
    args: dict = {
        "ticker": "BTC",
        "order_quantity": Decimal("0.001"),
        "iterations": 1,
        "direction": "buy",
    }
    args.update(kwargs)
    return HedgeBot(
        ticker=args["ticker"],
        order_quantity=args["order_quantity"],
        iterations=args["iterations"],
        direction=args["direction"],
    )


# ----------------------------------------------------------------------
# Construction / deployment config resolution
# ----------------------------------------------------------------------

def test_makers_and_taker_target_separate_deployments():
    bot = _make_bot()
    assert bot.maker_base_url == "https://mainnet.zklighter.elliot.ai"
    assert bot.taker_base_url == "https://api.rh.lighter.xyz"
    assert bot.maker_account_index == bot.taker_account_index  # shared fallback


def test_deployment_specific_env_precedence(monkeypatch):
    monkeypatch.setenv("LIGHTER_MAINNET_ACCOUNT_INDEX", "111")
    monkeypatch.setenv("LIGHTER_RH_ACCOUNT_INDEX", "222")
    monkeypatch.setenv("LIGHTER_MAINNET_API_KEY_PRIVATE_KEY", "0x" + "aa" * 32)
    monkeypatch.setenv("LIGHTER_RH_API_KEY_PRIVATE_KEY", "0x" + "bb" * 32)
    bot = _make_bot()
    assert bot.maker_account_index == 111
    assert bot.taker_account_index == 222
    assert bot._read_deployment_key("mainnet") == "0x" + "aa" * 32
    assert bot._read_deployment_key("rh") == "0x" + "bb" * 32


def test_shared_key_fallback(monkeypatch):
    monkeypatch.delenv("LIGHTER_MAINNET_API_KEY_PRIVATE_KEY", raising=False)
    monkeypatch.delenv("LIGHTER_RH_API_KEY_PRIVATE_KEY", raising=False)
    monkeypatch.setenv("API_KEY_PRIVATE_KEY", "0x" + "cc" * 32)
    bot = _make_bot()
    assert bot._read_deployment_key("mainnet") == "0x" + "cc" * 32
    assert bot._read_deployment_key("rh") == "0x" + "cc" * 32


def test_missing_key_raises(monkeypatch):
    monkeypatch.delenv("LIGHTER_MAINNET_API_KEY_PRIVATE_KEY", raising=False)
    monkeypatch.delenv("LIGHTER_RH_API_KEY_PRIVATE_KEY", raising=False)
    monkeypatch.delenv("API_KEY_PRIVATE_KEY", raising=False)
    bot = _make_bot()
    with pytest.raises(Exception, match="mainnet"):
        bot._read_deployment_key("mainnet")


# ----------------------------------------------------------------------
# Order book maintenance
# ----------------------------------------------------------------------

def test_update_order_book_sets_and_removes_levels():
    bot = _make_bot()
    bot.update_order_book(bot.maker_order_book, "bids", [["100.5", "2"], ["100.4", "1"]])
    bot.update_order_book(bot.maker_order_book, "asks", [["100.7", "3"]])
    assert bot.maker_order_book["bids"][Decimal("100.5")] == Decimal(2)
    assert bot.maker_order_book["asks"][Decimal("100.7")] == Decimal(3)

    # size 0 removes the level
    bot.update_order_book(bot.maker_order_book, "bids", [["100.4", "0"]])
    assert Decimal("100.4") not in bot.maker_order_book["bids"]


def test_get_best_levels_picks_best_prices():
    bot = _make_bot()
    bot.update_order_book(bot.taker_order_book, "bids", [["100.1", "1"], ["100.5", "2"], ["100.3", "4"]])
    bot.update_order_book(bot.taker_order_book, "asks", [["101.0", "5"], ["100.9", "6"]])
    best_bid, best_ask = bot.get_best_levels(bot.taker_order_book)
    assert best_bid == (Decimal("100.5"), Decimal(2))
    assert best_ask == (Decimal("100.9"), Decimal(6))


def test_get_best_levels_empty_book():
    bot = _make_bot()
    assert bot.get_best_levels(bot.maker_order_book) == (None, None)


# ----------------------------------------------------------------------
# Fill handling
# ----------------------------------------------------------------------

def _fill_event(bot, client_order_index):
    import asyncio
    event = asyncio.Event()
    bot.maker_fill_events[client_order_index] = event
    return event


def test_handle_order_result_updates_maker_long_position():
    bot = _make_bot()
    event = _fill_event(bot, 123)
    bot.handle_order_result(
        {"filled_base_amount": "0.001", "filled_quote_amount": "0.1005",
         "is_ask": False, "client_order_index": 123}, "mainnet")
    assert bot.maker_position == Decimal("0.001")
    assert bot.maker_fill_amounts[123] == Decimal("0.001")
    assert bot.maker_fill_prices[123] == Decimal("100.5")
    assert event.is_set()


def test_handle_order_result_updates_maker_short_position():
    bot = _make_bot()
    event = _fill_event(bot, 456)
    bot.handle_order_result(
        {"filled_base_amount": "0.002", "filled_quote_amount": "0.2010",
         "is_ask": True, "client_order_index": 456}, "mainnet")
    assert bot.maker_position == Decimal("-0.002")
    assert event.is_set()


def test_handle_order_result_updates_taker_position():
    bot = _make_bot()
    import asyncio
    event = asyncio.Event()
    bot.taker_fill_events[789] = event
    bot.handle_order_result(
        {"filled_base_amount": "0.003", "filled_quote_amount": "0.3018",
         "is_ask": True, "client_order_index": 789}, "rh")
    assert bot.taker_position == Decimal("-0.003")
    assert event.is_set()


def test_handle_order_result_zero_fill_keeps_position_flat():
    bot = _make_bot()
    _fill_event(bot, 1)
    bot.handle_order_result(
        {"filled_base_amount": "0", "filled_quote_amount": "0",
         "is_ask": False, "client_order_index": 1}, "mainnet")
    assert bot.maker_position == Decimal(0)
    assert bot.maker_fill_amounts.get(1, Decimal(0)) == Decimal(0)


def test_handle_order_result_counts_partial_fills_as_deltas():
    bot = _make_bot()
    event = _fill_event(bot, 42)
    # partial fill: 0.0004 of 0.001
    bot.handle_order_result(
        {"filled_base_amount": "0.0004", "filled_quote_amount": "0.0402",
         "is_ask": False, "client_order_index": 42}, "mainnet")
    assert bot.maker_position == Decimal("0.0004")
    assert bot.maker_fill_amounts[42] == Decimal("0.0004")
    # rest fills: cumulative totals arrive
    bot.handle_order_result(
        {"filled_base_amount": "0.001", "filled_quote_amount": "0.1006",
         "is_ask": False, "client_order_index": 42}, "mainnet")
    assert bot.maker_position == Decimal("0.001")
    assert bot.maker_fill_amounts[42] == Decimal("0.001")
    # duplicate of the same cumulative totals is ignored
    bot.handle_order_result(
        {"filled_base_amount": "0.001", "filled_quote_amount": "0.1006",
         "is_ask": False, "client_order_index": 42}, "mainnet")
    assert bot.maker_position == Decimal("0.001")
    assert bot.maker_fill_amounts[42] == Decimal("0.001")
    assert event.is_set()


def test_handle_order_result_wakes_waiter_on_zero_fill_cancel():
    """A plain cancel (no fill) must still wake the maker waiter."""
    bot = _make_bot()
    event = _fill_event(bot, 7)
    bot.handle_order_result(
        {"filled_base_amount": "0", "filled_quote_amount": "0",
         "is_ask": True, "client_order_index": 7, "status": "canceled"},
        "mainnet")
    assert bot.maker_position == Decimal(0)
    assert event.is_set()


# ----------------------------------------------------------------------
# Pricing
# ----------------------------------------------------------------------

def test_maker_offset_default():
    bot = _make_bot()
    assert bot.MAKER_OFFSET_BPS == Decimal("0.0003")  # 3 bps


def test_round_to_tick_without_tick_size_returns_price():
    bot = _make_bot()
    assert bot.round_to_tick(Decimal("100.456")) == Decimal("100.456")


def test_round_to_tick_with_tick_size():
    bot = _make_bot()
    bot.maker_tick_size = Decimal("0.1")
    assert bot.round_to_tick(Decimal("100.46")) == Decimal("100.5")
    assert bot.round_to_tick(Decimal("100.44")) == Decimal("100.4")
