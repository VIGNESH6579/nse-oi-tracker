from database.repository import SignalRepository


def _save_closed_trade(repository, *, symbol, entry_time, entry_price, exit_price, quality, reason="MOMENTUM_WEAK"):
    event_id = repository.save_scalp_entry({
        "trade_date": "2026-10-07",
        "mode": "scalp_chain",
        "symbol": symbol,
        "direction": "BUY",
        "strike": 100.0,
        "option_type": "CE",
        "expiry": "27OCT2026",
        "entry_time_ist": entry_time,
        "underlying_entry": 100.0,
        "entry_option_ltp": entry_price,
        "rule_ids": ["QUALITY_V2"],
        "chain_snapshot_id": f"sample-{symbol}",
        "signal_quality": quality,
    })
    repository.close_scalp(
        event_id,
        exit_time_ist="2026-10-07T10:10:00+05:30",
        underlying_exit=101.0,
        exit_option_ltp=exit_price,
        hold_minutes=10.0,
        exit_reason=reason,
        payload={"pnl_status": "QUOTED_PAPER_EXIT", "pnl_is_realized": False},
    )


def test_rolling_quality_summary_compares_winners_and_losers_without_daywise_archive(tmp_path):
    repository = SignalRepository(tmp_path / "quality.sqlite3")
    _save_closed_trade(
        repository,
        symbol="WINNER",
        entry_time="2026-10-07T10:00:00+05:30",
        entry_price=10.0,
        exit_price=12.0,
        quality={
            "underlying": {"relative_volume": 1.5, "momentum_pct": 0.55},
            "option": {"premium_change_pct": 1.2, "spread_pct": 0.8},
        },
    )
    _save_closed_trade(
        repository,
        symbol="LOSER",
        entry_time="2026-10-07T10:01:00+05:30",
        entry_price=10.0,
        exit_price=9.0,
        quality={
            "underlying": {"relative_volume": 0.9, "momentum_pct": 0.31},
            "option": {"premium_change_pct": 0.3, "spread_pct": 2.5},
        },
    )

    summary = repository.scalp_quality_summary()

    assert summary["sample_size"] == 2
    assert summary["wins"] == 1 and summary["losses"] == 1
    assert summary["win_rate_pct_ex_flat"] == 50.0
    assert summary["winner_avg_relative_volume"] == 1.5
    assert summary["loser_avg_relative_volume"] == 0.9
    assert summary["winner_avg_spread_pct"] == 0.8
    assert summary["loser_avg_spread_pct"] == 2.5
    assert summary["pnl_basis"] == "quoted_option_premium_before_costs_not_realized_pnl"

    # The visible day-history cleanup may delete old detailed events, but the
    # compact 100-trade rolling sample survives for signal-quality evaluation.
    repository.scalp_events_for_date("2026-10-08")
    assert repository.scalp_quality_summary()["sample_size"] == 2
