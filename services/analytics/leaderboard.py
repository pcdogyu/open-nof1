"""
Leaderboard and asset analytics helpers for the dashboard.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List

from accounts.models import Account
from accounts.repository import AccountRepository

LEADERBOARD_CACHE: Dict[str, Any] = {"as_of": None, "leaders": []}


def refresh_leaderboard_cache(repository: AccountRepository) -> Dict[str, Any]:
    """Recompute leaderboard rankings and persist to the in-memory cache."""
    accounts = repository.list_accounts()
    leaders = _rank_accounts(accounts)
    payload = {
        "as_of": datetime.now(tz=timezone.utc).isoformat(),
        "leaders": leaders,
    }
    LEADERBOARD_CACHE.update(payload)
    return LEADERBOARD_CACHE


def get_leaderboard(repository: AccountRepository | None = None) -> Dict[str, Any]:
    """
    Return the cached leaderboard snapshot; recompute when cache is empty
    and a repository instance is supplied.
    """
    if (not LEADERBOARD_CACHE.get("leaders")) and repository is not None:
        return refresh_leaderboard_cache(repository)
    return LEADERBOARD_CACHE


def _rank_accounts(accounts: List[Account]) -> List[Dict[str, Any]]:
    ranked = sorted(accounts, key=lambda account: account.equity, reverse=True)
    leaders: List[Dict[str, Any]] = []
    for account in ranked:
        leaders.append(
            {
                "account_id": account.account_id,
                "model_id": account.model_id,
                "equity": account.equity,
                "pnl": account.pnl,
                "starting_equity": account.starting_equity,
                "return_pct": _compute_return_pct(account),
            }
        )
    return leaders


def _compute_return_pct(account: Account) -> float:
    if account.starting_equity <= 0:
        return 0.0
    return round(((account.equity - account.starting_equity) / account.starting_equity) * 100, 2)
