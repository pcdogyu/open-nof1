"""
Background scheduler for refreshing analytics and recording equity curves.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

try:
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.triggers.interval import IntervalTrigger
except ImportError:  # pragma: no cover
    AsyncIOScheduler = None  # type: ignore
    IntervalTrigger = None  # type: ignore

from accounts.repository import AccountRepository
from services.analytics.leaderboard import refresh_leaderboard_cache
from services.webapp.dependencies import get_account_repository

logger = logging.getLogger(__name__)

_SCHEDULER: Optional["AsyncIOScheduler"] = None


def start_scheduler() -> None:
    """Start the APScheduler background jobs if the dependency is available."""
    global _SCHEDULER
    if AsyncIOScheduler is None:
        logger.warning("APScheduler not installed; background analytics disabled.")
        return
    if _SCHEDULER is not None:
        return

    scheduler = AsyncIOScheduler(timezone=timezone.utc)
    scheduler.add_job(
        _refresh_metrics_job,
        trigger=IntervalTrigger(seconds=60),
        id="refresh_leaderboard_and_equity_curve",
        replace_existing=True,
    )
    scheduler.start()
    _SCHEDULER = scheduler
    logger.info("Background scheduler started for analytics refresh.")


def shutdown_scheduler() -> None:
    """Stop the scheduler when the application shuts down."""
    global _SCHEDULER
    if _SCHEDULER is not None:
        _SCHEDULER.shutdown(wait=False)
        _SCHEDULER = None


def _refresh_metrics_job() -> None:
    """Refresh leaderboard cache and append equity points for each account."""
    repository = get_account_repository()
    _refresh_leaderboard(repository)
    _record_equity_points(repository)


def _refresh_leaderboard(repository: AccountRepository) -> None:
    snapshot = refresh_leaderboard_cache(repository)
    logger.debug("Leaderboard refreshed with %d entries at %s", len(snapshot.get("leaders", [])), snapshot.get("as_of"))


def _record_equity_points(repository: AccountRepository) -> None:
    accounts = repository.list_accounts()
    for account in accounts:
        repository.record_equity_point(account)
    if accounts:
        logger.debug(
            "Recorded equity points for %d accounts at %s",
            len(accounts),
            datetime.now(tz=timezone.utc).isoformat(),
        )
