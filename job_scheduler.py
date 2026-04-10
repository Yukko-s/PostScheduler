from datetime import datetime
from typing import Callable

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger
from zoneinfo import ZoneInfo

JST = ZoneInfo("Asia/Tokyo")
_scheduler = AsyncIOScheduler(timezone=JST)


def start():
    _scheduler.start()


def add_job(job_id: str, func: Callable, run_date: datetime, args: list = None):
    """指定日時にfuncを実行するジョブを登録する。同じIDが存在する場合は上書き。"""
    _scheduler.add_job(
        func,
        trigger=DateTrigger(run_date=run_date, timezone=JST),
        id=job_id,
        args=args or [],
        replace_existing=True,
        misfire_grace_time=300,  # 5分以内の遅延は許容
    )


def remove_job(job_id: str):
    """ジョブを削除する。存在しない場合は何もしない。"""
    try:
        _scheduler.remove_job(job_id)
    except Exception:
        pass


def has_job(job_id: str) -> bool:
    return _scheduler.get_job(job_id) is not None
