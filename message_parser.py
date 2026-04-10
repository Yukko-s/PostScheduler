import re
from calendar import monthrange
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

JST = ZoneInfo("Asia/Tokyo")

WEEKDAY_MAP = {
    "月": 0, "月曜": 0, "月曜日": 0,
    "火": 1, "火曜": 1, "火曜日": 1,
    "水": 2, "水曜": 2, "水曜日": 2,
    "木": 3, "木曜": 3, "木曜日": 3,
    "金": 4, "金曜": 4, "金曜日": 4,
    "土": 5, "土曜": 5, "土曜日": 5,
    "日": 6, "日曜": 6, "日曜日": 6,
}


@dataclass
class ParsedSchedule:
    scheduled_at: datetime
    target_channel_id: int
    repeat: Optional[str]  # None / "毎月25日" / "毎週月曜"
    body: str


def parse(text: str) -> tuple[Optional[ParsedSchedule], Optional[str]]:
    """
    メッセージをパースする。
    成功: (ParsedSchedule, None)
    失敗: (None, エラーメッセージ)
    """
    if "---" not in text:
        return None, "区切り線 `---` が見つかりません"

    header, _, body = text.partition("---")
    body = body.strip()

    # ヘッダーをキーバリューでパース
    data = {}
    for line in header.strip().splitlines():
        if ":" in line:
            key, _, val = line.partition(":")
            data[key.strip()] = val.strip()

    # 日時
    raw_dt = data.get("日時")
    if not raw_dt:
        return None, "`日時:` が見つかりません\n例: `日時: 2026-04-05 20:00`"
    try:
        fmt = "%Y-%m-%d %H:%M:%S" if raw_dt.count(":") == 2 else "%Y-%m-%d %H:%M"
        dt = datetime.strptime(raw_dt, fmt).replace(tzinfo=JST)
    except ValueError:
        return None, "`日時` の形式が正しくありません\n例: `日時: 2026-04-05 20:00` または `日時: 2026-04-05 20:00:30`"

    # 投稿先（チャンネルメンション <#123> または数字IDを受け付ける）
    raw_ch = data.get("投稿先")
    if not raw_ch:
        return None, "`投稿先:` が見つかりません\n例: `投稿先: <#チャンネルID>`"

    m = re.search(r"<#(\d+)>", raw_ch)
    if m:
        target_channel_id = int(m.group(1))
    else:
        m2 = re.fullmatch(r"\s*(\d+)\s*", raw_ch)
        if m2:
            target_channel_id = int(m2.group(1))
        else:
            return None, "`投稿先` にチャンネルをメンション（<#チャンネルID>）で指定してください"

    # 繰り返し
    raw_repeat = data.get("繰り返し", "なし").strip()
    if raw_repeat == "なし":
        repeat = None
    else:
        monthly_m = re.fullmatch(r"毎月(\d+)日", raw_repeat)
        weekly_m = re.fullmatch(r"毎週(.+)", raw_repeat)
        if monthly_m:
            day = int(monthly_m.group(1))
            if not 1 <= day <= 31:
                return None, f"繰り返しの日付が正しくありません: {day}"
        elif weekly_m:
            weekday_str = weekly_m.group(1).strip()
            if weekday_str not in WEEKDAY_MAP:
                valid = "月・火・水・木・金・土・日（または月曜〜日曜）"
                return None, f"曜日が正しくありません: `{weekday_str}`\n使用可能: {valid}"
        else:
            return None, "繰り返しの形式が正しくありません\n例: `毎月25日` または `毎週月曜`"
        repeat = raw_repeat

    return ParsedSchedule(
        scheduled_at=dt,
        target_channel_id=target_channel_id,
        repeat=repeat,
        body=body,
    ), None


def calc_next_dt(parsed: ParsedSchedule, after: datetime) -> Optional[datetime]:
    """繰り返しジョブの次回実行日時を計算する。"""
    if not parsed.repeat:
        return None

    hour = parsed.scheduled_at.hour
    minute = parsed.scheduled_at.minute
    second = parsed.scheduled_at.second

    monthly_m = re.fullmatch(r"毎月(\d+)日", parsed.repeat)
    if monthly_m:
        day = int(monthly_m.group(1))
        return _next_monthly(day, hour, minute, second, after)

    weekly_m = re.fullmatch(r"毎週(.+)", parsed.repeat)
    if weekly_m:
        weekday = WEEKDAY_MAP[weekly_m.group(1).strip()]
        return _next_weekly(weekday, hour, minute, second, after)

    return None


def _next_monthly(day: int, hour: int, minute: int, second: int, after: datetime) -> datetime:
    """毎月N日の次回日時を返す。存在しない日は末日にフォールバック。"""
    year, month = after.year, after.month

    candidate = _make_monthly_dt(year, month, day, hour, minute, second, after.tzinfo)
    if candidate <= after:
        month += 1
        if month > 12:
            month = 1
            year += 1
        candidate = _make_monthly_dt(year, month, day, hour, minute, second, after.tzinfo)

    return candidate


def _make_monthly_dt(year: int, month: int, day: int, hour: int, minute: int, second: int, tz) -> datetime:
    """指定月の指定日（存在しない場合は末日にフォールバック）のdatetimeを返す。"""
    max_day = monthrange(year, month)[1]
    actual_day = min(day, max_day)
    return datetime(year, month, actual_day, hour, minute, second, tzinfo=tz)


def _next_weekly(weekday: int, hour: int, minute: int, second: int, after: datetime) -> datetime:
    """毎週X曜日の次回日時を返す。"""
    days_ahead = weekday - after.weekday()
    if days_ahead < 0 or (
        days_ahead == 0 and (after.hour, after.minute, after.second) >= (hour, minute, second)
    ):
        days_ahead += 7

    next_date = after + timedelta(days=days_ahead)
    return next_date.replace(hour=hour, minute=minute, second=second, microsecond=0)
