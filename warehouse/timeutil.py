"""时间瞬间解析与营业日换算。

营业日按门店本地日历解释，所有跨天运算使用日历对象推进，
不通过固定 86400 秒推算，以正确覆盖夏令时切换（23/24/25 小时）。
"""
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

# 输入解析后的三类结果。
OK = 'ok'            # 可唯一确定一个瞬间
GAP = 'gap'          # 本地不存在的墙钟时间（春季跳时），禁止自行顺延
AMBIGUOUS = 'ambiguous'  # 回拨夜晚重复出现的本地时间，无偏移无法唯一确定瞬间


@dataclass(frozen=True)
class ResolvedInstant:
    status: str
    instant: datetime | None = None      # 带 UTC 时区的 datetime
    raw: str | None = None               # 原始输入文本
    reason: str | None = None


def parse_instant(text):
    """解析输入时间。

    带 UTC 或固定偏移的输入可唯一确定瞬间，直接归一到 UTC。
    无偏移输入不在此处猜测，交由门店时区做本地墙钟分类。
    """
    if not isinstance(text, str) or not text:
        return ResolvedInstant(status=GAP, raw=text, reason='时间文本为空')
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        return ResolvedInstant(status=GAP, raw=text, reason=f'时间无法解析: {exc}')
    if parsed.tzinfo is None:
        return ResolvedInstant(status='naive', raw=text, reason='缺少偏移，需按门店时区解释')
    return ResolvedInstant(status=OK, instant=parsed.astimezone(timezone.utc), raw=text)


def classify_naive(naive, zone):
    """把无偏移的本地墙钟时间按 IANA 时区分类。

    依据 PEP 495：fold=0 与 fold=1 两种解释做往返比较。
    - 往返不还原且两种解释偏移不同：落在春季跳时的空洞里，时间不存在；
    - 往返还原但两种解释偏移不同：落在秋季回拨的重叠区间，时间有歧义；
    - 其余：正常时间，瞬间唯一。
    """
    if naive.tzinfo is not None:
        raise ValueError('classify_naive 只接受无时区墙钟时间')
    fold0 = naive.replace(tzinfo=zone, fold=0)
    fold1 = naive.replace(tzinfo=zone, fold=1)
    roundtrip0 = fold0.astimezone(timezone.utc).astimezone(zone).replace(tzinfo=None)
    if roundtrip0 != naive:
        return GAP
    if fold0.utcoffset() != fold1.utcoffset():
        return AMBIGUOUS
    return OK


def resolve_naive(text, zone):
    """解析无偏移输入并按门店时区分类，返回 ResolvedInstant。"""
    try:
        naive = datetime.fromisoformat(text)
    except ValueError as exc:
        return ResolvedInstant(status=GAP, raw=text, reason=f'时间无法解析: {exc}')
    if naive.tzinfo is not None:
        raise ValueError('该输入带偏移，应走 parse_instant')
    kind = classify_naive(naive, zone)
    if kind == GAP:
        return ResolvedInstant(status=GAP, raw=text,
                               reason='本地时间不存在（春季跳时空洞），不得自动顺延')
    if kind == AMBIGUOUS:
        return ResolvedInstant(status=AMBIGUOUS, raw=text,
                               reason='回拨夜晚的歧义本地时间，缺少偏移无法唯一确定瞬间')
    aware = naive.replace(tzinfo=zone, fold=0)
    return ResolvedInstant(status=OK, instant=aware.astimezone(timezone.utc), raw=text)


def get_zone(name):
    """构造 IANA 时区对象，非法名称直接报错。"""
    if not isinstance(name, str) or not name:
        raise ValueError('时区名称为空')
    try:
        return ZoneInfo(name)
    except Exception as exc:  # ZoneInfoNotFoundError 等
        raise ValueError(f'未知 IANA 时区: {name}') from exc


def business_day(instant, zone, close_hour):
    """计算一个 UTC 瞬间在门店当地归属的营业日（date）。

    营业日 D 覆盖本地区间 [D 00:00, D+1 close_hour)。
    闭店时刻可以超过 24 点（如凌晨 2 点=26），跨月/跨年由日历对象处理。
    """
    if instant.tzinfo is None:
        raise ValueError('instant 必须带时区')
    local = instant.astimezone(zone)
    wall = local.timetz().replace(tzinfo=None)
    close = _close_time(close_hour)
    if close_hour >= 24:
        # 闭店在次日：本地日历日 D 的凌晨段仍属于前一营业日。
        if wall < time(hour=close_hour - 24, minute=0):
            return local.date() - timedelta(days=1)
        return local.date()
    if wall < close:
        return local.date() - timedelta(days=1)
    return local.date()


def _close_time(close_hour):
    if not isinstance(close_hour, int) or not (0 <= close_hour <= 47):
        raise ValueError('闭店时刻需为 0..47 的整数小时')
    if close_hour >= 24:
        return time(hour=close_hour - 24)
    return time(hour=close_hour)


def business_day_bounds(day, zone, close_hour):
    """营业日 day 对应的半开 UTC 区间 [start_utc, end_utc)。"""
    start_local = datetime.combine(day, time.min, tzinfo=zone)
    end_calendar_day = day + timedelta(days=1)
    close_h, close_m = _split_hour_minute(close_hour)
    end_local = datetime.combine(end_calendar_day, time(close_h, close_m), tzinfo=zone)
    return (start_local.astimezone(timezone.utc),
            end_local.astimezone(timezone.utc))


def _split_hour_minute(close_hour):
    # 目前配置只支持整点，保留分钟扩展位。
    return close_hour, 0


def iter_calendar_days(start, end_inclusive):
    """按日历推进枚举日期，绝不使用 86400 秒整数倍推算。"""
    if start > end_inclusive:
        return
    current = start
    while current <= end_inclusive:
        yield current
        current = current + timedelta(days=1)


def business_range_bounds(start_day, end_day, zone, close_hour):
    """营业日范围 [start_day, end_day] 合并后的 UTC 半开区间。"""
    first_start, _ = business_day_bounds(start_day, zone, close_hour)
    _, last_end = business_day_bounds(end_day, zone, close_hour)
    return first_start, last_end


def utc_partition_dates(start_utc, end_utc):
    """UTC 区间覆盖到的 UTC 日期分区候选（日历推进，含首尾）。"""
    if end_utc <= start_utc:
        return []
    first = start_utc.astimezone(timezone.utc).date()
    last = (end_utc - timedelta(microseconds=1)).astimezone(timezone.utc).date()
    return list(iter_calendar_days(first, last))


def parse_iso_day(text):
    return date.fromisoformat(text)
