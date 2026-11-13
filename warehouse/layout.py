"""湖表物理布局与分区候选生成。

两种布局并存：
- legacy UTC：store=<store>/utc_day=<YYYY-MM-DD>/...
- business day：store=<store>/biz_day=<YYYY-MM-DD>/config=<config_id>/...

候选分区按区间边界从时区换算得到，宁多勿漏；精确时间谓词在扫描阶段过滤。
"""
from dataclasses import dataclass, field
from datetime import date

from .timeutil import (business_day, business_range_bounds, iter_calendar_days,
                       parse_iso_day, utc_partition_dates)

LEGACY = 'utc'
BUSINESS = 'biz'


def legacy_partition_path(store, utc_day):
    return f'store={store}/utc_day={utc_day.isoformat()}'


def business_partition_path(store, biz_day, config_id):
    return (f'store={store}/biz_day={biz_day.isoformat()}/config={config_id}')


def legacy_partition_for(store, instant_utc):
    return legacy_partition_path(store, instant_utc.astimezone(
        datetime_timezone_utc()).date())


def datetime_timezone_utc():
    from datetime import timezone
    return timezone.utc


def business_partition_for(store, instant_utc, zone, close_hour, config_id):
    day = business_day(instant_utc, zone, close_hour)
    return business_partition_path(store, day, config_id), day


@dataclass(frozen=True)
class PartitionCandidate:
    layout: str
    store: str
    day: date
    config_id: str | None
    path: str


@dataclass
class ScanPlan:
    """一次查询的物理扫描计划与实际执行证据。"""
    store: str
    start_day: date
    end_day: date
    utc_window: tuple
    candidates: list = field(default_factory=list)       # 计划候选分区
    opened: list = field(default_factory=list)           # 实际打开的对象
    pruned: list = field(default_factory=list)           # 候选中不存在而跳过的分区
    scanned_rows: int = 0
    kept_rows: int = 0
    filtered_out: int = 0
    exact_predicate: str = ''

    def explain(self):
        return {
            'store': self.store,
            'business_day_range': [self.start_day.isoformat(),
                                   self.end_day.isoformat()],
            'utc_window_utc': [self.utc_window[0].isoformat(),
                               self.utc_window[1].isoformat()],
            'exact_predicate': self.exact_predicate,
            'candidate_partitions': [c.path for c in self.candidates],
            'opened_objects': list(self.opened),
            'pruned_partitions': list(self.pruned),
            'scanned_rows': self.scanned_rows,
            'rows_after_exact_filter': self.kept_rows,
            'rows_filtered_out': self.filtered_out,
        }


def build_scan_plan(store, start_day_text, end_day_text, zone, close_hour,
                    configs_for_store):
    """生成保证不漏行的候选分区集合。

    营业日范围整体换算为一个 UTC 窗口；UTC 布局按窗口覆盖的 UTC 日期展开，
    营业日布局按请求的每个日历营业日展开，并为该门店每个配置版本各取一份
    （配置生效日改变会产生同营业日多版本目录，全部列入候选）。
    """
    start_day = parse_iso_day(start_day_text)
    end_day = parse_iso_day(end_day_text)
    if start_day > end_day:
        raise ValueError('营业日范围起点晚于终点')
    window_start, window_end = business_range_bounds(
        start_day, end_day, zone, close_hour)
    candidates = []
    # 旧 UTC 布局：UTC 窗口覆盖的所有 UTC 日期。
    for utc_day in utc_partition_dates(window_start, window_end):
        candidates.append(PartitionCandidate(
            LEGACY, store, utc_day, None,
            legacy_partition_path(store, utc_day)))
    # 新营业日布局：每个请求营业日 × 每个已知配置版本。
    for biz_day in iter_calendar_days(start_day, end_day):
        for config_id in configs_for_store:
            candidates.append(PartitionCandidate(
                BUSINESS, store, biz_day, config_id,
                business_partition_path(store, biz_day, config_id)))
    predicate = (f"instant_utc >= '{window_start.isoformat()}' AND "
                 f"instant_utc < '{window_end.isoformat()}'")
    return ScanPlan(store, start_day, end_day, (window_start, window_end),
                    candidates, exact_predicate=predicate)
