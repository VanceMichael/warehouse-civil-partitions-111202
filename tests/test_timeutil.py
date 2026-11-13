"""营业日与时区边界单元测试。

围绕 America/New_York 的春季跳时（2026-03-08 02:00->03:00）
与秋季回拨（2026-11-01 02:00->01:00）验证，
并断言营业日由日历运算得出，不依赖固定 86400 秒。
"""
import unittest
from datetime import datetime, timedelta, timezone

from warehouse.timeutil import (AMBIGUOUS, GAP, OK, business_day,
                                business_day_bounds, business_range_bounds,
                                classify_naive, get_zone, iter_calendar_days,
                                parse_instant, resolve_naive,
                                utc_partition_dates)

NY = get_zone('America/New_York')


def utc(text):
    return datetime.fromisoformat(text).astimezone(timezone.utc)


class WallClockClassificationTests(unittest.TestCase):
    def test_spring_forward_nonexistent_hour_is_gap(self):
        # 02:30 在 2026-03-08 不存在（钟从 02:00 直接跳到 03:00）。
        self.assertEqual(classify_naive(datetime(2026, 3, 8, 2, 30), NY), GAP)
        resolved = resolve_naive('2026-03-08T02:30:00', NY)
        self.assertEqual(resolved.status, GAP)
        self.assertIsNone(resolved.instant)

    def test_spring_forward_hour_just_after_exists(self):
        self.assertEqual(classify_naive(datetime(2026, 3, 8, 3, 30), NY), OK)
        resolved = resolve_naive('2026-03-08T03:30:00', NY)
        self.assertEqual(resolved.status, OK)
        # 03:30 EDT = UTC-4，即 07:30Z。
        self.assertEqual(resolved.instant, utc('2026-03-08T07:30:00+00:00'))

    def test_fall_back_repeated_hour_is_ambiguous(self):
        # 01:30 在 2026-11-01 出现两次（EDT 一次、EST 一次）。
        self.assertEqual(classify_naive(datetime(2026, 11, 1, 1, 30), NY),
                         AMBIGUOUS)
        resolved = resolve_naive('2026-11-01T01:30:00', NY)
        self.assertEqual(resolved.status, AMBIGUOUS)
        self.assertIsNone(resolved.instant)

    def test_fall_back_explicit_offsets_are_unique(self):
        # 同名小时的两个瞬间，带偏移即可唯一确定。
        edt = parse_instant('2026-11-01T01:30:00-04:00')
        est = parse_instant('2026-11-01T01:30:00-05:00')
        self.assertEqual(edt.status, OK)
        self.assertEqual(est.status, OK)
        self.assertEqual(edt.instant, utc('2026-11-01T05:30:00+00:00'))
        self.assertEqual(est.instant, utc('2026-11-01T06:30:00+00:00'))
        self.assertNotEqual(edt.instant, est.instant)

    def test_normal_naive_wall_time_resolves(self):
        resolved = resolve_naive('2026-06-15T12:00:00', NY)
        self.assertEqual(resolved.status, OK)
        self.assertEqual(resolved.instant, utc('2026-06-15T16:00:00+00:00'))

    def test_offset_inputs_accepted_naive_flagged(self):
        self.assertEqual(parse_instant('2026-01-01T00:00:00Z').status, OK)
        self.assertEqual(parse_instant('2026-01-01T00:00:00+05:30').status, OK)
        self.assertEqual(parse_instant('2026-01-01T00:00:00').status, 'naive')


class BusinessDayTests(unittest.TestCase):
    # 闭店时刻凌晨 2 点：营业日 D 覆盖本地 [D 00:00, D+1 02:00)。
    CLOSE = 2

    def test_late_night_belongs_to_previous_business_day(self):
        # 本地 11-01 01:30 EST（06:30Z）仍属 10-31 营业日。
        self.assertEqual(
            business_day(utc('2026-11-01T06:30:00+00:00'), NY, self.CLOSE),
            datetime(2026, 10, 31).date())

    def test_after_close_belongs_to_current_day(self):
        # 本地 11-01 03:00 EST（08:00Z）属 11-01 营业日。
        self.assertEqual(
            business_day(utc('2026-11-01T08:00:00+00:00'), NY, self.CLOSE),
            datetime(2026, 11, 1).date())

    def test_close_boundary_is_half_open(self):
        # 恰在 02:00 闭店点：归入新一天；01:59:59 归前一天。
        just_before = utc('2026-11-01T05:59:59+00:00')  # 本地 01:59:59 EDT
        at_close = utc('2026-11-01T07:00:00+00:00')      # 本地 02:00:00 EST
        self.assertEqual(business_day(just_before, NY, self.CLOSE),
                         datetime(2026, 10, 31).date())
        self.assertEqual(business_day(at_close, NY, self.CLOSE),
                         datetime(2026, 11, 1).date())

    def test_cross_month_close_day(self):
        # 本地 12-01 01:00（06:00Z）属 11-30 营业日，跨月正确。
        self.assertEqual(
            business_day(utc('2026-12-01T06:00:00+00:00'), NY, self.CLOSE),
            datetime(2026, 11, 30).date())
        # 跨年：本地 01-01 01:00 属上一年 12-31。
        self.assertEqual(
            business_day(utc('2027-01-01T06:00:00+00:00'), NY, self.CLOSE),
            datetime(2026, 12, 31).date())

    def test_spring_forward_business_day_is_25_hours(self):
        # 跳时发生在 03-08 凌晨，落在 03-08 营业日（含 03-09 02:00 闭店）。
        start, end = business_day_bounds(
            datetime(2026, 3, 8).date(), NY, self.CLOSE)
        # 基准营业日跨度 26 小时；跳时当天损失 1 小时 -> 25 小时，
        # 绝不是固定 86400 秒。
        self.assertEqual(end - start, timedelta(hours=25))
        self.assertNotEqual(end - start, timedelta(seconds=86400))

    def test_day_before_spring_forward_is_normal_26_hours(self):
        start, end = business_day_bounds(
            datetime(2026, 3, 7).date(), NY, self.CLOSE)
        self.assertEqual(end - start, timedelta(hours=26))

    def test_fall_back_business_day_is_27_hours(self):
        # 回拨发生在 11-01 凌晨，落在 11-01 营业日。
        start, end = business_day_bounds(
            datetime(2026, 11, 1).date(), NY, self.CLOSE)
        # 回拨当天多出 1 小时 -> 27 小时。
        self.assertEqual(end - start, timedelta(hours=27))

    def test_normal_day_is_26_hours_with_2am_close(self):
        # 闭店凌晨 2 点，普通营业日跨度 26 小时（非 24），证明不按 86400 推算。
        start, end = business_day_bounds(
            datetime(2026, 6, 15).date(), NY, self.CLOSE)
        self.assertEqual(end - start, timedelta(hours=26))

    def test_range_uses_calendar_not_fixed_seconds(self):
        # 范围包含跳时当天 03-08（25 小时）与普通日 03-09（26 小时）。
        start, end = business_range_bounds(
            datetime(2026, 3, 8).date(), datetime(2026, 3, 9).date(),
            NY, self.CLOSE)
        span = end - start
        # 起点按 EST、终点按 EDT，跨度不是整数天 ×86400。
        self.assertNotEqual(span.total_seconds() % 86400, 0)
        self.assertEqual(span, timedelta(hours=49))

    def test_range_two_normal_days_spans_50_hours(self):
        start, end = business_range_bounds(
            datetime(2026, 6, 15).date(), datetime(2026, 6, 16).date(),
            NY, self.CLOSE)
        # 两个营业日并集：本地 06-15 00:00 至 06-17 02:00，共 50 小时（非 48）。
        self.assertEqual(end - start, timedelta(hours=50))

    def test_calendar_iteration_advances_by_calendar_day(self):
        days = list(iter_calendar_days(datetime(2026, 3, 7).date(),
                                       datetime(2026, 3, 10).date()))
        self.assertEqual([d.day for d in days], [7, 8, 9, 10])

    def test_utc_partition_candidates_cover_window_inclusive(self):
        start = utc('2026-11-01T05:00:00+00:00')
        end = utc('2026-11-02T07:00:00+00:00')
        dates = utc_partition_dates(start, end)
        self.assertEqual([d.isoformat() for d in dates],
                         ['2026-11-01', '2026-11-02'])

    def test_midnight_close_is_calendar_day(self):
        # 闭店 24 点（0）：营业日即本地日历日。
        self.assertEqual(
            business_day(utc('2026-06-15T23:59:00+00:00'), NY, 0),
            datetime(2026, 6, 15).date())

    def test_no_business_day_is_assumed_86400_seconds(self):
        # 反模式防护：任何营业日的物理跨度都不应被假定为固定 86400 秒。
        day = datetime(2026, 11, 1).date()
        for close in (0, 2, 6):
            start, end = business_day_bounds(day, NY, close)
            # 回拨日：close=0 时 25h；带凌晨闭店时再叠加 close 小时。
            self.assertNotEqual((end - start).total_seconds(), 86400.0)
        # 普通日闭店 0 点恰好 24 小时，但这是时区换算结果而非整数天假设；
        # 跳时日即便午夜闭店也不是 86400 秒。
        spring_start, spring_end = business_day_bounds(
            datetime(2026, 3, 8).date(), NY, 0)
        self.assertEqual(
            (spring_end - spring_start).total_seconds(), 23 * 3600.0)


if __name__ == '__main__':
    unittest.main()
