"""营业日湖表端到端测试。

覆盖：
- America/New_York 春季跳时（不存在时间拒绝，不顺延）
- 秋季回拨（无偏移歧义隔离，带偏移两个同名小时各自唯一）
- 跨月/跨年闭店日归属
- 迟到事件仍按其瞬间归属正确营业日
- 双布局读视图去重，同事件不计两次
- 扫描计划与实际打开证据：无关分区被裁剪、从未打开
- 分区迁移、失败恢复（幂等重试）、回退
- 配置生效日改变后新旧分区共存，解释绑定版本
"""
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path

from warehouse.catalog import connect, get_layout_mode, set_layout_mode
from warehouse.configuration import register_config
from warehouse.ingest import ingest_events
from warehouse.lake import PartitionedLake, ScanAuditor
from warehouse.layout import legacy_partition_path, business_partition_path
from warehouse.migration import (SWITCHED, build_plan, migrate_partition,
                                 rollback_partition)
from warehouse.query import explain_scan, query_business_days
from warehouse.reconcile import reconcile

NY = 'America/New_York'


def event(event_id, instant, value='1', store='ny'):
    return {'event': event_id, 'store': store, 'instant': instant,
            'value': value}


# 精心构造的 DST 与跨边界事件集。
EVENTS = {
    # 春跳：03-08 02:30 本地不存在（拒绝）；03:30 存在。
    'spring_gap': event('spring_gap', '2026-03-08T02:30:00'),
    'spring_ok': event('spring_ok', '2026-03-08T03:30:00-04:00'),
    # 秋拨：无偏移 01:30 歧义（隔离）；带偏移的两个 01:30 各唯一。
    'fall_ambiguous': event('fall_ambiguous', '2026-11-01T01:30:00'),
    'fall_edt': event('fall_edt', '2026-11-01T01:30:00-04:00'),
    'fall_est': event('fall_est', '2026-11-01T01:30:00-05:00'),
    # 跨月闭店：12-01 01:00 本地（06:00Z）属 11-30 营业日。
    'cross_month': event('cross_month', '2026-12-01T06:00:00+00:00'),
    # 跨年闭店：2027-01-01 01:00 本地属 2026-12-31 营业日。
    'cross_year': event('cross_year', '2027-01-01T06:00:00+00:00'),
    # 普通深夜单：11-01 00:30 EST 属 10-31 营业日。
    'late_night': event('late_night', '2026-11-01T05:30:00+00:00'),
    # 闭店边界之后：11-01 03:00 EST 属 11-01 营业日。
    'after_close': event('after_close', '2026-11-01T08:00:00+00:00'),
}


class LakeTestBase(unittest.TestCase):
    close_hour = 2

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.db = self.root / 'w.sqlite3'
        self.lake_root = self.root / 'lake'
        self.connection = connect(self.db)
        register_config(self.connection, 'ny', 'cfg-1', NY,
                        self.close_hour, '2026-01-01')
        self.lake = PartitionedLake(self.lake_root)

    def tearDown(self):
        self.connection.close()
        self.temporary.cleanup()

    def ingest(self, *ids, batch='b1', mode=None):
        if mode:
            set_layout_mode(self.connection, mode)
        bundle = {'events': [EVENTS[i] for i in ids]}
        return ingest_events(self.connection, self.lake_root, bundle, batch,
                             lake=self.lake)

    def query(self, start, end, store='ny'):
        return query_business_days(self.connection, self.lake_root, store,
                                   start, end, lake=self.lake)


class IngestClassificationTests(LakeTestBase):
    def test_spring_gap_rejected_not_silently_shifted(self):
        report = self.ingest('spring_gap', 'spring_ok', mode='dual')
        self.assertEqual(report.accepted, 1)
        self.assertEqual([r['event'] for r in report.rejected],
                         ['spring_gap'])
        # 被拒事件没有任何摆放，也没有进隔离区（它不是歧义而是不存在）。
        self.assertEqual(
            self.connection.execute(
                "SELECT COUNT(*) FROM event_placements WHERE event='spring_gap'"
            ).fetchone()[0], 0)
        reason = report.rejected[0]['reason']
        self.assertIn('不存在', reason)

    def test_fall_back_ambiguous_quarantined(self):
        report = self.ingest('fall_ambiguous', mode='dual')
        self.assertEqual(report.accepted, 0)
        self.assertEqual(len(report.quarantined), 1)
        quarantined = self.lake.list_quarantine()
        self.assertEqual(len(quarantined), 1)
        self.assertEqual(quarantined[0]['reason_code'], 'ambiguous_fold')
        # 隔离目录物理存在且独立于业务分区。
        self.assertTrue((self.lake_root / '_quarantine' /
                         'ambiguous_fold').exists())

    def test_two_fold_hours_with_offsets_are_distinct_events(self):
        report = self.ingest('fall_edt', 'fall_est', mode='dual')
        self.assertEqual(report.accepted, 2)
        rows, _ = self.query('2026-10-31', '2026-11-01')
        by_event = {r['event']: r for r in rows}
        # 两个 01:30 都在 10-31 营业日（凌晨段），但是不同瞬间、不同行。
        self.assertEqual(by_event['fall_edt']['business_day'], '2026-10-31')
        self.assertEqual(by_event['fall_est']['business_day'], '2026-10-31')
        self.assertNotEqual(by_event['fall_edt']['instant_utc'],
                            by_event['fall_est']['instant_utc'])


class BusinessDayPlacementTests(LakeTestBase):
    def test_cross_month_and_year_close_day(self):
        self.ingest('cross_month', 'cross_year', mode='business')
        rows, _ = self.query('2026-11-30', '2026-11-30')
        self.assertEqual([r['event'] for r in rows], ['cross_month'])
        rows, _ = self.query('2026-12-31', '2026-12-31')
        self.assertEqual([r['event'] for r in rows], ['cross_year'])

    def test_late_night_and_after_close_split(self):
        self.ingest('late_night', 'after_close', mode='business')
        rows, _ = self.query('2026-10-31', '2026-11-01')
        mapping = {r['event']: r['business_day'] for r in rows}
        self.assertEqual(mapping['late_night'], '2026-10-31')
        self.assertEqual(mapping['after_close'], '2026-11-01')

    def test_late_arriving_event_uses_its_instant_not_arrival_time(self):
        # 先装载 11 月数据，再补一个迟到的 10-31 营业日事件。
        self.ingest('after_close', batch='nov', mode='business')
        late = event('late_arrival', '2026-10-20T03:00:00+00:00')
        EVENTS.setdefault('late_arrival', late)
        bundle = {'events': [late]}
        ingest_events(self.connection, self.lake_root, bundle, 'late_batch',
                      lake=self.lake)
        rows, _ = self.query('2026-10-19', '2026-10-20')
        self.assertEqual([r['event'] for r in rows], ['late_arrival'])
        self.assertEqual(rows[0]['business_day'], '2026-10-19')

    def test_physical_business_partition_path_contains_config(self):
        self.ingest('after_close', mode='business')
        expected = business_partition_path('ny',
                                           datetime(2026, 11, 1).date(),
                                           'cfg-1')
        self.assertTrue(self.lake.partition_exists(expected))


class DualLayoutDedupTests(LakeTestBase):
    def test_same_event_not_double_counted_in_dual_mode(self):
        report = self.ingest('fall_edt', 'fall_est', 'late_night',
                             mode='dual')
        # 每个事件在两种布局各一条摆放。
        self.assertEqual(
            self.connection.execute(
                'SELECT COUNT(*) FROM event_placements').fetchone()[0], 6)
        rows, plan = self.query('2026-10-31', '2026-10-31')
        events = [r['event'] for r in rows]
        self.assertEqual(sorted(events),
                         sorted(['fall_edt', 'fall_est', 'late_night']))
        self.assertEqual(len(events), len(set(events)))
        # 扫描行数含两布局副本，但去重后结果不重复。
        self.assertGreater(plan.scanned_rows, plan.kept_rows)

    def test_duplicate_active_placement_rejected(self):
        self.ingest('after_close', batch='b1', mode='business')
        with self.assertRaises(ValueError):
            self.ingest('after_close', batch='b2', mode='business')

    def test_idempotent_batch_replay(self):
        self.ingest('after_close', batch='same', mode='dual')
        report = self.ingest('after_close', batch='same', mode='dual')
        self.assertFalse(report.changed)


class ScanPruningEvidenceTests(LakeTestBase):
    def test_unrelated_partitions_are_never_opened(self):
        self.ingest('late_night', 'after_close', 'cross_month',
                    'spring_ok', mode='dual')
        rows, plan = self.query('2026-10-31', '2026-10-31')
        self.assertEqual({r['event'] for r in rows}, {'late_night'})
        explanation = explain_scan(plan)
        opened_partitions = set(explanation['opened_partitions'])
        # 无关营业日（11-01、11-30、03-08 等）绝不出现在打开列表。
        for opened in opened_partitions:
            self.assertNotIn('biz_day=2026-11-01', opened)
            self.assertNotIn('biz_day=2026-11-30', opened)
            self.assertNotIn('biz_day=2026-03-08', opened)
        # 不存在的候选分区记录为裁剪。
        self.assertTrue(explanation['pruned_partitions'])
        # 精确谓词过滤证据存在。
        self.assertIn('instant_utc', explanation['exact_predicate'])

    def test_auditor_independently_records_open_files(self):
        self.ingest('late_night', 'after_close', mode='dual')
        auditor = ScanAuditor()
        lake = PartitionedLake(self.lake_root, auditor=auditor)
        query_business_days(self.connection, self.lake_root, 'ny',
                            '2026-10-31', '2026-10-31', lake=lake,
                            auditor=auditor)
        touched = auditor.touched_partitions()
        # 审计器证明：只有 10-31 营业日相关分区被触碰。
        self.assertTrue(touched)
        for partition in touched:
            self.assertTrue('biz_day=2026-10-31' in partition
                            or 'utc_day=2026-11-01' in partition
                            or 'utc_day=2026-10-31' in partition)

    def test_scan_window_covers_both_sides_of_midnight_close(self):
        self.ingest('late_night', mode='business')
        _, plan = self.query('2026-10-31', '2026-10-31')
        # 营业日 UTC 窗口必须延伸到次日闭店时刻（含），证明不漏凌晨单。
        window_end = plan.utc_window[1]
        self.assertGreaterEqual(
            window_end,
            datetime.fromisoformat('2026-11-01T07:00:00+00:00').replace(
                tzinfo=timezone.utc))

    def test_exact_predicate_filters_boundary_rows(self):
        # dual 模式下查询 11-01 营业日，UTC 候选分区 utc_day=2026-11-01
        # 同时含归属 10-31 的凌晨单 late_night；它必须被精确谓词/归属复核剔除，
        # 而 after_close 保留。
        self.ingest('late_night', 'after_close', mode='dual')
        rows, plan = self.query('2026-11-01', '2026-11-01')
        self.assertEqual({r['event'] for r in rows}, {'after_close'})
        self.assertGreaterEqual(plan.filtered_out, 1)
        self.assertEqual(plan.kept_rows, 1)
        # 证明包含跨边界行的 UTC 分区确实被打开（过滤发生在读后而非裁剪）。
        self.assertTrue(
            any('utc_day=2026-11-01' in o for o in plan.opened))


class MigrationTests(LakeTestBase):
    def _source_partition(self, utc_day):
        return legacy_partition_path('ny',
                                     datetime.fromisoformat(utc_day).date())

    def test_preview_plan_splits_utc_partition_by_business_day(self):
        self.ingest('late_night', 'after_close', mode='dual')
        plan = build_plan(self.connection, self.lake, 'ny')
        source = self._source_partition('2026-11-01')
        entry = next(p for p in plan
                     if p['source_partition'] == source)
        targets = {t['partition']: t['events'] for t in entry['targets']}
        # 同一个 UTC 日期分区被拆到两个营业日。
        self.assertIn(business_partition_path(
            'ny', datetime(2026, 10, 31).date(), 'cfg-1'), targets)
        self.assertIn(business_partition_path(
            'ny', datetime(2026, 11, 1).date(), 'cfg-1'), targets)
        self.assertEqual(targets[business_partition_path(
            'ny', datetime(2026, 10, 31).date(), 'cfg-1')], ['late_night'])

    def test_migrate_partition_switches_authority_and_is_idempotent(self):
        self.ingest('late_night', 'after_close', mode='dual')
        source = self._source_partition('2026-11-01')
        state = migrate_partition(self.connection, self.lake, source)
        self.assertEqual(state, SWITCHED)
        # 重复迁移直接返回 switched，不产生重复对象。
        again = migrate_partition(self.connection, self.lake, source)
        self.assertEqual(again, SWITCHED)
        rows, _ = self.query('2026-10-31', '2026-11-01')
        self.assertTrue(all(r['served_from_layout'] == 'biz' for r in rows))
        # UTC 摆放已停用。
        active_legacy = self.connection.execute(
            "SELECT COUNT(*) FROM event_placements WHERE layout='utc' AND "
            "active=1").fetchone()[0]
        self.assertEqual(active_legacy, 0)

    def test_reconcile_reports_conservation_and_balance(self):
        self.ingest('late_night', 'after_close', mode='dual')
        source = self._source_partition('2026-11-01')
        before = reconcile(self.connection, self.lake, 'ny')
        # 双写期间两布局完全对齐。
        self.assertTrue(before['balanced'])
        self.assertTrue(before['authoritative_conservation'])
        migrate_partition(self.connection, self.lake, source)
        after = reconcile(self.connection, self.lake, 'ny')
        # 切换后权威摆放总数仍等于去重事件数，无重复计数。
        self.assertTrue(after['authoritative_conservation'])
        self.assertEqual(after['total_authoritative_placements'],
                         after['total_distinct_events'])
        self.assertEqual(after['content_mismatches'], [])

    def test_rollback_restores_utc_read_path(self):
        self.ingest('late_night', 'after_close', mode='dual')
        source = self._source_partition('2026-11-01')
        migrate_partition(self.connection, self.lake, source)
        rollback_partition(self.connection, source)
        rows, _ = self.query('2026-10-31', '2026-11-01')
        self.assertTrue(rows)
        self.assertTrue(all(r['served_from_layout'] == 'utc' for r in rows))
        # 回退后对账恢复双布局权威守恒。
        summary = reconcile(self.connection, self.lake, 'ny')
        self.assertTrue(summary['authoritative_conservation'])

    def test_rollback_then_remigrate_is_idempotent(self):
        self.ingest('late_night', 'after_close', mode='dual')
        source = self._source_partition('2026-11-01')
        migrate_partition(self.connection, self.lake, source)
        rollback_partition(self.connection, source)
        # 回退后可再次迁移，最终权威仍为营业日布局，事件不丢不重。
        state = migrate_partition(self.connection, self.lake, source)
        self.assertEqual(state, SWITCHED)
        rows, _ = self.query('2026-10-31', '2026-11-01')
        self.assertEqual(sorted(r['event'] for r in rows),
                         ['after_close', 'late_night'])
        self.assertTrue(all(r['served_from_layout'] == 'biz' for r in rows))
        per_event = self.connection.execute(
            'SELECT event, SUM(active) AS n FROM event_placements '
            'GROUP BY event').fetchall()
        self.assertTrue(all(r['n'] == 1 for r in per_event))

    def test_migration_failure_leaves_no_half_state_and_retries(self):
        self.ingest('late_night', 'after_close', mode='dual')
        source = self._source_partition('2026-11-01')
        # 破坏校验：临时让目标事件集合不一致，迁移应整体回滚。
        original = self.lake.write_partition
        calls = {'n': 0}

        def flaky(partition_path, rows):
            calls['n'] += 1
            if calls['n'] == 1:
                raise RuntimeError('模拟对象存储写入中断')
            return original(partition_path, rows)

        self.lake.write_partition = flaky
        with self.assertRaises(RuntimeError):
            migrate_partition(self.connection, self.lake, source)
        self.lake.write_partition = original
        # 事务回滚：没有 switched 残留，重试成功。
        state_row = self.connection.execute(
            'SELECT state FROM migration_state WHERE partition_path=?',
            (source,)).fetchone()
        self.assertTrue(state_row is None
                        or state_row['state'] != 'switched')
        state = migrate_partition(self.connection, self.lake, source)
        self.assertEqual(state, SWITCHED)


class ConfigVersionTests(LakeTestBase):
    def test_new_config_effective_date_creates_versioned_partitions(self):
        self.ingest('after_close', batch='old', mode='business')
        # 新配置：闭店时刻改为凌晨 3 点，12-01 生效。
        register_config(self.connection, 'ny', 'cfg-2', NY, 3,
                        '2026-12-01')
        new_event = event('cfg2_event', '2026-12-02T07:30:00+00:00')
        EVENTS.setdefault('cfg2_event', new_event)
        ingest_events(self.connection, self.lake_root,
                      {'events': [new_event]}, 'new', lake=self.lake)
        # 新事件落在 config=cfg-2 的分区目录。
        path_cfg2 = business_partition_path(
            'ny', datetime(2026, 12, 1).date(), 'cfg-2')
        self.assertTrue(self.lake.partition_exists(path_cfg2))
        # 旧分区仍带 cfg-1，历史解释不被改写。
        path_cfg1 = business_partition_path(
            'ny', datetime(2026, 11, 1).date(), 'cfg-1')
        self.assertTrue(self.lake.partition_exists(path_cfg1))
        rows = self.connection.execute(
            'SELECT DISTINCT config_id FROM event_placements WHERE '
            "layout='biz' ORDER BY config_id").fetchall()
        self.assertEqual([r['config_id'] for r in rows],
                         ['cfg-1', 'cfg-2'])

    def test_query_covers_multiple_config_partition_versions(self):
        self.ingest('after_close', batch='old', mode='business')
        register_config(self.connection, 'ny', 'cfg-2', NY, 3,
                        '2026-12-01')
        # 查询候选应包含同一门店的两个配置版本目录。
        _, plan = self.query('2026-11-01', '2026-12-02')
        configs = {c.config_id for c in plan.candidates
                   if c.config_id is not None}
        self.assertEqual(configs, {'cfg-1', 'cfg-2'})


    def test_repeated_queries_on_shared_lake_keep_separate_evidence(self):
        # 回归：复用同一 lake 对象连续查询时，每次扫描证据必须独立，
        # 不能把上一次查询的 auditor 带到下一次。
        self.ingest('late_night', 'after_close', 'cross_month', mode='dual')
        rows1, plan1 = self.query('2026-10-31', '2026-10-31')
        rows2, plan2 = self.query('2026-11-30', '2026-11-30')
        self.assertTrue(rows1)
        self.assertTrue(rows2)
        # 两次打开的对象互不相同，且第二次只含 11-30 营业日相关分区。
        self.assertNotEqual(set(plan1.opened), set(plan2.opened))
        self.assertTrue(all('2026-11-30' in o or 'utc_day=2026-12-01' in o
                            for o in plan2.opened))
        self.assertFalse(any('2026-10-31' in o for o in plan2.opened))


class LegacyReadabilityTests(LakeTestBase):
    def test_legacy_layout_remains_queryable_during_migration(self):
        # 迁移期间旧 UTC 布局必须始终可读，营业日语义在旧数据上同样成立。
        set_layout_mode(self.connection, 'legacy')
        bundle = {'events': [event('legacy_only',
                                   '2026-11-01T06:30:00+00:00')]}
        report = ingest_events(self.connection, self.lake_root, bundle,
                               'legacy_batch', lake=self.lake)
        self.assertEqual([p['layout'] for p in report.placements], ['utc'])
        rows, plan = self.query('2026-10-31', '2026-10-31')
        self.assertEqual([(r['event'], r['served_from_layout']) for r in rows],
                         [('legacy_only', 'utc')])
        explanation = explain_scan(plan)
        # 营业日布局候选存在但无数据，记录为裁剪而非报错。
        self.assertTrue(any('biz_day=' in p for p in
                            explanation['pruned_partitions']))
        self.assertTrue(any('utc_day=' in o for o in
                            explanation['opened_objects']))


class ConcurrencyTests(LakeTestBase):
    def test_concurrent_writers_share_consistent_layout_mode(self):
        set_layout_mode(self.connection, 'dual')
        errors = []

        def writer(batch, event_id, instant):
            try:
                conn = connect(self.db)
                lake = PartitionedLake(self.lake_root)
                ingest_events(conn, self.lake_root,
                              {'events': [event(event_id, instant)]},
                              batch, lake=lake)
                conn.close()
            except Exception as exc:  # 串行化冲突应报错而非写出不一致布局
                errors.append(exc)

        threads = [
            threading.Thread(target=writer,
                             args=('t1', 'concurrent_1',
                                   '2026-11-03T10:00:00+00:00')),
            threading.Thread(target=writer,
                             args=('t2', 'concurrent_2',
                                   '2026-11-03T11:00:00+00:00')),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        # 两个批次都成功（BEGIN IMMEDIATE 串行化），布局模式保持 dual。
        self.assertEqual(errors, [])
        self.assertEqual(get_layout_mode(self.connection), 'dual')
        count = self.connection.execute(
            "SELECT COUNT(DISTINCT event) FROM event_placements").fetchone()[0]
        self.assertEqual(count, 2)
        # 每个事件都在统一的 dual 模式下双布局摆放。
        per_event = self.connection.execute(
            'SELECT event, COUNT(*) AS n FROM event_placements '
            'GROUP BY event').fetchall()
        self.assertTrue(all(r['n'] == 2 for r in per_event))


if __name__ == '__main__':
    unittest.main()
