"""营业日湖表端到端演示：DST、双布局、迁移、对账与扫描证据。

使用全新临时目录，真实执行配置、装载、查询、分区迁移、回退与对账，
打印每一步的物理扫描范围，证明无关分区从未被打开。失败返回非零退出码。
"""
import json
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from warehouse.catalog import connect, set_layout_mode
from warehouse.configuration import register_config
from warehouse.ingest import ingest_events
from warehouse.lake import PartitionedLake
from warehouse.migration import build_plan, migrate_partition, rollback_partition
from warehouse.query import explain_scan, query_business_days
from warehouse.reconcile import reconcile


def heading(text):
    print('\n' + '=' * 68)
    print(text)
    print('=' * 68)


def main():
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        db = root / 'warehouse.sqlite3'
        lake_root = root / 'lake'
        connection = connect(db)
        lake = PartitionedLake(lake_root)

        heading('1. 登记门店配置：America/New_York，凌晨 2 点闭店')
        cfg = register_config(connection, 'ny', 'cfg-1',
                              'America/New_York', 2, '2026-01-01')
        print(f"配置 {cfg.config_id} 生效 UTC 瞬间: {cfg.effective_utc.isoformat()}")

        set_layout_mode(connection, 'dual')
        events = {
            'events': [
                # 春跳：本地 02:30 不存在；03:30 带偏移唯一。
                {'event': 'spring_gap', 'store': 'ny',
                 'instant': '2026-03-08T02:30:00', 'value': '1'},
                {'event': 'spring_ok', 'store': 'ny',
                 'instant': '2026-03-08T03:30:00-04:00', 'value': '2'},
                # 秋拨：无偏移 01:30 歧义；两个带偏移 01:30 各自唯一。
                {'event': 'fall_ambiguous', 'store': 'ny',
                 'instant': '2026-11-01T01:30:00', 'value': '3'},
                {'event': 'fall_edt', 'store': 'ny',
                 'instant': '2026-11-01T01:30:00-04:00', 'value': '4'},
                {'event': 'fall_est', 'store': 'ny',
                 'instant': '2026-11-01T01:30:00-05:00', 'value': '5'},
                # 跨月闭店：12-01 01:00 本地属 11-30 营业日。
                {'event': 'cross_month', 'store': 'ny',
                 'instant': '2026-12-01T06:00:00+00:00', 'value': '6'},
                # 普通深夜单与闭店后单。
                {'event': 'late_night', 'store': 'ny',
                 'instant': '2026-11-01T05:30:00+00:00', 'value': '7'},
                {'event': 'after_close', 'store': 'ny',
                 'instant': '2026-11-01T08:00:00+00:00', 'value': '8'},
            ]
        }

        heading('2. dual 模式装载：跳时拒绝 / 回拨隔离 / 其余双布局写入')
        report = ingest_events(connection, lake_root, events, 'batch-1',
                               lake=lake)
        print(f"接受: {report.accepted} 行")
        print(f"拒绝（不存在时间，不顺延）: {[r['event'] for r in report.rejected]}")
        print(f"隔离（回拨歧义）: {[q['event'] for q in report.quarantined]}")

        heading('3. 查询营业日 2026-10-31：物理扫描范围与裁剪证据')
        rows, plan = query_business_days(
            connection, lake_root, 'ny', '2026-10-31', '2026-10-31',
            lake=lake)
        explanation = explain_scan(plan)
        print('返回行:', [(r['event'], r['business_day'],
                          r['served_from_layout']) for r in rows])
        print('UTC 窗口:', explanation['utc_window_utc'])
        print('精确谓词:', explanation['exact_predicate'])
        print('候选分区:')
        for path in explanation['candidate_partitions']:
            print('   -', path)
        print('实际打开对象:', explanation['opened_objects'])
        print('裁剪（从未打开）分区:', explanation['pruned_partitions'])
        print(f"扫描 {explanation['scanned_rows']} 行 -> "
              f"保留 {explanation['rows_after_exact_filter']} 行，"
              f"过滤 {explanation['rows_filtered_out']} 行")
        assert {r['event'] for r in rows} == {
            'fall_edt', 'fall_est', 'late_night'}
        assert all('2026-11-30' not in p and '2026-03-08' not in p
                   for p in explanation['opened_partitions'])

        heading('4. 预览重分区计划（UTC 分区按营业日拆分，不写入）')
        plan_preview = build_plan(connection, lake, 'ny')
        for entry in plan_preview:
            print(f"{entry['source_partition']} ({entry['source_rows']} 行)")
            for target in entry['targets']:
                print(f"   -> {target['partition']}: {target['events']}")

        heading('5. 迁移单个 UTC 分区并对账')
        source = 'store=ny/utc_day=2026-11-01'
        state = migrate_partition(connection, lake, source)
        print(f'迁移状态: {state}')
        summary = reconcile(connection, lake, 'ny')
        print('对账摘要:', json.dumps({
            k: summary[k] for k in (
                'legacy_active_events', 'business_active_events',
                'total_distinct_events', 'total_authoritative_placements',
                'content_mismatches', 'authoritative_conservation',
                'migration_state_counts')}, ensure_ascii=False, indent=2))
        assert summary['authoritative_conservation']
        assert not summary['content_mismatches']

        heading('6. 迁移后查询：权威切到营业日布局，UTC 分区被裁剪')
        rows, plan = query_business_days(
            connection, lake_root, 'ny', '2026-10-31', '2026-10-31',
            lake=lake)
        explanation = explain_scan(plan)
        print('返回行:', [(r['event'], r['served_from_layout']) for r in rows])
        print('实际打开对象:', explanation['opened_objects'])
        print('裁剪分区:', explanation['pruned_partitions'])
        assert all(r['served_from_layout'] == 'biz' for r in rows)

        heading('7. 回退到旧视图：UTC 布局恢复为权威读路径')
        rollback_partition(connection, source)
        rows, plan = query_business_days(
            connection, lake_root, 'ny', '2026-10-31', '2026-10-31',
            lake=lake)
        print('回退后返回行:',
              [(r['event'], r['served_from_layout']) for r in rows])
        assert all(r['served_from_layout'] == 'utc' for r in rows)

        heading('8. 隔离区内容核对')
        quarantined = lake.list_quarantine()
        print(json.dumps(quarantined, ensure_ascii=False, indent=2))
        assert len(quarantined) == 1
        assert quarantined[0]['row']['event'] == 'fall_ambiguous'

    print('\n全部演示断言通过。')
    return 0


if __name__ == '__main__':
    sys.exit(main())
