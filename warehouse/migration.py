"""按分区推进的重分区迁移。

状态机：planned -> copied -> verified -> switched
任何阶段失败都可重试（幂等），switched 分区由营业日布局服务；
未 switched 的分区继续由 UTC 布局服务。回退只需把分区标记撤回，
磁盘对象保留，读视图立即恢复旧路径。
"""
import hashlib
from datetime import datetime, timezone

from .catalog import transaction
from .configuration import config_at
from .lake import PartitionedLake
from .layout import BUSINESS, LEGACY, business_partition_path
from .timeutil import business_day

PLANNED = 'planned'
COPIED = 'copied'
VERIFIED = 'verified'
SWITCHED = 'switched'
FAILED = 'failed'
ROLLED_BACK = 'rolled_back'


def _now():
    return datetime.now(timezone.utc).isoformat()


def preview_plan(connection, lake, store=None):
    """生成可预览的重分区计划（只读，不写任何状态）。"""
    return build_plan(connection, lake, store)


def build_plan(connection, lake, store=None):
    """基于真实源对象内容构建目标分区计划（只读，不落任何状态）。"""
    sql = ('SELECT DISTINCT partition_path, store FROM partition_objects '
           'WHERE layout=? AND active=1')
    parameters = [LEGACY]
    if store is not None:
        sql += ' AND store=?'
        parameters.append(store)
    plan = []
    for part in connection.execute(sql, parameters):
        source_path = part['partition_path']
        rows = list(lake.open_partition(source_path))
        targets = {}
        for row in rows:
            cfg = config_at(connection, part['store'], row['config_id'])
            day = business_day(
                datetime.fromisoformat(row['instant_utc']),
                cfg.zone(), cfg.close_hour)
            target = business_partition_path(
                part['store'], day, cfg.config_id)
            targets.setdefault(target, []).append(row)
        state_row = connection.execute(
            'SELECT state FROM migration_state WHERE partition_path=?',
            (source_path,)).fetchone()
        plan.append({
            'source_partition': source_path,
            'store': part['store'],
            'source_rows': len(rows),
            'targets': [{
                'partition': target,
                'rows': len(target_rows),
                'events': sorted(r['event'] for r in target_rows),
            } for target, target_rows in sorted(targets.items())],
            'state': state_row['state'] if state_row else 'not_started',
        })
    return plan


def migrate_partition(connection, lake, source_partition, plan_entry=None):
    """迁移单个 UTC 分区，全过程幂等，返回当前状态。"""
    existing = connection.execute(
        'SELECT state FROM migration_state WHERE partition_path=?',
        (source_partition,)).fetchone()
    if existing and existing['state'] == SWITCHED:
        return SWITCHED
    if plan_entry is None:
        plan_entry = next((p for p in build_plan(connection, lake)
                           if p['source_partition'] == source_partition), None)
        if plan_entry is None:
            raise ValueError(f'没有可迁移的源分区: {source_partition}')

    with transaction(connection):
        _upsert_state(connection, source_partition, PLANNED,
                      target_partition=','.join(t['partition']
                                                for t in plan_entry['targets']))
        source_rows = list(lake.open_partition(source_partition))
        target_objects = []
        copied_events = []
        for target in plan_entry['targets']:
            target_rows = [r for r in source_rows
                           if r['event'] in target['events']]
            digest, object_rel, count = lake.write_partition(
                target['partition'], target_rows)
            lake.append_to_manifest(
                target['partition'],
                {'object': object_rel, 'batch': 'migration'})
            connection.execute(
                'INSERT INTO partition_objects '
                '(partition_path, object_rel, layout, store, day, config_id, '
                'row_count, digest, active, batch) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?) '
                'ON CONFLICT(partition_path, object_rel) DO UPDATE SET active=1',
                (target['partition'], object_rel, BUSINESS,
                 plan_entry['store'],
                 target['partition'].split('biz_day=')[1].split('/')[0],
                 target['partition'].split('config=')[1], count, digest,
                 'migration'))
            for row in target_rows:
                connection.execute(
                    'INSERT OR IGNORE INTO event_placements '
                    '(event, store, instant_utc, layout, partition_path, object_rel, '
                    'config_id, biz_day, batch, active) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)',
                    (row['event'], row['store'], row['instant_utc'], BUSINESS,
                     target['partition'], object_rel, row['config_id'],
                     target['partition'].split('biz_day=')[1].split('/')[0],
                     'migration'))
            target_objects.append(object_rel)
            copied_events.extend(target['events'])
        _upsert_state(connection, source_partition, COPIED,
                      digest=hashlib.sha256(
                          ''.join(target_objects).encode()).hexdigest(),
                      rows_copied=len(copied_events))

        # 校验：逐事件比对源与目标，数量与事件集合必须完全一致。
        source_events = sorted(r['event'] for r in source_rows)
        if sorted(copied_events) != source_events:
            _upsert_state(connection, source_partition, FAILED,
                          detail='事件集合与源分区不一致')
            raise ValueError(f'迁移校验失败: {source_partition}')
        _upsert_state(connection, source_partition, VERIFIED,
                      detail=f'校验 {len(source_events)} 个事件一致')

        # 切换：营业日摆放激活，UTC 摆放停用，读视图此后只从营业日布局取数。
        for event in source_events:
            connection.execute(
                'UPDATE event_placements SET active=1 WHERE event=? AND layout=?',
                (event, BUSINESS))
            connection.execute(
                'UPDATE event_placements SET active=0 WHERE event=? AND layout=?',
                (event, LEGACY))
        connection.execute(
            "UPDATE partition_objects SET active=0 WHERE partition_path=? AND layout=?",
            (source_partition, LEGACY))
        _upsert_state(connection, source_partition, SWITCHED,
                      detail='营业日布局已激活，UTC 摆放已停用')
    return SWITCHED


def rollback_partition(connection, source_partition):
    """回退单个分区：恢复 UTC 摆放为权威，营业日摆放停用。"""
    with transaction(connection):
        state = connection.execute(
            'SELECT state FROM migration_state WHERE partition_path=?',
            (source_partition,)).fetchone()
        if not state or state['state'] not in (SWITCHED, VERIFIED, COPIED, FAILED):
            raise ValueError('分区处于不可回退状态')
        events = _events_of_source(connection, source_partition)
        for event in events:
            connection.execute(
                'UPDATE event_placements SET active=1 WHERE event=? AND layout=?',
                (event, LEGACY))
            connection.execute(
                'UPDATE event_placements SET active=0 WHERE event=? AND layout=?',
                (event, BUSINESS))
        connection.execute(
            "UPDATE partition_objects SET active=1 WHERE partition_path=? AND layout=?",
            (source_partition, LEGACY))
        for target in _target_paths(connection, source_partition):
            connection.execute(
                "UPDATE partition_objects SET active=0 WHERE partition_path=? AND layout=?",
                (target, BUSINESS))
        _upsert_state(connection, source_partition, ROLLED_BACK,
                      detail='已恢复 UTC 布局为权威读路径')
    return ROLLED_BACK


def _events_of_source(connection, source_partition):
    return [r['event'] for r in connection.execute(
        'SELECT DISTINCT event FROM event_placements WHERE layout=? AND '
        'partition_path=?', (LEGACY, source_partition))]


def _target_paths(connection, source_partition):
    row = connection.execute(
        'SELECT target_partition FROM migration_state WHERE partition_path=?',
        (source_partition,)).fetchone()
    if not row or not row['target_partition']:
        return []
    return row['target_partition'].split(',')


def _upsert_state(connection, partition_path, state, target_partition=None,
                  digest=None, rows_copied=None, detail=None):
    connection.execute(
        'INSERT INTO migration_state (partition_path, layout_from, layout_to, '
        'state, target_partition, digest, rows_copied, updated_utc, detail) '
        'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) '
        'ON CONFLICT(partition_path) DO UPDATE SET state=excluded.state, '
        'target_partition=COALESCE(excluded.target_partition, '
        'migration_state.target_partition), digest=COALESCE(excluded.digest, '
        'migration_state.digest), rows_copied=COALESCE(excluded.rows_copied, '
        'migration_state.rows_copied), updated_utc=excluded.updated_utc, '
        'detail=excluded.detail',
        (partition_path, LEGACY, BUSINESS, state, target_partition, digest,
         rows_copied, _now(), detail))
