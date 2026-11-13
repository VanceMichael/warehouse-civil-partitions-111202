"""分区感知的事件装载。

流程（单事务内完成，保证失败不留半批）：
1. 批次幂等检查；
2. 逐行解析时间：带偏移唯一确定瞬间；无偏移按门店时区分类，
   跳时空洞拒绝，回拨歧义进隔离区；
3. 按当前布局模式写入物理分区，登记 event_placements，
   同一事件只有一条 active 摆放，双写也不会被读两次。
"""
from datetime import datetime

from .catalog import (encode, get_layout_mode, transaction)
from .configuration import resolve_config
from .lake import PartitionedLake
from .layout import (BUSINESS, LEGACY, business_partition_for,
                     legacy_partition_for)
from .timeutil import parse_instant, resolve_naive


class IngestReport:
    def __init__(self, batch, mode):
        self.batch = batch
        self.mode = mode
        self.accepted = 0
        self.quarantined = []
        self.rejected = []
        self.written_objects = []
        self.placements = []

    def as_dict(self):
        return {
            'batch': self.batch,
            'layout_mode': self.mode,
            'accepted': self.accepted,
            'quarantined': self.quarantined,
            'rejected': self.rejected,
            'written_objects': self.written_objects,
            'placements': self.placements,
        }


def _batch_seen(connection, batch, digest):
    previous = connection.execute(
        'SELECT digest FROM ingest_batches WHERE batch=?', (batch,)).fetchone()
    if previous is None:
        return False
    if previous['digest'] != digest:
        raise ValueError('同批次内容不一致')
    return True


def ingest_events(connection, lake_root, bundle, batch, lake=None):
    """装载 events 集合到分区湖表。"""
    if not isinstance(bundle, dict) or 'events' not in bundle:
        raise ValueError('分区装载需要 events 数据集合')
    import hashlib
    digest = hashlib.sha256(encode(bundle)).hexdigest()
    mode = get_layout_mode(connection)
    report = IngestReport(batch, mode)
    lake = lake or PartitionedLake(lake_root)

    with transaction(connection):
        if _batch_seen(connection, batch, digest):
            report.changed = False
            return report
        report.changed = True
        # 按分区缓冲，单事务内一次落盘。
        buffers = {}
        for row in bundle['events']:
            store = row['store']
            text = row['instant']
            parsed = parse_instant(text)
            if parsed.status == 'naive':
                # 需要门店时区才能解释：先取配置（同时校验门店存在）。
                # 用一个远未来瞬间取最新配置仅用于解析墙钟，真正归属再按瞬间重取。
                cfg_latest = _latest_config(connection, store)
                resolved = resolve_naive(text, cfg_latest.zone())
            else:
                resolved = parsed
            if resolved.status == 'gap':
                report.rejected.append({'event': row['event'],
                                        'reason': resolved.reason})
                continue
            if resolved.status == 'ambiguous':
                object_rel = lake.write_quarantine(
                    'ambiguous_fold', row, resolved.reason)
                connection.execute(
                    'INSERT OR REPLACE INTO quarantine_records VALUES '
                    '(?, ?, ?, ?, ?, ?, ?)',
                    (row['event'], store, text, 'ambiguous_fold',
                     resolved.reason, object_rel, batch))
                report.quarantined.append(
                    {'event': row['event'], 'object': object_rel,
                     'reason': resolved.reason})
                continue
            if resolved.status != 'ok':
                raise ValueError(f'未知解析状态: {resolved.status}')
            instant_utc = resolved.instant
            cfg = resolve_config(connection, store, instant_utc)
            stored_row = dict(row)
            stored_row['instant_utc'] = instant_utc.isoformat()
            stored_row['config_id'] = cfg.config_id
            _place_row(connection, lake, mode, stored_row, cfg,
                       instant_utc, batch, buffers, report)
        _flush_buffers(connection, lake, buffers, mode, report)
        connection.execute(
            'INSERT INTO ingest_batches VALUES (?, ?, ?)',
            (batch, digest, report.accepted))
    return report


def _latest_config(connection, store):
    row = connection.execute(
        'SELECT config_id, store, timezone, close_hour, effective_local_date, '
        'effective_utc FROM store_configs WHERE store=? '
        'ORDER BY effective_utc DESC, rowid DESC LIMIT 1', (store,)).fetchone()
    if row is None:
        raise ValueError(f'门店 {store} 未登记配置')
    from .configuration import StoreConfig
    return StoreConfig(row['config_id'], row['store'], row['timezone'],
                       row['close_hour'], row['effective_local_date'],
                       datetime.fromisoformat(row['effective_utc']))


def _place_row(connection, lake, mode, row, cfg, instant_utc, batch,
               buffers, report):
    event = row['event']
    store = row['store']
    placements = []
    if mode == 'legacy':
        path = legacy_partition_for(store, instant_utc)
        placements.append((LEGACY, path, None, None))
    elif mode == 'dual':
        legacy_path = legacy_partition_for(store, instant_utc)
        biz_path, biz_day = business_partition_for(
            store, instant_utc, cfg.zone(), cfg.close_hour, cfg.config_id)
        placements.append((LEGACY, legacy_path, None, None))
        placements.append((BUSINESS, biz_path, cfg.config_id,
                           biz_day.isoformat()))
    elif mode == 'business':
        biz_path, biz_day = business_partition_for(
            store, instant_utc, cfg.zone(), cfg.close_hour, cfg.config_id)
        placements.append((BUSINESS, biz_path, cfg.config_id,
                           biz_day.isoformat()))
    else:
        raise ValueError(f'未知布局模式: {mode}')

    for layout, path, config_id, biz_day in placements:
        buffers.setdefault((layout, path, config_id, biz_day), []).append(row)

    # 事件级摆放登记：每布局一条 active，读取时按布局优先级只取一份。
    for layout, path, config_id, biz_day in placements:
        existing = connection.execute(
            'SELECT 1 FROM event_placements WHERE event=? AND layout=? AND active=1',
            (event, layout)).fetchone()
        if existing:
            raise ValueError(f'事件 {event} 在 {layout} 布局已有 active 摆放，拒绝重复写入')
        report.placements.append(
            {'event': event, 'layout': layout, 'partition': path,
             'config_id': config_id, 'biz_day': biz_day})
    report.accepted += 1


def _flush_buffers(connection, lake, buffers, mode, report):
    for (layout, partition_path, config_id, biz_day), rows in buffers.items():
        digest, object_rel, row_count = lake.write_partition(
            partition_path, rows)
        lake.append_to_manifest(
            partition_path, {'object': object_rel, 'batch': rows[0].get('batch')})
        connection.execute(
            'INSERT INTO partition_objects '
            '(partition_path, object_rel, layout, store, day, config_id, '
            'row_count, digest, active, batch) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?) '
            'ON CONFLICT(partition_path, object_rel) DO UPDATE SET active=1',
            (partition_path, object_rel, layout, rows[0]['store'],
             biz_day or object_rel.split('utc_day=')[1].split('/')[0],
             config_id, row_count, digest, rows[0].get('batch')))
        for row in rows:
            connection.execute(
                'INSERT INTO event_placements '
                '(event, store, instant_utc, layout, partition_path, object_rel, '
                'config_id, biz_day, batch, active) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)',
                (row['event'], row['store'], row['instant_utc'], layout,
                 partition_path, object_rel, config_id, biz_day,
                 row.get('batch', report.batch)))
        report.written_objects.append(
            {'layout': layout, 'partition': partition_path,
             'object': object_rel, 'rows': row_count})
