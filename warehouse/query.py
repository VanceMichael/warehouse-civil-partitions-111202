"""统一读视图：同时覆盖 UTC 与营业日布局，按事件去重，精确谓词过滤。

查询只打开扫描计划中的候选分区；不存在的分区记为 pruned（裁剪证据）。
同一事件在两种布局都有副本时，按当前布局模式选择权威布局，绝不计两次。
返回业务行之外，还输出实际扫描说明（scan explanation）。
"""
from datetime import datetime, timezone

from .catalog import get_layout_mode
from .configuration import resolve_config
from .lake import PartitionedLake, ScanAuditor
from .layout import BUSINESS, LEGACY, build_scan_plan
from .timeutil import business_day


def query_business_days(connection, lake_root, store, start_day, end_day,
                        lake=None, auditor=None):
    mode = get_layout_mode(connection)
    # 用门店当前配置的时区/闭店时刻生成候选窗口；
    # 历史配置版本的分区通过 config 维度的候选一并覆盖。
    probe = datetime.now(timezone.utc)
    cfg = resolve_config(connection, store, probe)
    zone = cfg.zone()
    config_ids = [row['config_id'] for row in connection.execute(
        'SELECT DISTINCT config_id FROM store_configs WHERE store=? '
        'ORDER BY config_id', (store,))]
    plan = build_scan_plan(store, start_day, end_day, zone, cfg.close_hour,
                           config_ids)
    auditor = auditor or ScanAuditor()
    own_lake = lake is None
    lake = lake or PartitionedLake(lake_root, auditor=auditor)
    # 共享 lake 可能带着上一次查询的 auditor；本次强制绑定当前 auditor，
    # 结束后恢复原值，避免跨查询串台导致扫描证据丢失。
    previous_auditor = lake.auditor
    lake.auditor = auditor

    authoritative = BUSINESS if mode in ('dual', 'business') else LEGACY
    # 事件去重：先收集权威布局，再用另一布局补齐缺失事件。
    chosen = {}
    fallback = {}
    def _is_active(partition_path, object_rel):
        row = connection.execute(
            'SELECT active FROM partition_objects WHERE partition_path=? AND '
            'object_rel=?', (partition_path, object_rel)).fetchone()
        return row is not None and row['active'] == 1

    try:
        for candidate in plan.candidates:
            if not lake.partition_exists(candidate.path):
                plan.pruned.append(candidate.path)
                continue
            rows = list(lake.open_partition(candidate.path,
                                            active_filter=_is_active))
            if not rows:
                # 分区存在但对象均已停用（迁移切换后），同样记录为裁剪。
                plan.pruned.append(candidate.path)
                continue
            plan.scanned_rows += len(rows)
            bucket = chosen if candidate.layout == authoritative else fallback
            for row in rows:
                # 同布局内多对象/多配置版本也可能出现同一事件，保留首份。
                bucket.setdefault(row['event'], (row, candidate))

        window_start, window_end = plan.utc_window
        events = dict(chosen)
        for event, payload in fallback.items():
            if event not in events:
                events[event] = payload

        kept = []
        for event, (row, candidate) in sorted(events.items()):
            instant = datetime.fromisoformat(row['instant_utc'])
            # 精确时间谓词：候选宁可多开，边界行在这里过滤。
            if not (window_start <= instant < window_end):
                plan.filtered_out += 1
                continue
            # 再用行内配置版本复核营业日归属，防止旧版本目录串日。
            row_cfg = _row_config(connection, store, row)
            actual_day = business_day(instant, row_cfg.zone(), row_cfg.close_hour)
            if not (plan.start_day <= actual_day <= plan.end_day):
                plan.filtered_out += 1
                continue
            plan.kept_rows += 1
            kept.append({
                'event': row['event'],
                'store': row['store'],
                'instant_utc': row['instant_utc'],
                'value': row['value'],
                'business_day': actual_day.isoformat(),
                'served_from_layout': candidate.layout,
                'served_from_partition': candidate.path,
            })
        plan.opened = list(auditor.opened)
        return kept, plan
    finally:
        if not own_lake:
            lake.auditor = previous_auditor


def _row_config(connection, store, row):
    config_id = row.get('config_id')
    if config_id:
        from .configuration import config_at
        try:
            return config_at(connection, store, config_id)
        except ValueError:
            pass
    instant = datetime.fromisoformat(row['instant_utc'])
    return resolve_config(connection, store, instant)


def explain_scan(plan):
    explanation = plan.explain()
    explanation['opened_partitions'] = sorted({
        path.rsplit('/', 1)[0] for path in explanation['opened_objects']
    })
    return explanation
