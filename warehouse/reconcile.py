"""对账：跨布局比较事件集合、行数与内容哈希，输出可审计摘要。"""
import hashlib
import json
from datetime import datetime, timezone

from .catalog import encode, transaction
from .layout import BUSINESS, LEGACY


def _fingerprint(rows):
    rows = sorted(rows, key=lambda r: r['event'])
    payload = b''.join(encode(r) + b'\n' for r in rows)
    return hashlib.sha256(payload).hexdigest()


def _active_events(connection, layout):
    return {row['event']: dict(row) for row in connection.execute(
        'SELECT event, store, instant_utc, partition_path, object_rel, config_id, '
        'biz_day FROM event_placements WHERE layout=? AND active=1', (layout,))}


def reconcile(connection, lake, store=None):
    """比较 UTC 与营业日两套布局的 active 摆放与物理对象内容。"""
    legacy = _active_events(connection, LEGACY)
    business = _active_events(connection, BUSINESS)
    if store is not None:
        legacy = {k: v for k, v in legacy.items() if v['store'] == store}
        business = {k: v for k, v in business.items() if v['store'] == store}

    legacy_events, biz_events = set(legacy), set(business)
    only_legacy = sorted(legacy_events - biz_events)
    only_biz = sorted(biz_events - legacy_events)
    common = sorted(legacy_events & biz_events)

    mismatched = []
    for event in common:
        left, right = legacy[event], business[event]
        if left['instant_utc'] != right['instant_utc']:
            mismatched.append({'event': event,
                               'reason': 'instant_utc 不一致',
                               'legacy': left['instant_utc'],
                               'business': right['instant_utc']})
        else:
            left_payload = _object_fingerprint(lake, left['object_rel'])
            right_payload = _object_fingerprint(lake, right['object_rel'])
            left_row = _find_row(lake, left['object_rel'], event)
            right_row = _find_row(lake, right['object_rel'], event)
            if left_row != right_row:
                mismatched.append({'event': event,
                                   'reason': '对象内事件内容不一致'})

    migration_rows = connection.execute(
        'SELECT partition_path, state, rows_copied, detail FROM migration_state '
        + ('WHERE partition_path LIKE ?' if store else ''),
        (f'store={store}/%',) if store else ()).fetchall()
    states = {}
    for row in migration_rows:
        states.setdefault(row['state'], 0)
        states[row['state']] += 1

    # 权威守恒：所有事件至少有一个 active 摆放，且不存在同一事件被多个
    # active 布局同时当作权威（双写期两布局都 active 是预期的读视图冗余，
    # 由查询层按权威优先级去重；这里专门检测会导致重复计数的异常）。
    all_events = set(legacy) | set(business)
    authoritative_count = {
        event: (1 if event in legacy else 0)
        + (1 if event in business else 0)
        for event in all_events
    }
    double_counted = sorted(e for e, n in authoritative_count.items() if n > 1)
    uncounted = sorted(e for e, n in authoritative_count.items() if n == 0)
    total_distinct = connection.execute(
        'SELECT COUNT(DISTINCT event) AS n FROM event_placements'
        + (' WHERE store=?' if store else ''),
        (store,) if store else ()).fetchone()['n']
    total_active = len(legacy) + len(business)
    # 双写期：每个事件在两布局都 active（count==2）属正常；
    # 迁移切换期：部分事件 count==1、部分 count==2。真正的守恒不变量是
    # 没有事件彻底丢失（count==0），且去重事件总数与权威摆放可对账。
    no_event_lost = not uncounted
    active_covers_distinct = total_active >= total_distinct

    summary = {
        'store': store,
        'generated_utc': datetime.now(timezone.utc).isoformat(),
        'legacy_active_events': len(legacy),
        'business_active_events': len(business),
        'total_distinct_events': total_distinct,
        'total_authoritative_placements': total_active,
        'common_events': len(common),
        'only_in_legacy': only_legacy,
        'only_in_business': only_biz,
        'double_counted_events': double_counted,
        'uncounted_events': uncounted,
        'content_mismatches': mismatched,
        'migration_state_counts': states,
        # 权威守恒：没有事件丢失、去重总数可对账、无内容冲突。
        'authoritative_conservation': (
            no_event_lost and active_covers_distinct
            and not mismatched and total_distinct == len(all_events)),
        # 无重复计数：不存在同一事件被多个 active 布局同时视为权威。
        'no_double_count': not double_counted,
        # 双布局完全对齐：全部事件在两套布局都有一致副本（双写期为真）。
        'balanced': (not only_legacy and not only_biz and not mismatched),
        'legacy_fingerprint': _fingerprint(list(legacy.values())),
        'business_fingerprint': _fingerprint(list(business.values())),
    }
    _persist_run(connection, summary)
    return summary


def _find_row(lake, object_rel, event):
    payload = (lake.root / object_rel).read_bytes()
    for line in payload.splitlines():
        row = json.loads(line)
        if row['event'] == event:
            return row
    return None


def _object_fingerprint(lake, object_rel):
    return hashlib.sha256((lake.root / object_rel).read_bytes()).hexdigest()


def _persist_run(connection, summary):
    run_id = hashlib.sha256(
        (summary['generated_utc'] + str(summary['store'])).encode()
    ).hexdigest()[:16]
    with transaction(connection):
        connection.execute(
            'INSERT OR REPLACE INTO reconciliation_runs '
            '(run_id, started_utc, finished_utc, summary) VALUES (?, ?, ?, ?)',
            (run_id, summary['generated_utc'], summary['generated_utc'],
             json.dumps(summary, ensure_ascii=False, sort_keys=True)))
    summary['run_id'] = run_id
