import argparse
import json
from pathlib import Path

from .catalog import connect, get_layout_mode, load_file, set_layout_mode
from .configuration import list_configs, register_config
from .domain import overview
from .ingest import ingest_events
from .lake import PartitionedLake
from .migration import build_plan, migrate_partition, rollback_partition
from .query import explain_scan, query_business_days
from .reconcile import reconcile
from .store import ObjectStore


def _print(value):
    print(json.dumps(value, ensure_ascii=False, sort_keys=True,
                     allow_nan=False, indent=2))


def _load_json(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def main():
    parser = argparse.ArgumentParser(description='营业日湖表装载、查询与迁移')
    parser.add_argument('command', choices=[
        'import', 'inspect', 'export',
        'config', 'ingest', 'query', 'plan', 'migrate', 'rollback',
        'reconcile', 'quarantine', 'mode'])
    parser.add_argument('--db', required=True)
    parser.add_argument('--input')
    parser.add_argument('--batch')
    parser.add_argument('--objects')
    parser.add_argument('--lake')
    parser.add_argument('--store')
    parser.add_argument('--config-id')
    parser.add_argument('--timezone')
    parser.add_argument('--close-hour', type=int)
    parser.add_argument('--effective')
    parser.add_argument('--start-day')
    parser.add_argument('--end-day')
    parser.add_argument('--partition')
    parser.add_argument('--set-mode', choices=['legacy', 'dual', 'business'])
    args = parser.parse_args()

    Path(args.db).parent.mkdir(parents=True, exist_ok=True)
    connection = connect(args.db)
    try:
        result = _dispatch(args, connection)
        _print(result)
    finally:
        connection.close()


def _dispatch(args, connection):
    command = args.command
    if command == 'import' and (not args.input or not args.batch):
        raise SystemExit('导入需要 --input 和 --batch')
    if command == 'export' and not args.objects:
        raise SystemExit('导出需要 --objects')

    if command == 'import':
        return {'changed': load_file(connection, args.input, args.batch)}
    if command == 'inspect':
        return overview(connection)
    if command == 'export':
        records = overview(connection)
        records = records if isinstance(records, list) else [records]
        return {'object': ObjectStore(args.objects).put(records),
                'rows': len(records)}

    if command == 'config':
        if args.config_id is None:
            return [vars(c) | {'effective_utc': c.effective_utc.isoformat()}
                    for c in list_configs(connection, args.store)]
        if not (args.store and args.timezone and args.close_hour is not None
                and args.effective):
            raise SystemExit('登记配置需要 --store --config-id --timezone '
                             '--close-hour --effective')
        cfg = register_config(connection, args.store, args.config_id,
                              args.timezone, args.close_hour, args.effective)
        return {'registered': cfg.config_id,
                'effective_utc': cfg.effective_utc.isoformat()}

    if command == 'mode':
        if args.set_mode:
            set_layout_mode(connection, args.set_mode)
        return {'mode': get_layout_mode(connection)}

    if not args.lake:
        raise SystemExit('该命令需要 --lake 指定湖表根目录')
    lake = PartitionedLake(args.lake)

    if command == 'ingest':
        if not (args.input and args.batch):
            raise SystemExit('装载需要 --input 和 --batch')
        report = ingest_events(connection, args.lake,
                               _load_json(args.input), args.batch, lake=lake)
        return report.as_dict()

    if command == 'query':
        if not (args.store and args.start_day and args.end_day):
            raise SystemExit('查询需要 --store --start-day --end-day')
        rows, plan = query_business_days(
            connection, args.lake, args.store, args.start_day,
            args.end_day, lake=lake)
        return {'rows': rows, 'scan': explain_scan(plan)}

    if command == 'plan':
        return {'mode': get_layout_mode(connection),
                'plan': build_plan(connection, lake, args.store)}

    if command == 'migrate':
        if not args.partition:
            raise SystemExit('迁移需要 --partition 指定源 UTC 分区')
        state = migrate_partition(connection, lake, args.partition)
        return {'partition': args.partition, 'state': state}

    if command == 'rollback':
        if not args.partition:
            raise SystemExit('回退需要 --partition 指定源 UTC 分区')
        state = rollback_partition(connection, args.partition)
        return {'partition': args.partition, 'state': state}

    if command == 'reconcile':
        return reconcile(connection, lake, args.store)

    if command == 'quarantine':
        return {'quarantined': lake.list_quarantine()}

    raise SystemExit(f'未知命令: {command}')


if __name__ == '__main__':
    main()
