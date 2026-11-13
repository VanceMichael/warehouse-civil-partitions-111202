import hashlib
import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path

from .definition import SCHEMA

EXTRA_DDL = [
    # 门店时区/闭店配置（版本化，只追加）
    'CREATE TABLE IF NOT EXISTS store_configs ('
    'config_id TEXT NOT NULL, store TEXT NOT NULL, timezone TEXT NOT NULL, '
    'close_hour INTEGER NOT NULL, effective_local_date TEXT NOT NULL, '
    'effective_utc TEXT NOT NULL, created_utc TEXT NOT NULL, '
    'PRIMARY KEY (store, config_id))',
    # 每个事件在每种布局各有一条摆放记录；同布局重复 active 即报错，防止双写重复计数
    'CREATE TABLE IF NOT EXISTS event_placements ('
    'event TEXT NOT NULL, store TEXT NOT NULL, instant_utc TEXT NOT NULL, '
    'layout TEXT NOT NULL, partition_path TEXT NOT NULL, object_rel TEXT NOT NULL, '
    'config_id TEXT, biz_day TEXT, batch TEXT NOT NULL, '
    'active INTEGER NOT NULL DEFAULT 1, PRIMARY KEY (event, layout))',
    'CREATE INDEX IF NOT EXISTS idx_placements_partition '
    'ON event_placements(partition_path)',
    'CREATE INDEX IF NOT EXISTS idx_placements_store_day '
    'ON event_placements(store, biz_day)',
    # 分区对象清单（与磁盘 manifest 对应）
    'CREATE TABLE IF NOT EXISTS partition_objects ('
    'partition_path TEXT NOT NULL, object_rel TEXT NOT NULL, '
    'layout TEXT NOT NULL, store TEXT NOT NULL, day TEXT NOT NULL, '
    'config_id TEXT, row_count INTEGER NOT NULL, digest TEXT NOT NULL, '
    'active INTEGER NOT NULL DEFAULT 1, batch TEXT, '
    'PRIMARY KEY (partition_path, object_rel))',
    # 隔离区登记
    'CREATE TABLE IF NOT EXISTS quarantine_records ('
    'event TEXT PRIMARY KEY, store TEXT NOT NULL, raw_instant TEXT NOT NULL, '
    'reason_code TEXT NOT NULL, reason TEXT NOT NULL, object_rel TEXT NOT NULL, '
    'batch TEXT NOT NULL)',
    # 分区迁移状态机
    'CREATE TABLE IF NOT EXISTS migration_state ('
    'partition_path TEXT PRIMARY KEY, layout_from TEXT NOT NULL, '
    'layout_to TEXT NOT NULL, state TEXT NOT NULL, '
    'target_partition TEXT, digest TEXT, rows_copied INTEGER, '
    'updated_utc TEXT NOT NULL, detail TEXT)',
    # 全库布局版本（并发写入与回退的一致性锚点）
    'CREATE TABLE IF NOT EXISTS layout_registry ('
    'name TEXT PRIMARY KEY, value TEXT NOT NULL, updated_utc TEXT NOT NULL)',
    # 对账快照
    'CREATE TABLE IF NOT EXISTS reconciliation_runs ('
    'run_id TEXT PRIMARY KEY, started_utc TEXT NOT NULL, '
    'finished_utc TEXT, summary TEXT NOT NULL)',
]


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf-8')


def connect(path):
    connection = sqlite3.connect(path, timeout=5, isolation_level=None)
    connection.row_factory = sqlite3.Row
    connection.execute('PRAGMA foreign_keys=ON')
    connection.execute('PRAGMA busy_timeout=5000')
    for table, columns in SCHEMA.items():
        connection.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({columns})')
    connection.execute('CREATE TABLE IF NOT EXISTS ingest_batches (batch TEXT PRIMARY KEY, digest TEXT NOT NULL, rows INTEGER NOT NULL)')
    for statement in EXTRA_DDL:
        connection.execute(statement)
    return connection


@contextmanager
def transaction(connection):
    connection.execute('BEGIN IMMEDIATE')
    try:
        yield connection
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()


def load_bundle(connection, bundle, batch):
    if not isinstance(bundle, dict) or not isinstance(batch, str) or not batch:
        raise ValueError('输入需要命名数据集合与批次标识')
    if set(bundle) - set(SCHEMA):
        raise ValueError('来源包含未登记的数据集合')
    digest = hashlib.sha256(encode(bundle)).hexdigest()
    count = 0
    with transaction(connection):
        previous = connection.execute('SELECT digest FROM ingest_batches WHERE batch=?', (batch,)).fetchone()
        if previous:
            if previous['digest'] != digest:
                raise ValueError('同批次内容不一致')
            return False
        for table, rows in bundle.items():
            if not isinstance(rows, list):
                raise ValueError('数据集合必须是记录数组')
            columns = [record['name'] for record in connection.execute(f'PRAGMA table_info("{table}")')]
            for row in rows:
                if not isinstance(row, dict) or set(row) != set(columns):
                    raise ValueError('记录字段与已登记结构不一致')
                names = ', '.join(f'"{column}"' for column in columns)
                placeholders = ', '.join('?' for column in columns)
                connection.execute(f'INSERT INTO "{table}" ({names}) VALUES ({placeholders})', [row[column] for column in columns])
                count += 1
        connection.execute('INSERT INTO ingest_batches VALUES (?, ?, ?)', (batch, digest, count))
    return True


def load_file(connection, path, batch):
    return load_bundle(connection, json.loads(Path(path).read_text(encoding='utf-8')), batch)


# ---------------------------------------------------------------------
# 布局版本注册：并发写入者在同一布局版本下工作，可整体回退。
# ---------------------------------------------------------------------
def get_layout_mode(connection):
    row = connection.execute(
        "SELECT value FROM layout_registry WHERE name='mode'").fetchone()
    return row['value'] if row else 'legacy'


def set_layout_mode(connection, mode):
    from datetime import datetime, timezone
    if mode not in ('legacy', 'dual', 'business'):
        raise ValueError('布局模式必须是 legacy/dual/business')
    connection.execute(
        'INSERT INTO layout_registry(name, value, updated_utc) VALUES (?, ?, ?) '
        'ON CONFLICT(name) DO UPDATE SET value=excluded.value, '
        'updated_utc=excluded.updated_utc',
        ('mode', mode, datetime.now(timezone.utc).isoformat()))


def get_layout_version(connection):
    row = connection.execute(
        "SELECT value FROM layout_registry WHERE name='version'").fetchone()
    return int(row['value']) if row else 1


def bump_layout_version(connection):
    from datetime import datetime, timezone
    current = get_layout_version(connection)
    connection.execute(
        'INSERT INTO layout_registry(name, value, updated_utc) VALUES (?, ?, ?) '
        'ON CONFLICT(name) DO UPDATE SET value=excluded.value, '
        'updated_utc=excluded.updated_utc',
        ('version', str(current + 1),
         datetime.now(timezone.utc).isoformat()))
    return current + 1
