"""门店时区与闭店时刻的版本化配置。

一条配置在某个 UTC 瞬间起生效，营业日解释永远绑定生效当时的配置版本。
配置永远追加，不就地改写历史记录，保证已写入分区的解释可追溯。
"""
from dataclasses import dataclass
from datetime import datetime, timezone

from .timeutil import get_zone

CONFIG_TABLE = 'store_configs'

CONFIG_DDL = (
    'CREATE TABLE IF NOT EXISTS store_configs ('
    'config_id TEXT NOT NULL,'
    'store TEXT NOT NULL,'
    'timezone TEXT NOT NULL,'
    'close_hour INTEGER NOT NULL,'
    'effective_local_date TEXT NOT NULL,'
    'effective_utc TEXT NOT NULL,'
    'created_utc TEXT NOT NULL,'
    'PRIMARY KEY (store, config_id))'
)


@dataclass(frozen=True)
class StoreConfig:
    config_id: str
    store: str
    timezone: str
    close_hour: int
    effective_local_date: str
    effective_utc: datetime

    def zone(self):
        return get_zone(self.timezone)


def register_config(connection, store, config_id, timezone_name, close_hour,
                    effective_local_date):
    """登记一条门店配置，生效时刻由本地日期零点换算为 UTC 并固定。"""
    zone = get_zone(timezone_name)
    if not isinstance(close_hour, int) or not (0 <= close_hour <= 47):
        raise ValueError('闭店时刻需为 0..47 的整数小时')
    from datetime import date
    day = date.fromisoformat(effective_local_date)
    effective_utc = datetime.combine(day, datetime.min.time(),
                                     tzinfo=zone).astimezone(timezone.utc)
    created = datetime.now(timezone.utc)
    connection.execute(
        'INSERT INTO store_configs VALUES (?, ?, ?, ?, ?, ?, ?)',
        (config_id, store, timezone_name, int(close_hour),
         effective_local_date, effective_utc.isoformat(), created.isoformat()))
    return StoreConfig(config_id, store, timezone_name, close_hour,
                       effective_local_date, effective_utc)


def resolve_config(connection, store, instant_utc):
    """取事件瞬间生效的配置（effective_utc <= instant 的最新一条）。"""
    row = connection.execute(
        'SELECT config_id, store, timezone, close_hour, effective_local_date, '
        'effective_utc FROM store_configs WHERE store=? AND effective_utc <= ? '
        'ORDER BY effective_utc DESC, rowid DESC LIMIT 1',
        (store, instant_utc.isoformat())).fetchone()
    if row is None:
        raise ValueError(f'门店 {store} 在 {instant_utc.isoformat()} 没有生效配置')
    return StoreConfig(row['config_id'], row['store'], row['timezone'],
                       row['close_hour'], row['effective_local_date'],
                       datetime.fromisoformat(row['effective_utc']))


def list_configs(connection, store=None):
    sql = ('SELECT config_id, store, timezone, close_hour, effective_local_date, '
           'effective_utc FROM store_configs')
    parameters = ()
    if store is not None:
        sql += ' WHERE store=?'
        parameters = (store,)
    sql += ' ORDER BY store, effective_utc'
    return [StoreConfig(row['config_id'], row['store'], row['timezone'],
                        row['close_hour'], row['effective_local_date'],
                        datetime.fromisoformat(row['effective_utc']))
            for row in connection.execute(sql, parameters)]


def config_at(connection, store, config_id):
    row = connection.execute(
        'SELECT config_id, store, timezone, close_hour, effective_local_date, '
        'effective_utc FROM store_configs WHERE store=? AND config_id=?',
        (store, config_id)).fetchone()
    if row is None:
        raise ValueError(f'配置不存在: {store}/{config_id}')
    return StoreConfig(row['config_id'], row['store'], row['timezone'],
                       row['close_hour'], row['effective_local_date'],
                       datetime.fromisoformat(row['effective_utc']))
