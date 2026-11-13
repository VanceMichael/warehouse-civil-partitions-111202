"""分区湖存储：内容寻址 JSONL 对象、物理分区目录与隔离区。

对象按内容哈希命名、写入时 fsync 并以硬链接原子发布；
每个分区维护 manifest（对象清单），读取只打开清单内对象。
所有打开动作经过审计器，便于测试证明哪些分区被裁剪、从未打开。
"""
import hashlib
import json
import os
import tempfile
from pathlib import Path

from .catalog import encode

MANIFEST = 'manifest.json'
QUARANTINE_ROOT = '_quarantine'


class ScanAuditor:
    """记录查询过程中真正发生的文件打开动作。"""

    def __init__(self):
        self.opened = []
        self.manifest_reads = []

    def record_manifest(self, path):
        self.manifest_reads.append(path)

    def record_object(self, path):
        self.opened.append(path)

    def touched_partitions(self):
        return sorted({str(Path(p).parent) for p in self.opened})


class PartitionedLake:
    def __init__(self, root, auditor=None):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        (self.root / QUARANTINE_ROOT).mkdir(exist_ok=True)
        self.auditor = auditor

    # ---- 底层对象写入 -------------------------------------------------
    def _write_payload(self, directory, payload):
        directory.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256(payload).hexdigest()
        target = directory / (digest + '.jsonl')
        if target.exists():
            return digest
        descriptor, temporary = tempfile.mkstemp(
            dir=directory, prefix='.writing-')
        try:
            with os.fdopen(descriptor, 'wb') as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            try:
                os.link(temporary, target)
            except FileExistsError:
                pass
            dir_handle = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(dir_handle)
            finally:
                os.close(dir_handle)
        finally:
            Path(temporary).unlink(missing_ok=True)
        return digest

    def _payload(self, rows):
        return b''.join(encode(row) + b'\n' for row in rows)

    # ---- 分区写入 -----------------------------------------------------
    def write_partition(self, partition_path, rows):
        """把一批行写入分区，返回 (对象摘要, 对象相对路径, 行数)。"""
        directory = self.root / partition_path
        payload = self._payload(rows)
        digest = self._write_payload(directory, payload)
        object_rel = f'{partition_path}/{digest}.jsonl'
        return digest, object_rel, len(rows)

    def append_to_manifest(self, partition_path, entry):
        """把对象登记追加进分区 manifest（按 object 路径去重，原子替换）。"""
        directory = self.root / partition_path
        directory.mkdir(parents=True, exist_ok=True)
        manifest = directory / MANIFEST
        entries = self._read_manifest(partition_path, audit=False)
        object_rel = entry['object'] if isinstance(entry, dict) else entry
        if not any((e['object'] if isinstance(e, dict) else e) == object_rel
                   for e in entries):
            entries.append(entry)
        self._atomic_write_json(manifest, entries)
        return entries

    def replace_manifest(self, partition_path, entries):
        directory = self.root / partition_path
        directory.mkdir(parents=True, exist_ok=True)
        self._atomic_write_json(directory / MANIFEST, entries)

    def _atomic_write_json(self, target, value):
        payload = json.dumps(value, ensure_ascii=False, sort_keys=True,
                             allow_nan=False).encode('utf-8')
        descriptor, temporary = tempfile.mkstemp(
            dir=target.parent, prefix='.manifest-')
        try:
            with os.fdopen(descriptor, 'wb') as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, target)
        finally:
            Path(temporary).unlink(missing_ok=True)

    def _read_manifest(self, partition_path, audit=True):
        manifest = self.root / partition_path / MANIFEST
        if not manifest.exists():
            return []
        if audit and self.auditor is not None:
            self.auditor.record_manifest(str(manifest.relative_to(self.root)))
        return json.loads(manifest.read_text(encoding='utf-8'))

    def partition_exists(self, partition_path):
        return (self.root / partition_path / MANIFEST).exists()

    # ---- 分区读取 -----------------------------------------------------
    def open_partition(self, partition_path, active_filter=None):
        """打开分区内 manifest 登记的全部对象，逐行 yield。

        不存在的分区返回 None，供查询计划记录为裁剪（pruned）。
        active_filter: 可选回调 (partition_path, object_rel) -> bool，
        迁移切换后用 catalog 的 active 标志排除已停用对象，避免重复打开。
        """
        entries = self._read_manifest(partition_path)
        if not entries:
            return None
        for entry in entries:
            object_rel = entry['object'] if isinstance(entry, dict) else entry
            if active_filter is not None and not active_filter(
                    partition_path, object_rel):
                continue
            object_path = self.root / object_rel
            if self.auditor is not None:
                self.auditor.record_object(object_rel)
            payload = object_path.read_bytes()
            digest = object_path.name[:-len('.jsonl')]
            if hashlib.sha256(payload).hexdigest() != digest:
                raise ValueError(f'对象内容校验失败: {object_rel}')
            for line in payload.splitlines():
                yield json.loads(line)

    # ---- 隔离区 -------------------------------------------------------
    def write_quarantine(self, reason_code, row, reason):
        directory = self.root / QUARANTINE_ROOT / reason_code
        payload = self._payload([{'row': row, 'reason': reason,
                                  'reason_code': reason_code}])
        digest = self._write_payload(directory, payload)
        object_rel = f'{QUARANTINE_ROOT}/{reason_code}/{digest}.jsonl'
        entries = self._read_manifest(
            f'{QUARANTINE_ROOT}/{reason_code}', audit=False)
        entry = {'object': object_rel}
        if entry not in entries:
            entries.append(entry)
            self.replace_manifest(f'{QUARANTINE_ROOT}/{reason_code}', entries)
        return object_rel

    def list_quarantine(self, reason_code=None):
        base = self.root / QUARANTINE_ROOT
        if reason_code is not None:
            codes = [reason_code]
        else:
            codes = sorted(p.name for p in base.iterdir() if p.is_dir()) \
                if base.exists() else []
        result = []
        for code in codes:
            for entry in self._read_manifest(
                    f'{QUARANTINE_ROOT}/{code}', audit=False):
                object_rel = entry['object'] if isinstance(entry, dict) else entry
                payload = (self.root / object_rel).read_bytes()
                for line in payload.splitlines():
                    result.append(json.loads(line))
        return result

    # ---- 维护 ---------------------------------------------------------
    def delete_object(self, object_rel):
        target = self.root / object_rel
        if target.exists():
            target.unlink()
