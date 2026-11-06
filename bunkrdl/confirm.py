"""Explicit user confirmation of local files; caller must hold JobLock."""
from pathlib import Path


def mark_done(store, album_id, file_ids, paths=None):
    if not isinstance(file_ids, list) or not file_ids or not all(isinstance(i, str) for i in file_ids):
        raise ValueError('file_ids 须为非空文件 id 数组')
    paths = {} if paths is None else paths
    if not isinstance(paths, dict) or set(paths) - set(file_ids) or not all(isinstance(v, str) and v for v in paths.values()):
        raise ValueError('paths 须为所选文件 id 到路径的映射')
    records = {r.file_id: r for r in store.list_files(album_id)}
    if set(file_ids) - records.keys():
        raise ValueError('包含不属于此相册的文件')
    owners = {}
    for album in store.list_albums(include_hidden=True):
        for r in store.list_files(album.album_id):
            if r.local_path:
                owners.setdefault(str(Path(r.local_path).resolve()).casefold(), set()).add((r.album_id, r.file_id))
    updates = []
    used = set()
    for ident in dict.fromkeys(file_ids):
        value = paths.get(ident) or records[ident].local_path
        if not value:
            raise ValueError('文件没有 local_path，请指定路径')
        path = Path(value).expanduser().absolute()
        key = str(path.resolve()).casefold()
        if path.is_symlink() or not path.is_file():
            raise ValueError('文件不存在或不是普通文件：' + str(path))
        if path.stat().st_size == 0:
            raise ValueError('0 字节文件不能标为完成，请检查文件：' + str(path))
        if key in used or owners.get(key, set()) - {(album_id, ident)}:
            raise ValueError('文件路径已归属其他记录')
        used.add(key)
        updates.append((album_id, ident, dict(status='done', local_path=str(path), last_error=None)))
    store.update_files(updates)
    return len(updates)
