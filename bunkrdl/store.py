"""Thread-safe SQLite persistence for albums and download state."""
from __future__ import annotations

import sqlite3
import threading
import time
from dataclasses import asdict, fields
from pathlib import Path

from .models import Album, AlbumFile, AlbumSummary, FileRecord, STATUSES
from .worktypes import WorkItem, ACTIONS, WORK_STATES, LANE_IMAGE, LANE_VIDEO


class Store:
    def __init__(self, db_path):
        if str(db_path) != ":memory:":
            Path(db_path).expanduser().parent.mkdir(parents=True, exist_ok=True)
            db_path = str(Path(db_path).expanduser())
        self._lock = threading.RLock()
        self._db = sqlite3.connect(db_path, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        with self._lock, self._db:
            self._db.executescript("""
                CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS albums (
                    album_id TEXT PRIMARY KEY, title TEXT NOT NULL,
                    url TEXT NOT NULL, added_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS files (
                    album_id TEXT NOT NULL, file_id TEXT NOT NULL,
                    name TEXT NOT NULL, slug TEXT NOT NULL, page_url TEXT NOT NULL,
                    size INTEGER, cdn_name TEXT, kind TEXT NOT NULL, uploaded TEXT,
                    status TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT, local_path TEXT, link_url TEXT, link_expires REAL,
                    updated_at REAL, PRIMARY KEY (album_id, file_id)
                );
                CREATE TABLE IF NOT EXISTS work_items (
                    work_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    album_id TEXT NOT NULL, file_id TEXT NOT NULL,
                    action TEXT NOT NULL, seq INTEGER NOT NULL,
                    state TEXT NOT NULL DEFAULT 'queued', lane TEXT NOT NULL,
                    created_at REAL NOT NULL, UNIQUE(album_id, file_id)
                );
            """)
            columns = {row['name'] for row in self._db.execute('PRAGMA table_info(albums)')}
            for name, definition in (('dest_dir', 'TEXT'), ('hidden', 'INTEGER DEFAULT 0')):
                if name not in columns:
                    self._db.execute(f'ALTER TABLE albums ADD COLUMN {name} {definition}')


    def recover_interrupted(self):
        """Call only while holding the database JobLock."""
        with self._lock, self._db:
            self._db.execute("UPDATE files SET status='pending', updated_at=? WHERE status='downloading'",
                             (time.time(),))

    def remember_action(self, album_id, file_id, action):
        """Remember the last operation (download/link) tried on a file, so a later
        "retry failed" repeats it whichever interface caused the failure."""
        self.set_meta(f'web:{album_id}:last_action:{file_id}', action)

    def get_meta(self, key):
        with self._lock:
            row = self._db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
            return row[0] if row else None

    def set_meta(self, key, value):
        with self._lock, self._db:
            self._db.execute("INSERT INTO meta VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                             (key, str(value)))

    def upsert_album(self, album):
        metadata = [f.name for f in fields(AlbumFile)]
        names = ["album_id", *metadata, "updated_at"]
        update = ", ".join(f"{name}=excluded.{name}" for name in metadata)
        with self._lock, self._db:
            self._db.execute(
                "INSERT INTO albums (album_id, title, url, added_at) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(album_id) DO UPDATE SET title=excluded.title, url=excluded.url, hidden=0",
                (album.album_id, album.title, album.url, time.time()),
            )
            for file in album.files:
                values = asdict(file)
                self._db.execute(
                    f"INSERT INTO files ({', '.join(names)}) VALUES "
                    f"({', '.join('?' for _ in names)}) "
                    f"ON CONFLICT(album_id, file_id) DO UPDATE SET {update}",
                    [album.album_id, *(values[key] for key in metadata), time.time()],
                )

    def get_album(self, album_id):
        with self._lock:
            row = self._db.execute("SELECT * FROM albums WHERE album_id=?", (album_id,)).fetchone()
            if row is None:
                return None
            metadata = [f.name for f in fields(AlbumFile)]
            files = [AlbumFile(**{key: getattr(file, key) for key in metadata})
                     for file in self.list_files(album_id)]
            return Album(row["album_id"], row["title"], row["url"], files)

    def list_albums(self, include_hidden=False):
        with self._lock:
            result = []
            query = 'SELECT album_id, title, url, added_at FROM albums'
            if not include_hidden:
                query += ' WHERE COALESCE(hidden, 0)=0'
            for row in self._db.execute(query + ' ORDER BY added_at, album_id').fetchall():
                counts = dict.fromkeys(STATUSES, 0)
                counts.update(dict(self._db.execute(
                    "SELECT status, COUNT(*) FROM files WHERE album_id=? GROUP BY status",
                    (row["album_id"],),
                ).fetchall()))
                result.append(AlbumSummary(**dict(row), counts=counts))
            return result

    def set_album_hidden(self, album_id, hidden: bool) -> None:
        with self._lock, self._db:
            cursor = self._db.execute('UPDATE albums SET hidden=? WHERE album_id=?',
                                      (int(bool(hidden)), album_id))
            if not cursor.rowcount:
                raise KeyError(album_id)

    def get_album_dest(self, album_id) -> str | None:
        with self._lock:
            row = self._db.execute('SELECT dest_dir FROM albums WHERE album_id=?', (album_id,)).fetchone()
            return row['dest_dir'] if row else None

    def set_album_dest(self, album_id, path: str) -> None:
        with self._lock, self._db:
            cursor = self._db.execute('UPDATE albums SET dest_dir=? WHERE album_id=?', (path, album_id))
            if not cursor.rowcount:
                raise KeyError(album_id)

    def add_work_items(self, rows) -> list[WorkItem]:
        added = []
        with self._lock, self._db:
            # AUTOINCREMENT keeps FIFO identities monotonic even after an empty queue.
            for album_id, file_id, action, lane in rows:
                self._validate_work({'action': action, 'lane': lane})
                cursor = self._db.execute(
                    "INSERT INTO work_items (album_id, file_id, action, seq, state, lane, created_at) "
                    "VALUES (?, ?, ?, 0, 'queued', ?, ?) ON CONFLICT(album_id, file_id) DO NOTHING",
                    (album_id, file_id, action, lane, time.time()))
                if cursor.rowcount:
                    work_id = cursor.lastrowid
                    self._db.execute('UPDATE work_items SET seq=? WHERE work_id=?', (work_id, work_id))
                    row = self._db.execute('SELECT * FROM work_items WHERE work_id=?', (work_id,)).fetchone()
                    added.append(WorkItem(**dict(row)))
        return added

    def list_work_items(self, states=None) -> list[WorkItem]:
        query, params = 'SELECT * FROM work_items', []
        if states is not None:
            params = [states] if isinstance(states, str) else list(states)
            if not params:
                return []
            query += f" WHERE state IN ({', '.join('?' for _ in params)})"
        with self._lock:
            return [WorkItem(**dict(row)) for row in self._db.execute(
                query + ' ORDER BY seq, work_id', params).fetchall()]

    @staticmethod
    def _validate_work(changes):
        allowed = {f.name for f in fields(WorkItem)} - {'work_id'}
        if (set(changes) - allowed
                or ('action' in changes and changes['action'] not in ACTIONS)
                or ('state' in changes and changes['state'] not in WORK_STATES)
                or ('lane' in changes and changes['lane'] not in (LANE_IMAGE, LANE_VIDEO))):
            raise ValueError('未知的工作项字段或状态')

    def update_work_item(self, work_id, **changes) -> None:
        self._validate_work(changes)
        if not changes:
            return
        with self._lock, self._db:
            cursor = self._db.execute(
                'UPDATE work_items SET ' + ', '.join(f'{key}=?' for key in changes)
                + ' WHERE work_id=?', [*changes.values(), work_id])
            if not cursor.rowcount:
                raise KeyError(work_id)

    def replace_work_action(self, work_id, action) -> None:
        self._validate_work({'action': action})
        with self._lock, self._db:
            cursor = self._db.execute("UPDATE work_items SET action=? WHERE work_id=? AND state='queued'",
                                      (action, work_id))
            if not cursor.rowcount:
                raise ValueError('工作项不存在或已开始处理')

    def delete_work_items(self, work_ids) -> None:
        with self._lock, self._db:
            self._db.executemany('DELETE FROM work_items WHERE work_id=?', ((i,) for i in work_ids))

    def reset_running_work(self) -> int:
        """Call only at startup, under JobLock and without active workers."""
        with self._lock, self._db:
            return self._db.execute("UPDATE work_items SET state='queued' WHERE state='running'").rowcount

    def list_files(self, album_id, status=None, kinds=None):
        query = "SELECT * FROM files WHERE album_id=?"
        params = [album_id]
        for column, selected in (("status", status), ("kind", kinds)):
            if selected is not None:
                selected = [selected] if isinstance(selected, str) else list(selected)
                if not selected:
                    return []
                query += f" AND {column} IN ({', '.join('?' for _ in selected)})"
                params.extend(selected)
        with self._lock:
            return [FileRecord(**dict(row)) for row in self._db.execute(
                query + " ORDER BY rowid", params
            ).fetchall()]

    def update_file(self, album_id, file_id, **changes):
        self.update_files([(album_id, file_id, changes)])

    def update_files(self, updates):
        """Apply (album_id, file_id, fields) entries atomically, including validation."""
        allowed = {f.name for f in fields(FileRecord)} - {"album_id", "file_id"}
        with self._lock, self._db:
            for album_id, file_id, values in updates:
                changes = dict(values)
                if set(changes) - allowed:
                    raise ValueError("未知的文件字段或状态")
                if "status" in changes and changes["status"] not in STATUSES:
                    raise ValueError("未知的文件字段或状态")
                changes["updated_at"] = time.time()
                cursor = self._db.execute(
                    "UPDATE files SET " + ", ".join(f"{key}=?" for key in changes)
                    + " WHERE album_id=? AND file_id=?",
                    [*changes.values(), album_id, file_id],
                )
                if not cursor.rowcount:
                    raise KeyError((album_id, file_id))

    # ---- 清除记录（只删状态库里的记录，磁盘上的文件一律不动） ----
    # Meta keys owned by one album live under `web:<album_id>:` (folder hint,
    # per-file last_action). Album ids match [A-Za-z0-9_-]+, so a trailing colon
    # makes the prefix unambiguous; global keys (web:ratelimit:*, web:lane:*)
    # never start with an album id.
    @staticmethod
    def _meta_keys(db, prefix) -> list[str]:
        return [row[0] for row in db.execute(
            'SELECT key FROM meta WHERE substr(key, 1, ?)=?', (len(prefix), prefix))]

    @staticmethod
    def _file_scope(album_id, file_ids, statuses):
        """Build the parameterised WHERE clause shared by match and delete."""
        if file_ids is None and statuses is None:
            raise ValueError('须指定文件 id 或状态')
        clause, params = 'album_id=?', [album_id]
        if file_ids is not None:
            ids = list(dict.fromkeys(file_ids))
            if not ids:
                return None, []
            clause += f" AND file_id IN ({', '.join('?' for _ in ids)})"
            params.extend(ids)
        if statuses is not None:
            states = list(dict.fromkeys(statuses))
            if any(state not in STATUSES for state in states):
                raise ValueError('未知的文件状态')
            if not states:
                return None, []
            clause += f" AND status IN ({', '.join('?' for _ in states)})"
            params.extend(states)
        return clause, params

    def match_file_ids(self, album_id, file_ids=None, statuses=None) -> list[str]:
        """File ids in one album that a purge with these filters would delete."""
        clause, params = self._file_scope(album_id, file_ids, statuses)
        if clause is None:
            return []
        with self._lock:
            return [row[0] for row in self._db.execute(
                f'SELECT file_id FROM files WHERE {clause} ORDER BY rowid', params)]

    def delete_files(self, album_id, file_ids=None, statuses=None) -> int:
        """Delete matching file records of one album; returns how many were removed.

        Also drops their work items and their `last_action` memory, so a later
        re-add or re-enqueue starts without the old history. Disk files are
        never touched. Unknown albums/files simply match nothing.
        """
        clause, params = self._file_scope(album_id, file_ids, statuses)
        if clause is None:
            return 0
        with self._lock, self._db:
            ids = [row[0] for row in self._db.execute(
                f'SELECT file_id FROM files WHERE {clause}', params)]
            if not ids:
                return 0
            self._db.execute(f'DELETE FROM files WHERE {clause}', params)
            self._db.executemany('DELETE FROM work_items WHERE album_id=? AND file_id=?',
                                 [(album_id, file_id) for file_id in ids])
            wanted = set(ids)
            prefix = f'web:{album_id}:last_action:'
            stale = [key for key in self._meta_keys(self._db, prefix)
                     if key[len(prefix):] in wanted]
            self._db.executemany('DELETE FROM meta WHERE key=?', [(key,) for key in stale])
            return len(ids)

    def delete_album_records(self, album_id) -> int:
        """Delete every record of one album; returns the number of files removed.

        Removes the album row (title, hidden flag and the directory binding),
        its files, its work items and its `web:<album_id>:*` meta keys, so a
        re-add behaves like a brand-new album and re-decides the download
        directory from the DESIGN rules. Downloaded files stay on disk.
        """
        with self._lock, self._db:
            ids = [row[0] for row in self._db.execute(
                'SELECT file_id FROM files WHERE album_id=?', (album_id,))]
            self._db.execute('DELETE FROM files WHERE album_id=?', (album_id,))
            self._db.execute('DELETE FROM work_items WHERE album_id=?', (album_id,))
            self._db.executemany('DELETE FROM meta WHERE key=?', [
                (key,) for key in self._meta_keys(self._db, f'web:{album_id}:')])
            self._db.execute('DELETE FROM albums WHERE album_id=?', (album_id,))
            return len(ids)

    def close(self):
        with self._lock:
            self._db.close()
