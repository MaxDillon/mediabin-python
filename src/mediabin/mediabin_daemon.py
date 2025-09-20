from dataclasses import dataclass
from datetime import datetime
from mediabin.daemon import Daemon
import os
import duckdb
from mediabin.migration import ensure_schema_table
import threading
import os
import duckdb
from typing import List, Optional, Dict, Tuple
from werkzeug.serving import make_server

from mediabin.server import ServerStartOptions, create_app
from mediabin.ytdlp_downloader import (
    YTDLPDownloader,
    StatusFinished,
    StatusError,
)
from mediabin.ytdlp_downloader.downloader import DownloadCurrentStatus, DownloadOptions, StatusDownloading, StatusPending, VideoInfo


HOME_DIRECTORY = os.path.expanduser("~")
MEDIABIN_DIRECTORY = os.path.join(HOME_DIRECTORY, ".mediabin")
# mediabin/daemon_mediabin.py
# Replace or add to your existing MediabinDaemon definition.
# Assumes the rest of your imports and project layout are intact.


# keep constants local to file for easy tuning
_DB_POLL_INTERVAL = 1.0  # seconds


class MediabinDaemon(Daemon):
    def on_spawn(self, ledgerpath: Optional[str] = None, server_options: Optional[ServerStartOptions]=None):
        os.makedirs(MEDIABIN_DIRECTORY, exist_ok=True)
        LAST_LEDGERPATH_FILE = os.path.join(MEDIABIN_DIRECTORY, "last_ledgerpath")

        if ledgerpath is None:
            if os.path.exists(LAST_LEDGERPATH_FILE):
                with open(LAST_LEDGERPATH_FILE, "r") as f:
                    self.ledgerpath = os.path.abspath(f.read().strip())
            else:
                self.ledgerpath = os.path.abspath(os.path.join(MEDIABIN_DIRECTORY, "ledger.db"))
        else:
            self.ledgerpath = os.path.abspath(ledgerpath)
        
        with open(LAST_LEDGERPATH_FILE, "w") as f:
            f.write(self.ledgerpath)

        self.db = self._init_db()
        self.datadir = self._get_or_set_datadir()

        self._restart_downloading_jobs()

        self.new_in_queue = threading.Event()
        self.exit_event = threading.Event()

        self.max_concurrent_downloads = 3
        self.current_downloads: Dict[str, YTDLPDownloader] = {}
        self._lock_current_downloads = threading.Lock()

        self.current_statuses: Dict[str, StatusDownloading | StatusPending] = {}
        self._lock_current_statuses = threading.Lock()

        self._worker_thread = threading.Thread(target=self._worker_thread_proc, daemon=False)
        self._worker_thread.start()

        app = create_app(ledgerpath=self.ledgerpath)
        self._web_server = make_server("0.0.0.0", 8080, app)
        self._web_thread = threading.Thread(target=self._web_server.serve_forever, daemon=False)
        self._web_thread.start()

    def _restart_downloading_jobs(self):
        self.db.execute("UPDATE media.media SET status = 'pending' WHERE status = 'downloading'")

    def _init_db(self):
        conn = duckdb.connect(self.ledgerpath)
        ensure_schema_table(conn)
        # Ensure schema is up to date
        from mediabin.migration import migrate_to_version, get_hightest_version

        highest_version = get_hightest_version()
        version = highest_version

        migrate_to_version(conn, version)
        return conn

    def _get_or_set_datadir(self) -> str:
        datadir_result = self.db.sql("SELECT datadir_location FROM metadata").fetchone()
        if datadir_result and datadir_result[0] is not None:
            datadir: str = datadir_result[0]
        else:
            datadir = os.path.abspath(os.path.join(os.path.dirname(self.ledgerpath), "media_data"))
            self.db.execute(
                "INSERT INTO metadata (datadir_location) VALUES (?)",
                (datadir,),
            )
            self.db.commit()

        os.makedirs(datadir, exist_ok=True)
        return datadir

    def get_datadir_location(self) -> str | None:
        result = self.db.sql("SELECT datadir_location FROM metadata").fetchone()
        return result[0] if result else None

    def register_new_download(self, url):
        info = YTDLPDownloader.fetch_info(url)

        # starts downloader process, returns info of started process
        if info is None:
            print(f"Failed to get url {url}")
            return

        if info.mb_identifier is None:
            print(f"Failed to get url {url}")
            return

        try:
            self.db.execute(
                """INSERT INTO media.media (
                    id,
                    title,
                    origin_url,
                    video_url,
                    thumbnail_url,
                    timestamp_created,
                    object_path,
                    status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')""", 
                (info.mb_identifier, info.title, info.webpage_url, info.video_url, info.thumbnail, info.timestamp, info.mb_path)
            )
        except duckdb.ConstraintException:
            print(f"{url} is already downloaded or is currently in the queue")

        self.new_in_queue.set()

    @dataclass
    class ListCurrentProcsResp:
        current_jobs: List[Tuple[str, StatusDownloading]]
        pending_jobs: List[Tuple[str, StatusPending]]

    def list_current_procs(self) -> ListCurrentProcsResp:
        with self._lock_current_statuses:
            downloading_ids = [id for id, status in self.current_statuses.items() if isinstance(status, StatusDownloading)]
            pending_ids =  [id for id, status in self.current_statuses.items() if isinstance(status, StatusPending)]

            active_jobs = self.db.execute("SELECT id, title FROM media.media WHERE id IN ?", (downloading_ids,)).fetchall()

            downloading = [(title, self.current_statuses[id]) for id, title in active_jobs]


            pending = self.db.execute("""
                SELECT title FROM media.media WHERE status = 'pending'
                UNION ALL
                SELECT title FROM media.media WHERE id IN ?
            """, (pending_ids,)).fetchall()
            
            return MediabinDaemon.ListCurrentProcsResp(
                current_jobs=downloading,
                pending_jobs=[(res[0], StatusPending()) for res in pending]
            )

    def list_media(self, title_like: str | None = None, tags: list[str] = []) -> list[str]:
        query = "SELECT m.title FROM media.media m"
        where_clauses = ["m.status = 'complete'"]
        join_clauses = []
        query_args = []

        if tags:
            join_clauses.append("JOIN media.tags t ON m.id = t.resource_id")
            where_clauses.append(f"t.tag IN ({', '.join(['?'] * len(tags))})")
            query_args.extend(tags)

        if title_like:
            search_words = title_like.split()
            if search_words:
                title_pattern = '%'.join(search_words)
                where_clauses.append("m.title ILIKE ?")
                query_args.append(f"%{title_pattern}%")

        if join_clauses:
            query += " " + " ".join(join_clauses)
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)

        complete = self.db.execute(query, tuple(query_args)).fetchall()
        return [row[0] for row in complete]
        
    def _status_callback(self, info: Optional[VideoInfo], status: DownloadCurrentStatus):
        if not info:
            return
        id = info.mb_identifier

        with self._lock_current_statuses, self._lock_current_downloads:
            print(f"Got status {status} for {id}")
            match status:
                case StatusPending():
                    self.current_statuses[id] = status
                case StatusDownloading():
                    self.current_statuses[id] = status
                case StatusError():
                    self.db.execute("UPDATE media.media SET status = 'error' WHERE id = ?", (id,))
                    del self.current_downloads[id]
                    del self.current_statuses[id]
                case StatusFinished():
                    now = datetime.now()
                    self.db.execute("UPDATE media.media SET status = 'complete', timestamp_installed = ?, timestamp_updated = ? WHERE id = ?", (now, now, id))
                    del self.current_downloads[id]
                    del self.current_statuses[id]

    def on_stop(self):
        with self._lock_current_downloads:
            for job in self.current_downloads.values():
                job.cancel_download()

        self._web_server.shutdown()
        self.exit_event.set()

        self._web_thread.join()
        self._worker_thread.join()

    def _worker_thread_proc(self):
        while not self.exit_event.is_set():
            # trigger when event arrives or DB_POLL_INTERVAL elapses
            if self.new_in_queue.wait(timeout=_DB_POLL_INTERVAL):
                self.new_in_queue.clear()
            
            with self._lock_current_downloads:
                if len(self.current_downloads) >= self.max_concurrent_downloads:
                    continue

                next_vals = self.db.sql("SELECT id, origin_url FROM media.media WHERE status = 'pending'").fetchone()
                if next_vals is None:
                    continue
                
                id, url = next_vals
                self.db.execute("UPDATE media.media SET status = 'downloading' WHERE id = ?", (id,))

                new_job = YTDLPDownloader(DownloadOptions(url, self.datadir))
                new_job.register_status_callback(self._status_callback)

                self.current_downloads[id] = new_job
                self.current_statuses[id] = StatusPending()
                print(f"Starting job {id}")
                new_job.start_download()