from mediabin.daemon import Daemon
import os
import duckdb
from mediabin.migrate import ensure_schema_table
import threading
import os
import duckdb
from typing import Set, Optional, Dict
from datetime import datetime

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
    def on_spawn(self, ledgerpath: Optional[str] = None):
        # --- existing initialization (kept as you provided) ---
        self.ledgerpath = ledgerpath
        if self.ledgerpath is None:
            self.ledgerpath = os.path.join(MEDIABIN_DIRECTORY, "ledger.db")

        self.db = self._init_db()
        self.datadir = self._get_or_set_datadir()

        self.new_in_queue = threading.Event()
        self.exit_event = threading.Event()

        self.max_concurrent_downloads = 3
        self.current_downloads: Set[YTDLPDownloader] = set()
        self._lock_current_downloads = threading.Lock()

        self.current_statuses: Dict[str, StatusDownloading | StatusPending] = {}
        self._lock_current_statuses = threading.Lock()

        self._worker_thread = threading.Thread(target=self._worker_thread_proc, daemon=False)
        self._worker_thread.start()



    def _init_db(self):
        conn = duckdb.connect(self.ledgerpath)
        ensure_schema_table(conn)
        # Ensure schema is up to date
        from mediabin.migrate import migrate_to_version, get_hightest_version

        highest_version = get_hightest_version()
        version = highest_version

        migrate_to_version(conn, version)
        return conn

    def _get_or_set_datadir(self) -> str:
        datadir_result = self.db.sql("SELECT datadir_location FROM metadata").fetchone()
        if datadir_result and datadir_result[0] is not None:
            datadir: str = datadir_result[0]
        else:
            datadir = os.path.join(MEDIABIN_DIRECTORY, "media_data")
            self.db.execute(
                "INSERT INTO metadata (datadir_location) VALUES (?)",
                (datadir,),
            )
            self.db.commit()

        os.makedirs(datadir, exist_ok=True)
        return datadir


    def register_new_download(self, url):
        info = YTDLPDownloader.fetch_info(url)

        # starts downloader process, returns info of started process
        if info is None:
            print(f"Failed to get url {url}")
            return

        if info.hash_hex is None:
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
                    status
                ) VALUES (?, ?, ?, ?, ?, ?, 'pending')""", 
                (info.hash_hex, info.title, info.webpage_url, info.video_url, info.thumbnail, info.timestamp)
            )
        except duckdb.ConstraintException:
            print(f"{url} is already downloaded or is currently in the queue")

        self.new_in_queue.set()

    def list_media(self):
        pending = self.db.sql("SELECT title, status FROM media.media WHERE status = 'pending'").fetchall()
        if pending:
            print("Pending:")
        for title, status in pending:
            print(f"    - {title}")
        
        complete = self.db.sql("SELECT title, status FROM media.media WHERE status = 'complete'").fetchall()
        if complete:
            print("Complete:")
        for title, status in complete:
            print(f"    - {title}")
        
        if len(self.current_statuses) > 0:
            with self._lock_current_statuses:
                for status in self.current_statuses.values():
                    print(status)
    
    def _worker_thread_proc(self):
        while not self.exit_event.is_set():
            # trigger when event arrives or DB_POLL_INTERVAL elapses
            if self.new_in_queue.wait(timeout=_DB_POLL_INTERVAL):
                self.new_in_queue.clear()
            
            with self._lock_current_downloads:
                for job in self.current_downloads:
                    status = job.get_current_status()
                    info = job.info()

                    match status:
                        case StatusPending():
                            self.current_statuses[info.hash_hex] = status
                        case StatusDownloading():
                            self.current_statuses[info.hash_hex] = status
                        case StatusError():
                            self.db.execute("UPDATE media.media SET status = 'error' WHERE id = ?", (info.hash_hex,))
                            self.current_downloads.remove(job)
                        case StatusFinished():
                            self.db.execute("UPDATE media.media SET status = 'complete' WHERE id = ?", (info.hash_hex,))
                            self.current_downloads.remove(job)

                if len(self.current_downloads) < self.max_concurrent_downloads:
                    next_vals = self.db.sql("SELECT id, origin_url FROM media.media WHERE status = 'pending'").fetchone()
                    if next_vals is None:
                        return
                    
                    id, url = next_vals
                    self.db.execute("UPDATE media.media SET status = 'downloading' WHERE id = ?", (id,))
                    new_job = YTDLPDownloader(DownloadOptions(url, self.datadir))

                    def status_callback(info: VideoInfo, status: DownloadCurrentStatus):
                        with self._lock_current_statuses:
                            match status:
                                case StatusPending():
                                    self.current_statuses[info.hash_hex] = status
                                case StatusDownloading():
                                    self.current_statuses[info.hash_hex] = status
                                case StatusError():
                                    del self.current_statuses[info.hash_hex]
                                case StatusFinished():
                                    del self.current_statuses[info.hash_hex]
                    
                    new_job.register_status_callback(status_callback)

                    self.current_downloads.add(new_job)
                    new_job.start_download()