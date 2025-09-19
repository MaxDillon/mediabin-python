from mediabin.daemon import Daemon
import os
import duckdb
from mediabin.migrate import ensure_schema_table
import threading
import os
import duckdb
from typing import Set, Optional
from datetime import datetime

from mediabin.ytdlp_downloader import (
    YTDLPDownloader,
    StatusFinished,
    StatusError,
)
from mediabin.ytdlp_downloader.downloader import DownloadOptions


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
        self.current_downloads = set()
        self._lock_current_downloads = threading.Lock()

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
        info = YTDLPDownloader.get_info(url)

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
        
    
    def _worker_thread(self):
        while not self.exit_event.is_set():
            # trigger when event arrives or DB_POLL_INTERVAL elapses
            if self.new_in_queue.wait(timeout=_DB_POLL_INTERVAL):
                self.new_in_queue.clear()
            
            with self._lock_current_downloads:
                pass
                