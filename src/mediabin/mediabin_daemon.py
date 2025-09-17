from mediabin.daemon import Daemon
import time
from typing import Dict, Optional
import typer
import threading
import queue
import os
import duckdb
from mediabin.migrate import migrate_to_version, ensure_schema_table, get_hightest_version, get_current_version

from mediabin.ytdlp_downloader import YTDLPDownloader, DownloadOptions, StatusDownloading, StatusFinished, StatusError, StatusPending

HOME_DIRECTORY = os.path.expanduser("~")
MEDIABIN_DIRECTORY = os.path.join(HOME_DIRECTORY, ".mediabin")

class MediabinDaemon(Daemon):
    """
    Daemon process for handling mediabin daemon requests
    """
    def on_spawn(
        self,
        ledgerpath: Optional[str] = None
    ):
        """
        Initializes daemon-specific resources.

        Args:
            ledgerpath: An optional path to the ledger file. If not provided, a default path will be used.
        """

        self.ledgerpath = ledgerpath
        if self.ledgerpath == None:
            self.ledgerpath = os.path.join(MEDIABIN_DIRECTORY, "ledger.db")

        # Handle schema migrations automatically
        self.db = duckdb.connect(self.ledgerpath)
        ensure_schema_table(self.db)
        version = get_hightest_version()
        if get_current_version(self.db) != version:
            migrate_to_version(self.db, version)