import sys
import os
import threading
import time
import logging
from dataclasses import dataclass, field
from typing import Optional, Any, Callable, Dict, Iterator
from enum import Enum

from yt_dlp import YoutubeDL
from tqdm import tqdm # Keep tqdm import for now, will be used in generator

# Configure logging for the module
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class DownloadOptions:
    url: str
    output_dir: str = "./out"

# --- New Status Management --- #

@dataclass
class StatusPending:
    message: str = "Download pending"

@dataclass
class StatusDownloading:
    progress: float = 0.0    # 0.0 to 100.0
    filename: Optional[str] = None
    total_bytes: Optional[int] = None
    downloaded_bytes: Optional[int] = None
    speed: Optional[str] = None # e.g., "4.88MiB/s"
    eta: Optional[str] = None   # e.g., "00:56"

@dataclass
class StatusFinished:
    filename: Optional[str] = None
    filepath: Optional[str] = None
    message: str = "Download finished successfully"

@dataclass
class StatusError:
    message: str = "An error occurred during download"
    details: Optional[str] = None

# Union type for convenience
DownloadCurrentStatus = StatusPending | StatusDownloading | StatusFinished | StatusError

class YTDLPDownloader:
    def __init__(self, options: DownloadOptions):
        self.options = options
        self._current_status: DownloadCurrentStatus = StatusPending()
        self._status_lock = threading.Lock()
        self._download_event = threading.Event() # Event to signal download completion/error
        self._download_queue = [] # Queue for statuses to be consumed by generator
        self._download_thread: Optional[threading.Thread] = None

    def _post_status(self, status: DownloadCurrentStatus):
        with self._status_lock:
            self._current_status = status
            self._download_queue.append(status)

        # Signal completion/error
        if isinstance(status, (StatusFinished, StatusError)):
            self._download_event.set()

    def _progress_hook(self, d):
        # Ensure output directory exists for yt-dlp to write to
        os.makedirs(self.options.output_dir, exist_ok=True)

        if d['status'] == 'downloading':
            total_bytes = d.get('total_bytes') or d.get('total_bytes_estimate')
            downloaded_bytes = d.get('downloaded_bytes')
            progress = (downloaded_bytes / total_bytes) * 100 if total_bytes and downloaded_bytes is not None else 0.0

            self._post_status(StatusDownloading(
                progress=progress,
                filename=d.get('filename'),
                total_bytes=total_bytes,
                downloaded_bytes=downloaded_bytes,
                speed=d.get('speed'),
                eta=d.get('eta')
            ))

        elif d['status'] == 'finished':
            self._post_status(StatusFinished(
                filename=d.get('filename'),
                filepath=d.get('info_dict', {}).get('_filename') # Get final path
            ))

        elif d['status'] == 'error':
            self._post_status(StatusError(
                message=f"Download failed for {d.get('filename', self.options.url)}",
                details=str(d.get('error'))
            ))

    def _download_target(self):
        # Reset event for new download
        self._download_event.clear()
        self._download_queue.clear() # Clear queue for new download
        self._post_status(StatusPending())

        try:
            ydl_opts = {
                'outtmpl': os.path.join(self.options.output_dir, '%(id)s.%(ext)s'),
                'format': 'best',
                'progress_hooks': [self._progress_hook],
                'noplaylist': True,  # Ensure only single video is downloaded
                'quiet': True,  # Suppress default yt-dlp output
                'noprogress': True,  # Suppress default yt-dlp progress bar (tqdm will manage)
                'postprocessors': [] # Ensure no unexpected post-processing interferes
            }

            with YoutubeDL(ydl_opts) as ydl:
                ydl.download([self.options.url])
        except Exception as e:
            self._post_status(StatusError(
                message=f"Unhandled exception during download of {self.options.url}",
                details=str(e)
            ))
            logger.exception(f"Unhandled exception during download: {e}")

    def download_generator(self, progress: bool = False, interval: float = 0.5, sample_rate: int = 1) -> Iterator[DownloadCurrentStatus]:
        if not self._download_thread or not self._download_thread.is_alive():
            self._download_thread = threading.Thread(target=self._download_target, daemon=True)
            self._download_thread.start()
            logger.info(f"Download thread started for {self.options.url}")

        last_yield_time = time.time()
        downloading_status_count = 0
        pbar = None # tqdm instance for this generator

        try:
            while True:
                with self._status_lock:
                    # Yield all accumulated statuses
                    while self._download_queue:
                        status = self._download_queue.pop(0)

                        # Manage tqdm within the generator if progress=True
                        if progress and isinstance(status, StatusDownloading):
                            if pbar is None:
                                pbar = tqdm(total=status.total_bytes, unit='B', unit_scale=True, desc=status.filename or "Downloading")
                            if pbar.total != status.total_bytes: # Update total if it changed
                                pbar.total = status.total_bytes
                                pbar.refresh()
                            pbar.update(status.downloaded_bytes - pbar.n if status.downloaded_bytes is not None else 0)
                            downloading_status_count += 1
                            
                            # Apply sample_rate for downloading statuses
                            if sample_rate > 0 and downloading_status_count % sample_rate != 0 and status.progress < 100.0:
                                continue # Skip yielding this downloading status

                        yield status
                        last_yield_time = time.time()

                    # Check for completion/error
                    if isinstance(self._current_status, (StatusFinished, StatusError)):
                        if pbar:
                            pbar.close()
                        return # Exit generator

                # Implement interval delay
                if time.time() - last_yield_time < interval:
                    time.sleep(max(0, interval - (time.time() - last_yield_time)))

        except Exception as e:
            logger.exception("Error in download generator")
            if pbar:
                pbar.close()
            yield StatusError(message="Error in generator", details=str(e))
            return # Ensure generator exits
        finally:
            if pbar: # Ensure pbar is closed on any exit
                pbar.close()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        download_url = sys.argv[1]
        output_dir = "./downloads_test"
        os.makedirs(output_dir, exist_ok=True)

        options = DownloadOptions(url=download_url, output_dir=output_dir)
        downloader = YTDLPDownloader(options)

        # Example usage of the generator with progress bar
        logger.info(f"Starting download for {download_url} to {output_dir}")
        for status in downloader.download_generator(progress=True, sample_rate=5):
            # The generator handles tqdm printing, but you can also log statuses
            match status:
                case StatusPending():
                    logger.info(f"Status: {status.message}")
                case StatusDownloading():
                    pass
                case StatusFinished():
                    logger.info(f"Download finished: {status.filename} at {status.filepath}")
                case StatusError():
                    logger.error(f"Download error: {status.message} - {status.details}")
                    
    else:
        print("Usage: python downloader.py <URL>")