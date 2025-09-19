import sys
import os
import threading
import time
import logging
import hashlib
import base64
from dataclasses import dataclass, field
from typing import Optional, Any, Callable, Dict, Iterator
from enum import Enum

from yt_dlp import YoutubeDL
from tqdm import tqdm # Keep tqdm import for now, will be used in generator

# Configure logging for the module
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

class CustomDownloader(YoutubeDL):
    def __init__(self, *args, info_callback: Optional[Callable[[Dict[str, Any]], None]] = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.info_callback = info_callback

    def process_info(self, info_dict):
        video_id = info_dict['id']
        extractor = info_dict['extractor']
        unique_id_str = f"{extractor}-{video_id}"
        md5_hash_digest = hashlib.md5(unique_id_str.encode('utf-8')).digest()
        md5_b32 = base64.b32encode(md5_hash_digest).decode('utf-8').rstrip('=').upper()
        md5_hex = md5_hash_digest.hex()

        info_dict['b1_b32'] = md5_b32[:2]
        info_dict['b2_b32'] = md5_b32[2:4]

        info_dict['b1_hex'] = md5_hex[:2]
        info_dict['b2_hex'] = md5_hex[2:4]

        info_dict['hash_b32'] = md5_b32 # Store the full base32 hash
        info_dict['hash_hex'] = md5_hex # Store the full base32 hash
        if self.info_callback:
            self.info_callback(info_dict)
        return super().process_info(info_dict)

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

    def _download_target(self, info_callback: Optional[Callable[[Dict[str, Any]], None]] = None):
        # Reset event for new download
        self._download_event.clear()
        self._download_queue.clear() # Clear queue for new download
        self._post_status(StatusPending())

        try:
            ydl_opts = {
                'outtmpl': os.path.join(self.options.output_dir, '%(b1_b32)s/%(b2_b32)s/%(hash_b32)s/video.%(ext)s'),
                'format': 'best',
                'progress_hooks': [self._progress_hook],
                'noplaylist': True,  # Ensure only single video is downloaded
                'quiet': True,  # Suppress default yt-dlp output
                'noprogress': True,  # Suppress default yt-dlp progress bar (tqdm will manage)
                'writethumbnail': True, # Download video thumbnail
                'writeinfojson': True, # Download info.json
                'postprocessors': [] # Empty postprocessors list
            }

            with CustomDownloader(ydl_opts, info_callback=info_callback) as ydl: # Pass info_callback here
                ydl.download([self.options.url])
        except Exception as e:
            self._post_status(StatusError(
                message=f"Unhandled exception during download of {self.options.url}",
                details=str(e)
            ))
            logger.exception(f"Unhandled exception during download: {e}")


    def start_download(self) -> Optional[Dict[str, Any]]:
        # Event and storage for info_dict
        info_dict_ready_event = threading.Event()
        _info_dict_storage: Dict[str, Any] = {}

        def _info_callback(info_dict: Dict[str, Any]):
            _info_dict_storage.update(info_dict)
            info_dict_ready_event.set()

        with self._status_lock:
            if self._download_thread and self._download_thread.is_alive():
                logger.warning("Download already in progress.")
                return None
            self._current_status = StatusPending() # Reset current status
            self._download_queue.clear() # Clear any old messages
            self._download_event.clear() # Clear old event
            self._download_thread = threading.Thread(target=self._download_target, args=(_info_callback,), daemon=True)
            self._download_thread.start()
            logger.info(f"Download started for {self.options.url}")
        
        if info_dict_ready_event.wait():
            logger.info("Info dict processed and returned.")
            return _info_dict_storage
        else:
            logger.warning("Unexpected event exit before complete.")
            return None

    def get_current_status(self) -> DownloadCurrentStatus:
        with self._status_lock:
            return self._current_status

    def download_generator(self, progress: bool = False, interval: float = 0.5, sample_rate: int = 1) -> Iterator[DownloadCurrentStatus]:
        # If download not started, start it implicitly
        with self._status_lock:
            if not self._download_thread or not self._download_thread.is_alive():
                logger.info("Download not started, initiating...")
                self.start_download()
        
        last_yield_time = time.time()
        downloading_status_count = 0
        pbar = None # tqdm instance for this generator

        try:
            # Initialize pbar if needed based on current status
            if progress and isinstance(self.get_current_status(), StatusDownloading):
                current_dl_status = self.get_current_status()
                if current_dl_status.total_bytes:
                    pbar = tqdm(total=current_dl_status.total_bytes, initial=current_dl_status.downloaded_bytes or 0, unit='B', unit_scale=True, desc=current_dl_status.filename or "Downloading")
                else:
                    pbar = tqdm(unit='B', unit_scale=True, desc=current_dl_status.filename or "Downloading", initial=current_dl_status.downloaded_bytes or 0)

            while True:
                status_to_yield = None
                # Acquire lock for reading from queue and checking current status
                with self._status_lock:
                    if self._download_queue:
                        status_to_yield = self._download_queue.pop(0)
                    else:
                        # If queue is empty, and download is finished/errored, exit
                        if isinstance(self._current_status, (StatusFinished, StatusError)):
                            if pbar:
                                pbar.close()
                            yield self._current_status # Yield final status if not already yielded
                            return

                # If we have a status from the queue to process/yield
                if status_to_yield:
                    # Manage tqdm within the generator if progress=True
                    if progress and isinstance(status_to_yield, StatusDownloading):
                        if pbar is None:
                            # Initialize tqdm if it wasn't initialized at the start of generator (e.g., started as pending)
                            pbar = tqdm(total=status_to_yield.total_bytes, initial=status_to_yield.downloaded_bytes or 0, unit='B', unit_scale=True, desc=status_to_yield.filename or "Downloading")
                        
                        if pbar.total != status_to_yield.total_bytes: # Update total if it changed
                            pbar.total = status_to_yield.total_bytes
                            pbar.refresh()
                        # Update pbar's position; handle case where initial was 0 but new is higher
                        pbar.update((status_to_yield.downloaded_bytes or 0) - pbar.n)
                        downloading_status_count += 1
                        
                        # Apply sample_rate for downloading statuses
                        if sample_rate > 0 and downloading_status_count % sample_rate != 0 and status_to_yield.progress < 100.0:
                            # Skip yielding this downloading status unless it's the last one or error
                            if not isinstance(self._current_status, (StatusFinished, StatusError)):
                                continue # Skip yielding this specific downloading status

                    yield status_to_yield
                    last_yield_time = time.time()

                # If no status was immediately available, wait for some time or for event
                else:
                    # If download is still active, wait for new messages or interval
                    if self._download_thread and self._download_thread.is_alive():
                        # Wait for a new status to be posted or for the interval to pass
                        self._download_event.wait(timeout=interval)
                        self._download_event.clear() # Clear event after waiting
                    else:
                        # If thread died unexpectedly without final status, yield error
                        if not isinstance(self._current_status, (StatusFinished, StatusError)):
                            yield StatusError(message="Download thread unexpectedly terminated.")
                        return
                

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

        logger.info(f"Starting download for {download_url} to {output_dir}")
        downloader.start_download() # Explicitly start the download

        # Consume statuses with a generator and progress bar
        for status in downloader.download_generator(progress=True, sample_rate=1):
            match status:
                case StatusPending():
                    logger.info(f"Status: {status.message}")
                case StatusDownloading():
                    pass # tqdm handles printing
                case StatusFinished():
                    logger.info(f"Download finished: {status.filename} at {status.filepath}")
                case StatusError():
                    logger.error(f"Download error: {status.message} - {status.details}")
        
        logger.info("Generator finished.")

        # Demonstrate checking status after generator exits
        final_status = downloader.get_current_status()
        logger.info(f"Final status after generator exit: {final_status}")

        # You could potentially call generator again to see if new messages appeared
        # for status in downloader.download_generator(progress=True):
        #     logger.info(f"Second generator call status: {status}")

    else:
        print("Usage: python downloader.py <URL>")