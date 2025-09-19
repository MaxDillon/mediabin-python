from datetime import datetime
import os
import threading
import logging
import hashlib
import base64
from dataclasses import dataclass, field
from typing import Optional, Any, Callable, Dict, Iterator
from enum import Enum
import time # Added for time.time()
from yt_dlp import YoutubeDL
from yt_dlp.utils import DownloadCancelled

# Configure logging for the module
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


# Dataclass to represent relevant video information
@dataclass
class VideoInfo:
    mb_identifier: str
    mb_path: str

    id: str
    title: str
    uploader: Optional[str] = None
    timestamp: Optional[datetime] = None
    description: Optional[str] = None
    thumbnail: Optional[str] = None
    duration: Optional[int] = None
    webpage_url: Optional[str] = None
    video_url: Optional[str] = None # Direct URL to the video file
    extractor: Optional[str] = None
    extractor_key: Optional[str] = None
    channel: Optional[str] = None
    channel_id: Optional[str] = None
    upload_date: Optional[str] = None # YYYYMMDD format


def enrich_infodict(info_dict):
    video_id = info_dict['id']
    extractor = info_dict['extractor']
    unique_id_str = f"{extractor}__{video_id}".encode('utf-8')
    md5_hash_digest = hashlib.md5(unique_id_str).hexdigest()

    base_1 = md5_hash_digest[:4]
    base_2 = md5_hash_digest[4:8]

    info_dict['mb_path'] = f"{base_1}/{base_2}/{md5_hash_digest}"
    info_dict['mb_identifier'] = md5_hash_digest # Store the full base32 hash
    info_dict['_mb_path_components'] = {
        'b1': base_1,
        'b2': base_2,
        'full': md5_hash_digest
    }
    return info_dict


def create_video_info_from_dict(info_dict: Dict[str, Any]) -> VideoInfo:
    if 'mb_identifier' not in info_dict:
        info_dict = enrich_infodict(info_dict)

    return VideoInfo(
        mb_identifier=info_dict.get('mb_identifier'),
        mb_path=info_dict.get('mb_path'),

        id=info_dict.get('id'),
        title=info_dict.get('title'),
        uploader=info_dict.get('uploader'),
        timestamp=datetime.fromtimestamp(info_dict.get('timestamp')),
        description=info_dict.get('description'),
        thumbnail=info_dict.get('thumbnail'),
        duration=info_dict.get('duration'),
        webpage_url=info_dict.get('webpage_url'),
        video_url=info_dict.get('url'), # In some cases, 'url' directly points to the video file
        extractor=info_dict.get('extractor'),
        extractor_key=info_dict.get('extractor_key'),
        channel=info_dict.get('channel'),
        channel_id=info_dict.get('channel_id'),
        upload_date=info_dict.get('upload_date'),
    )


class CustomDownloader(YoutubeDL):
    def __init__(self, *args, info_callback: Optional[Callable[[Dict[str, Any]], None]] = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.info_callback = info_callback

    def process_info(self, info_dict):
        info_dict = enrich_infodict(info_dict)
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
        self.infodict: Optional[VideoInfo] = None
        self._current_status: DownloadCurrentStatus = StatusPending()
        self._status_lock = threading.Lock()
        self._download_event = threading.Event() # Event to signal download completion/error
        self._download_queue = [] # Queue for statuses to be consumed by generator
        self._download_thread: Optional[threading.Thread] = None
        self._cancel_event = threading.Event()

        self.status_callbacks = set()
        self._last_progress_update_time = 0.0
        self._progress_update_interval = 0.5 # seconds

    def _post_status(self, status: DownloadCurrentStatus):
        with self._status_lock:
            self._current_status = status
            self._download_queue.append(status)

        # Signal completion/error
        if isinstance(status, (StatusFinished, StatusError)):
            self._download_event.set()
        
        # Call all registered callbacks
        for cb in self.status_callbacks:
            cb(self.infodict, status)

    def _progress_hook(self, d):
        if self._cancel_event.is_set():
            raise DownloadCancelled("Download cancelled by user.")
        
        if d['status'] == 'downloading':
            current_time = time.time()
            if (current_time - self._last_progress_update_time) < self._progress_update_interval:
                return # Throttle updates
            self._last_progress_update_time = current_time

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
    
    def register_status_callback(self, cb: Callable[[Optional[VideoInfo], DownloadCurrentStatus], Any]):
        self.status_callbacks.add(cb)

    def _download_target(self):
        # Reset event for new download
        self._download_event.clear()
        self._download_queue.clear() # Clear queue for new download
        self._cancel_event.clear() # Clear cancel event for new download
        self._post_status(StatusPending())

        def info_callback(infodict):
            self.infodict = create_video_info_from_dict(infodict)

        try:
            ydl_opts = {
                'outtmpl': os.path.join(self.options.output_dir, '%(_mb_path_components.b1)s/%(_mb_path_components.b2)s/%(_mb_path_components.full)s/video.%(ext)s'),
                'format': 'best',
                'progress_hooks': [self._progress_hook],
                'noplaylist': True,  # Ensure only single video is downloaded
                'quiet': True,  # Suppress default yt-dlp output
                'noprogress': True,  # Suppress default yt-dlp progress bar (tqdm will manage)
                'writethumbnail': True, # Download video thumbnail
                'writeinfojson': True, # Download info.json
                'postprocessors': [] # Empty postprocessors list
            }

            with CustomDownloader(ydl_opts, info_callback=info_callback) as ydl:
                ydl.download([self.options.url])
        except DownloadCancelled:
            logger.info(f"Download of {self.options.url} was cancelled.")
        except Exception as e:
            self._post_status(StatusError(
                message=f"Unhandled exception during download of {self.options.url}",
                details=str(e)
            ))
            logger.exception(f"Unhandled exception during download: {e}")


    def start_download(self):
        os.makedirs(self.options.output_dir, exist_ok=True)

        with self._status_lock:
            if self._download_thread and self._download_thread.is_alive():
                logger.warning("Download already in progress.")
                return None
            self._current_status = StatusPending() # Reset current status
            self._download_queue.clear() # Clear any old messages
            self._download_event.clear() # Clear old event
            self._download_thread = threading.Thread(target=self._download_target, daemon=False)
            self._download_thread.start()
            logger.info(f"Download started for {self.options.url}")
    
    def cancel_download(self):
        with self._status_lock:
            if self._download_thread and self._download_thread.is_alive():
                logger.info(f"Attempting to cancel download for {self.options.url}")
                self._cancel_event.set() # Signal the download to cancel
            else:
                logger.info(f"No active download to cancel for {self.options.url}")


    def get_current_status(self) -> DownloadCurrentStatus:
        with self._status_lock:
            return self._current_status


    @staticmethod
    def fetch_info(url: str) -> Optional[VideoInfo]:
        """
        Retrieve video metadata (yt-dlp info_dict) for the given URL
        without downloading the media file. This is a blocking call.

        Args:
            url (str): The video URL to extract info for.

        Returns:
            dict: The yt-dlp info_dict for the video.
            None: If extraction failed.
        """
        try:
            ydl_opts = {
                'quiet': True,
                'skip_download': True,   # Do not download the video
                'no_warnings': True,
                'simulate': True,        # Only simulate, don’t download
                'forcejson': True,       # Ensure json info is retrieved
                'writesubtitles': False,
                'writethumbnail': False,
                'writeinfojson': False,  # Prevent writing info.json to disk
                'noplaylist': True,      # Don’t extract playlists
            }
            with YoutubeDL(ydl_opts) as ydl:
                info_dict = ydl.extract_info(url, download=False)
                return create_video_info_from_dict(info_dict)
        except Exception as e:
            logger.exception(f"Failed to retrieve info for {url}: {e}")
            return None

    def info(self) -> VideoInfo:
        return self.infodict