import time
from typing import Dict, Optional
import typer
import threading
import queue

from mediabin.daemon import Daemon
from mediabin.ytdlp_downloader import YTDLPDownloader, DownloadOptions, StatusDownloading, StatusFinished, StatusError, StatusPending

class MyDaemon(Daemon):
    def on_spawn(self):
        self.active_downloads: Dict[str, YTDLPDownloader] = {}
        self.download_queue: queue.Queue = queue.Queue()
        self.completed_downloads: Dict[str, YTDLPDownloader] = {}
        self.active_downloads_limit: int = 3
        self.download_worker_thread: Optional[threading.Thread] = None
        self._stop_worker_event: threading.Event = threading.Event()
        
        # Start the download worker thread
        self.download_worker_thread = threading.Thread(target=self._download_worker, daemon=True)
        self.download_worker_thread.start()

    def _download_worker(self):
        while not self._stop_worker_event.is_set():
            # Check for completed downloads and move them
            for url, downloader in list(self.active_downloads.items()):
                status = downloader.get_current_status()
                if isinstance(status, (StatusFinished, StatusError)):
                    self.completed_downloads[url] = self.active_downloads.pop(url)
                    print(f"Download {url} finished/errored: {status}")

            # Start new downloads if capacity allows and queue is not empty
            while len(self.active_downloads) < self.active_downloads_limit and not self.download_queue.empty():
                try:
                    url, options = self.download_queue.get_nowait()
                    if url not in self.active_downloads and url not in self.completed_downloads:
                        downloader = YTDLPDownloader(options)
                        self.active_downloads[url] = downloader
                        downloader.start_download()
                        print(f"Started download for {url} from queue.")
                    else:
                        print(f"Skipping {url}, already active or completed.")
                    self.download_queue.task_done()
                except queue.Empty:
                    break # Queue is empty

            time.sleep(1) # Poll every second

    def on_stop(self):
        self._stop_worker_event.set()
        if self.download_worker_thread and self.download_worker_thread.is_alive():
            self.download_worker_thread.join(timeout=5) # Give it some time to stop
        super().on_stop()


app = typer.Typer()

daemon = MyDaemon()

@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    start_service: bool = typer.Option(False, "--start-service", help="Starts the mediabin daemon service."),
    stop_service: bool = typer.Option(False, "--stop-service", help="Stops the mediabin daemon service."),
    restart_service: bool = typer.Option(False, "--restart-service", help="Restarts the mediabin daemon service (stops then starts)."),
):
    if ctx.invoked_subcommand is not None:
        return
    service_options = [start_service, stop_service, restart_service]
    if sum(service_options) > 1:
        raise typer.BadParameter("Cannot specify more than one of --start-service, --stop-service, or --restart-service.")
    elif sum(service_options) == 0:
        cli = typer.main.get_command(app)
        typer.echo(cli.get_help(ctx))  # reuse the current ctx here

    if stop_service or restart_service:
        print("Stopping mediabin daemon service...")
        try:
            daemon.stop()
        except ProcessLookupError:
            print("no service to stop")
    
    if start_service or restart_service:
        if daemon.is_process_running():
            print(f"Daemon is already running")
            raise typer.Exit(code=1)
        print("Starting mediabin daemon service...")
        pid = daemon.spawn()
        print(f"Started with pid: {pid}")


@app.command("ping")
@daemon.command
def ping_command():
    print("pong")

@app.command("install")
@app.command("i")
@daemon.command
def install_media(url: str = typer.Argument(..., help="The URL of the media to download.")):
    if url in daemon.active_downloads:
        print(f"Download for {url} is already active or in queue.")
        return

    options = DownloadOptions(url=url)
    daemon.download_queue.put((url, options))
    print(f"Added {url} to download queue. Check status with 'mb list'.")

@app.command("list")
@app.command("ls")
@daemon.command
def list_downloads():
    output = []

    if not daemon.active_downloads and daemon.download_queue.empty() and not daemon.completed_downloads:
        return "No downloads in progress, queued, or completed."

    if daemon.active_downloads:
        output.append("Active Downloads:")
        for url, downloader in daemon.active_downloads.items():
            status = downloader.get_current_status()
            match status:
                case StatusDownloading():
                    output.append(f"  - {url}: DOWNLOADING {status.progress:.2f}% ({status.downloaded_bytes / (1024*1024):.2f}MiB/{status.total_bytes / (1024*1024):.2f}MiB) Speed: {status.speed} ETA: {status.eta}")
                case StatusPending(): # In active but not yet downloading
                    output.append(f"  - {url}: PENDING (Active)")
                case _: # Fallback for other states in active (e.g. error, finished but not yet moved)
                    output.append(f"  - {url}: {status.message}")
        output.append("")

    if not daemon.download_queue.empty():
        output.append("Queued Downloads:")
        # It's not easy to list items from a queue directly without removing them.
        # A more sophisticated approach would involve a separate list for queued items.
        # For now, we'll just indicate how many are in the queue.
        output.append(f"  - {daemon.download_queue.qsize()} items in queue.")
        output.append("")

    if daemon.completed_downloads:
        output.append("Completed Downloads:")
        for url, downloader in daemon.completed_downloads.items():
            status = downloader.get_current_status()
            if isinstance(status, StatusFinished):
                output.append(f"  - {url}: FINISHED to {status.filepath}")
            elif isinstance(status, StatusError):
                output.append(f"  - {url}: ERROR: {status.message} ({status.details})")
        output.append("")

    print("\n".join(output))


if __name__ == "__main__":
    app()