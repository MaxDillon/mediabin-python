import time
from typing import Sequence, Dict
import typer

from mediabin.daemon import Daemon
from mediabin.ytdlp_downloader.downloader import YTDLPDownloader, DownloadOptions, DownloadCurrentStatus, StatusDownloading, StatusFinished, StatusError

class MyDaemon(Daemon):
    def on_spawn(self, ):
        self.active_downloads: Dict[str, YTDLPDownloader] = {}


app = typer.Typer()

daemon = MyDaemon()

@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    start_service: bool = typer.Option(False, "--start-service", help="Starts the mediabin daemon service."),
    stop_service: bool = typer.Option(False, "--stop-service", help="Stops the mediabin daemon service."),
    restart_service: bool = typer.Option(False, "--restart-service", help="Restarts the mediabin daemon service (stops then starts)."),
):
    service_options = [start_service, stop_service, restart_service]
    if sum(service_options) > 1:
        raise typer.BadParameter("Cannot specify more than one of --start-service, --stop-service, or --restart-service.")
    elif sum(service_options) == 0:
        if ctx.invoked_subcommand is None:
            raise typer.BadParameter("Missing option: One of --start-service, --stop-service, or --restart-service")

    if start_service:
        if daemon.is_process_running():
            print(f"Daemon is already running")
            raise typer.Exit(code=1)
        print("Starting mediabin daemon service...")
        pid = daemon.spawn()
        print(f"Started with pid: {pid}")
    elif stop_service:
        print("Stopping mediabin daemon service...")
        try:
            daemon.stop()
        except ProcessLookupError:
            print("no service to stop")
    elif restart_service:
        print("Restarting mediabin daemon service...")
        try:
            daemon.stop()
            print("Daemon stopped.")
        except ProcessLookupError:
            print("No existing daemon to stop, proceeding with start.")
        
        if daemon.is_process_running(): # Check if it's still running for some reason
            print(f"Daemon is still running after stop attempt. Cannot restart.")
            raise typer.Exit(code=1)

        pid = daemon.spawn()
        print(f"Started with pid: {pid}")


@app.command("ping")
@daemon.command
def ping_command():
    for i in range(4):
        print(i)
        time.sleep(1)
    print("pong")

@app.command("install")
@app.command("i")
@daemon.command
def install_media(url: str = typer.Argument(..., help="The URL of the media to download.")):
    if url in daemon.active_downloads:
        print(f"Download for {url} is already active or in queue.")

    options = DownloadOptions(url=url)
    downloader = YTDLPDownloader(options)
    daemon.active_downloads[url] = downloader
    downloader.start_download()
    print(f"Started download for {url}. Check status with 'mb list'.")

@app.command("list")
@app.command("ls")
@daemon.command
def list_downloads():
    if not daemon.active_downloads:
        return "No active downloads."

    output = ["Active Downloads:"]
    for url, downloader in daemon.active_downloads.items():
        status = downloader.get_current_status()
        match status:
            case StatusDownloading():
                output.append(f"  - {url}: DOWNLOADING {status.progress:.2f}% ({status.downloaded_bytes / (1024*1024):.2f}MiB/{status.total_bytes / (1024*1024):.2f}MiB) Speed: {status.speed} ETA: {status.eta}")
            case StatusFinished():
                output.append(f"  - {url}: FINISHED to {status.filepath}")
            case StatusError():
                output.append(f"  - {url}: ERROR: {status.message} ({status.details})")
    print("\n".join(output))


if __name__ == "__main__":
    app()