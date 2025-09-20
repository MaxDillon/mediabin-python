import sys
from typing import Optional
import click
import os
import shutil

from mediabin import coloring
from mediabin.mediabin_daemon import MediabinDaemon, ServerStartOptions
from mediabin.daemon import DaemonConnectionError

def format_bytes(size: int) -> str:
    # 2**10 = 1024
    power = 2**10
    n = 0
    power_labels = {0: '', 1: 'K', 2: 'M', 3: 'G', 4: 'T'}
    while size > power:
        size /= power
        n += 1
    return f"{size:.2f} {power_labels[n]}B"

daemon = MediabinDaemon() # Instantiate MediabinDaemon

@click.group(invoke_without_command=True)
@click.pass_context
@click.option("--start-service", is_flag=True, help="Starts the mediabin daemon service.")
@click.option("--stop-service", is_flag=True, help="Stops the mediabin daemon service.")
@click.option("--restart-service", "-r", is_flag=True, help="Restarts the mediabin daemon service (stops then starts).")
@click.option("--ledger-path", default=None, help="Path to the DuckDB ledger file.")
@click.option("--serve", is_flag=True, help="Whether to start the media server")
@click.option("--port", default=80, help="Whether to start the media server")
@click.option("--tailscale", is_flag=True, help="Whether to automatically forward to your tailnet on serve")
def app(
    ctx: click.Context,
    start_service: bool,
    stop_service: bool,
    restart_service: bool,
    ledger_path: Optional[str],
    serve: bool,
    port: int,
    tailscale: bool
):
    """Mediabin CLI for managing media downloads and daemon service."""
    if ctx.invoked_subcommand is not None:
        return
    service_options = [start_service, stop_service, restart_service]
    if sum(service_options) > 1:
        raise click.BadParameter("Cannot specify more than one of --start-service, --stop-service, or --restart-service.")
    elif sum(service_options) == 0:
        click.echo(ctx.get_help())  # reuse the current ctx here

    if stop_service or restart_service:
        print("Stopping mediabin daemon service...")
        try:
            daemon.stop()
        except ProcessLookupError:
            print("no service to stop")

    server_options = None
    if serve:
        if tailscale:
            if not shutil.which("tailscale"):
                print(coloring.error("tailscale is not in your PATH. Please install it or ensure it's accessible."))
                raise exit(1)
        server_options = ServerStartOptions(tailscale=tailscale, port=port)

    
    if start_service or restart_service:
        if daemon.is_process_running():
            print(f"Daemon is already running")
            raise exit(1)
        print("Starting mediabin daemon service...")
        # Pass ledger_path to the spawn method
        pid = daemon.spawn(ledgerpath=ledger_path, server_options=server_options)
        print(f"Started with pid: {pid}")


@app.command("du")
@daemon.command
def get_available_disk_space():
    """Gets disk space used and available for new media"""
    datadir = daemon.get_datadir_location()
    if not datadir:
        print(coloring.error("Datadir location not set. Please run 'mediabin init' first."))
        return

    if not os.path.exists(datadir):
        print(coloring.error(f"Datadir location '{datadir}' does not exist."))
        return

    total, used, free = shutil.disk_usage(datadir)

    print(f"Disk space for media directory: {datadir}")
    print(f"  Total: {format_bytes(total)}")
    print(f"  Used:  {format_bytes(used)}")
    print(f"  Free:  {format_bytes(free)}")


@app.command("i")
@click.argument('url')
@daemon.command
def install_media(url):
    """Adds a URL to the download queue."""
    # Call the daemon's method to add the download job
    daemon.register_new_download(url=url)
    print(f"Added {url} to download queue. Check status with 'mb list'.")

@app.command("ps")
@daemon.command
def list_current_proces():
    """Lists current and pending download processes."""
    procs = daemon.list_current_procs()
    for title, status in procs.current_jobs:
        if status.progress < 30:
            color = coloring.RED
        elif status.progress < 60:
            color = coloring.YELLOW
        else:
            color = coloring.GREEN 
        print(f"[{color}{status.progress:6.2f}%{coloring.RESET}] {title}")

    for title, _ in procs.pending_jobs:
        print(f"[{coloring.MEDIUM_GRAY}pending{coloring.RESET}] {title}")


@app.command("ls")
@daemon.command
@click.option("--query", "-q",type=str, default=None, help="Filter by title (case-insensitive, partial match)")
@click.option("--tag", "-t", multiple=True, help="Filter by tags (can be specified multiple times)")
def list_media(query: str | None, tag: list[str]) -> None:
    """Lists all downloaded media."""
    titles = daemon.list_media(title_like=query, tags=list(tag))
    for title in titles:
        print(f"- {title}")


def main():
    try:
        app()
    except DaemonConnectionError:
        print("Cannot connect to Daemon")
        exit(1)

if __name__ == "__main__":
    main()
