from typing import Optional, Any
import click

from mediabin import coloring
from mediabin.mediabin_daemon import MediabinDaemon


daemon = MediabinDaemon() # Instantiate MediabinDaemon

@click.group(invoke_without_command=True)
@click.pass_context
@click.option("--start-service", is_flag=True, help="Starts the mediabin daemon service.")
@click.option("--stop-service", is_flag=True, help="Stops the mediabin daemon service.")
@click.option("--restart-service", "-r", is_flag=True, help="Restarts the mediabin daemon service (stops then starts).")
@click.option("--ledger-path", default=None, help="Path to the DuckDB ledger file.")
def app(
    ctx: click.Context,
    start_service: bool,
    stop_service: bool,
    restart_service: bool,
    ledger_path: Optional[str]
):
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
    
    if start_service or restart_service:
        if daemon.is_process_running():
            print(f"Daemon is already running")
            raise click.Exit(code=1)
        print("Starting mediabin daemon service...")
        # Pass ledger_path to the spawn method
        pid = daemon.spawn(ledgerpath=ledger_path)
        print(f"Started with pid: {pid}")


@app.command("ping")
@daemon.command
def ping_command():
    print("pong")

@app.command("echo")
@daemon.command
@click.argument('msg', nargs=-1)
def echo_command(msg: Any):
    print(" ".join(msg))

@app.command("i")
@click.argument('url')
@daemon.command
def install_media(url):
    # Call the daemon's method to add the download job
    daemon.register_new_download(url=url)
    print(f"Added {url} to download queue. Check status with 'mb list'.")

@app.command("ps")
@daemon.command
def list_current_proces():
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
def list_media():
    titles = daemon.list_media()
    for title in titles:
        print(f"- {title}")

if __name__ == "__main__":
    app()