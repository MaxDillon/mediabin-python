from typing import Optional, Any
import typer

from mediabin import coloring
from mediabin.mediabin_daemon import MediabinDaemon

app = typer.Typer()

daemon = MediabinDaemon() # Instantiate MediabinDaemon

@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    start_service: bool = typer.Option(False, "--start-service", help="Starts the mediabin daemon service."),
    stop_service: bool = typer.Option(False, "--stop-service", help="Stops the mediabin daemon service."),
    restart_service: bool = typer.Option(False, "--restart-service", "-r",  help="Restarts the mediabin daemon service (stops then starts)."),
    ledger_path: Optional[str] = typer.Option(None, "--ledger-path", help="Path to the DuckDB ledger file."),
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
        # Pass ledger_path to the spawn method
        pid = daemon.spawn(ledgerpath=ledger_path)
        print(f"Started with pid: {pid}")


@app.command("ping")
@daemon.command
def ping_command():
    print("pong")

@app.command("echo")
@daemon.command
def echo_command(msg: list[str]):
    print(" ".join(msg))

@app.command("install")
@app.command("i")
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
    daemon.list_media()

if __name__ == "__main__":
    app()