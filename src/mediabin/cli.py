import time
import typer

from mediabin.daemon import Daemon

app = typer.Typer()

daemon = Daemon()

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

resources = []

@app.command("add")
@daemon.command
def add_resource(resource: str):
    resources.append(resource)
    return "ok"

@app.command("list")
@daemon.command
def list_resource():
    for resource in resources:
        print(resource)



if __name__ == "__main__":
    app()