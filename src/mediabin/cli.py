import typer
import os
import sys

from mediabin.daemon import Daemon

app = typer.Typer()

daemon = Daemon()

@app.command("start-service")
def start_service():
    """
    Starts the mediabin daemon service.
    """    
    if daemon.is_process_running():
        print(f"Daemon is already running")
        return  # Exit if daemon is already running

    print("Starting mediabin daemon service...")
    pid = daemon.spawn()
    print(f"Started with pid: {pid}")


@app.command("stop-service")
def stop_service():
    """
    Stops the mediabin daemon service.
    """
    print("Stopping mediabin daemon service...")
    try:
        daemon.stop()
    except ProcessLookupError:
        print("no service to stop")


@app.command("ping")
@daemon.command(typer=True)
def ping_command():
    return "pong"


if __name__ == "__main__":
    app()