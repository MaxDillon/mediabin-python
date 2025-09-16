from dataclasses import dataclass
import os
import socket
import sys
import signal
from typing import Optional
import pickle
import struct
from typing import Any
import threading
from functools import wraps
import inspect
import io

HOME_DIRECTORY = os.path.expanduser("~")
DAEMON_DIRECTORY = os.path.join(HOME_DIRECTORY, ".mediabin", "daemon")
os.makedirs(DAEMON_DIRECTORY, exist_ok=True)


PID_FILE = os.path.join(DAEMON_DIRECTORY, "process.pid")
SOCKET_FILE = os.path.join(DAEMON_DIRECTORY, "socket.sock")

class StreamProxy(io.TextIOBase):
    def __init__(self, conn, stream_cls):
        self.conn = conn
        self.stream_cls = stream_cls

    def write(self, s):
        send_pickle(self.conn, self.stream_cls(s))
        return len(s)

    def flush(self):
        pass  # no-op, every write is already sent

@dataclass
class Message:
    name: str
    args: list[Any]
    kwargs: dict[str, Any]

@dataclass
class StdoutMessage:
    text: str

@dataclass
class StderrMessage:
    text: str


def send_pickle(sock: socket.socket, obj):
    """Send an arbitrary Python object over a socket."""
    data = pickle.dumps(obj)
    length = struct.pack(">Q", len(data))  # 8-byte big-endian length
    sock.sendall(length)
    sock.sendall(data)


def recv_pickle(sock: socket.socket):
    """Receive an arbitrary Python object over a socket."""
    # Read 8-byte length prefix
    length_buf = b""
    while len(length_buf) < 8:
        chunk = sock.recv(8 - len(length_buf))
        if not chunk:
            raise ConnectionError("Socket closed while reading length")
        length_buf += chunk
    length = struct.unpack(">Q", length_buf)[0]

    # Read the actual pickled data
    data = b""
    while len(data) < length:
        chunk = sock.recv(min(4096, length - len(data)))
        if not chunk:
            raise ConnectionError("Socket closed while reading data")
        data += chunk

    return pickle.loads(data)


def _read_pid_file() -> Optional[int]:
    if not os.path.exists(PID_FILE):
        return None

    with open(PID_FILE, "r") as f:
        return int(f.read().strip())

def _is_running(pid: Optional[int]) -> bool:
    if pid is None:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


class Daemon:
    def __init__(self):
        self.socket = None
        self.is_daemon = False
        self._commands = {}

    @classmethod
    def current_pid(cls) -> Optional[int]:
        return _read_pid_file()

    @classmethod
    def is_process_running(cls) -> bool:
        return _is_running(cls.current_pid())

    def on_spawn(self):
        """
        Placeholder method for subclasses to initialize daemon-specific resources.
        This method is called once when the daemon process is spawned.
        Subclasses should override this method to set up any necessary resources
        that are unique to the daemon's operation (e.g., database connections, queues).
        """
        raise NotImplementedError

    def spawn(self) -> int:
        """Daemonize the current script using fork+setsid."""
        pid = os.fork()
        if pid > 0:
            # Parent: return child PID
            return pid

        # Child process
        os.setsid()  # Start a new session and detach from terminal

        # Optional: second fork to fully detach from controlling terminal
        pid2 = os.fork()
        if pid2 > 0:
            os._exit(0)  # Exit first child

        # Now in the grandchild: fully daemonized
        # Redirect stdio to a log file
        log_file_path = os.path.join(DAEMON_DIRECTORY, "log.txt")
        sys.stdout.flush()
        sys.stderr.flush()
        with open(log_file_path, "a+") as log_file:
            os.dup2(log_file.fileno(), sys.stdout.fileno())
            os.dup2(log_file.fileno(), sys.stderr.fileno())
            os.dup2(log_file.fileno(), sys.stdin.fileno())

        self.is_daemon = True
        try:
            self.on_spawn()
        except NotImplementedError:
            pass
        # Run the daemon main loop
        self.run()



    def command(self, func=None, *, typer=False):
        if func is None:
            # Allow using decorator with arguments: @daemon.command(typer=True)
            def decorator(f):
                return self.command(f, typer=typer)
            return decorator

        name = func.__name__
        if name in self._commands:
            raise ValueError(f"Command {name} already registered")
        
        self._commands[name] = func

        @wraps(func)
        def wrapper(*args, **kwargs):
            message = Message(name=name, args=list(args), kwargs=kwargs)
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
                s.connect(SOCKET_FILE)

                send_pickle(s, message)

                while True:
                    response = recv_pickle(s)

                    # Handle streamed stdout/stderr
                    if isinstance(response, StdoutMessage):
                        print(response.text, end="")
                        continue
                    elif isinstance(response, StderrMessage):
                        print(response.text, end="", file=sys.stderr)
                        continue

                    # Final result
                    if isinstance(response, Exception):
                        raise response
                    return response

        # Preserve signature for Typer
        wrapper.__signature__ = inspect.signature(func)
        return wrapper


    @classmethod
    def stop(cls) -> None:
        pid = cls.current_pid()
        if pid is None:
            return print("Daemon not running (PID file not found).")

        try:
            os.kill(pid, signal.SIGTERM)
            print(f"Sent SIGTERM to daemon with PID: {pid}")
        except ProcessLookupError:
            print("Daemon not running (Process not found).")
        finally:
            try:
                os.remove(PID_FILE)
            except OSError:
                pass

    @classmethod
    def send_message(cls, message: Message) -> Any:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
            s.connect(SOCKET_FILE)
            send_pickle(s, message)       # send message
            response = recv_pickle(s)     # wait for server response
            return response


    def _cleanup(self, signum, frame):
        print("Daemon shutting down...")
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)
        if os.path.exists(SOCKET_FILE):
            os.remove(SOCKET_FILE)
        if self.socket:
            self.socket.close()
        sys.exit(0)
    

    def handle_client(self, conn, addr):
        with conn:
            print(f"Connected by {addr}")
            while True:
                try:
                    message: Message = recv_pickle(conn)
                except ConnectionError:
                    print(f"Client disconnected: {addr}")
                    break
                except Exception as e:
                    print(f"Error reading from {addr}: {e}")
                    break

                cmd_name = message.name
                args = message.args
                kwargs = message.kwargs

                if cmd_name in self._commands:
                    try:
                        # Redirect stdout/stderr to proxy that sends immediately
                        old_stdout, old_stderr = sys.stdout, sys.stderr
                        sys.stdout = StreamProxy(conn, StdoutMessage)
                        sys.stderr = StreamProxy(conn, StderrMessage)
                        try:
                            result = self._commands[cmd_name](*args, **kwargs)
                        finally:
                            # Flush any remaining buffered output
                            sys.stdout.flush()
                            sys.stderr.flush()
                            sys.stdout = old_stdout
                            sys.stderr = old_stderr
                    except Exception as e:
                        result = e
                else:
                    result = RuntimeError(f"Unknown command: {cmd_name}")

                try:
                    send_pickle(conn, result)
                except BrokenPipeError:
                    print(f"Client disconnected before receiving response: {addr}")
                    break


    def run(self):
        # Register signal handlers
        signal.signal(signal.SIGTERM, self._cleanup)
        signal.signal(signal.SIGHUP, self._cleanup)
        signal.signal(signal.SIGINT, self._cleanup)

        # Write PID file
        with open(PID_FILE, "w") as f:
            f.write(str(os.getpid()))

        # Create and bind socket
        if os.path.exists(SOCKET_FILE):
            os.remove(SOCKET_FILE)

        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.socket.bind(SOCKET_FILE)
        self.socket.listen(5)  # allow multiple queued connections
        print(f"Daemon listening on {SOCKET_FILE}")

        try:
            while True:
                conn, addr = self.socket.accept()
                threading.Thread(target=self.handle_client, args=(conn, addr), daemon=True).start()
        finally:
            self.socket.close()
            if os.path.exists(SOCKET_FILE):
                os.remove(SOCKET_FILE)
