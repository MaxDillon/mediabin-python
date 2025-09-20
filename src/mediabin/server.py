from dataclasses import dataclass
import os
from flask import Flask, jsonify, send_from_directory, abort, current_app, g
import duckdb

@dataclass
class ServerStartOptions:
    pass

def get_datadir(conn: duckdb.DuckDBPyConnection) -> str | None:
    res = conn.execute("SELECT datadir_location from metadata").fetchone()
    return res[0] if res else None


def create_app(ledgerpath: str):
    app = Flask(__name__)

    @app.before_request
    def open_db():
        g.db = duckdb.connect(ledgerpath)
    
    @app.teardown_request
    def close_db(exc):
        db = getattr(g, 'db', None)
        if db:
            db.close()

    @app.get("/media/list")
    def list_media():
        rows = g.db.execute(
            "SELECT id, title FROM media.media WHERE status='complete' ORDER BY timestamp_updated DESC, timestamp_installed DESC, title ASC"
        ).fetchall()
        return jsonify(items=[{"id": r[0], "title": r[1]} for r in rows])

    @app.get("/media/play/<mid>")
    def play(mid: str):
        row = g.db.execute(
            "SELECT object_path FROM media.media WHERE id=? AND status='complete'", (mid,)
        ).fetchone()
        datadir = get_datadir()

        if not row or not datadir:
            abort(404)
 
        filepath = os.path.join(row[0], "video.mp4")
        response = send_from_directory(datadir, filepath, mimetype="video/mp4", conditional=True) 
        response.headers['Accept-Ranges'] = 'bytes'
        return response

    return app