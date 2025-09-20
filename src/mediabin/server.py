from dataclasses import dataclass
import os
from flask import Flask, jsonify, send_from_directory, abort, current_app, g
import duckdb

@dataclass
class ServerStartOptions:
    pass

def create_app(ledgerpath: str, datadir: str):
    app = Flask(__name__)
    app.config["ledgerpath"] = ledgerpath
    app.config["datadir"] = datadir

    @app.before_request
    def open_db():
        g.db = duckdb.connect(app.config["ledgerpath"])
    
    @app.teardown_request
    def close_db(exc):
        db = getattr(g, 'db', None)
        if db:
            db.close()

    @app.get("/ping")
    def ping():
        return "pong"

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
        if not row:
            abort(404)
        
        filepath = os.path.join(row[0], "video.mp4")
        response = send_from_directory(app.config["datadir"], filepath, mimetype="video/mp4", conditional=True) 
        response.headers['Accept-Ranges'] = 'bytes'
        return response

    return app