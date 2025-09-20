from dataclasses import dataclass
from flask import Flask, jsonify, send_file, abort, current_app, g
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

    return app