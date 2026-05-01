import oracledb
from flask import Flask
from config import Config
from portal import db as portal_db

_ORACLE_CLIENT = r"C:\Users\arham.ali\Downloads\instantclient-basic-windows.x64-23.26.1.0.0\instantclient_23_0"
try:
    oracledb.init_oracle_client(lib_dir=_ORACLE_CLIENT)
except Exception:
    pass  # already initialised (thick mode can only be set once per process)


def create_app(config_class=Config):
    app = Flask(__name__, template_folder="templates")
    app.config.from_object(config_class)

    # Bootstrap SQLite on startup
    portal_db.bootstrap(app.config["DB_PATH"])

    # ── Blueprints ────────────────────────────────────────────────────────────
    from portal.blueprints.home.routes import bp as home_bp
    from portal.blueprints.revenue_validation.routes import bp as rv_bp
    from portal.blueprints.admin.routes import bp as admin_bp
    from portal.blueprints.commissions.routes import bp as commissions_bp

    app.register_blueprint(home_bp)
    app.register_blueprint(rv_bp,          url_prefix="/revenue-validation")
    app.register_blueprint(admin_bp,        url_prefix="/admin")
    app.register_blueprint(commissions_bp,  url_prefix="/commissions")

    return app
