# init_db.py
import argparse
import os
import sqlite3
from pathlib import Path

def pick_default_paths():
    base = Path(__file__).resolve().parent

    schema_candidates = [
        base / "schema.sql",
        base / "EV_Central" / "schema.sql",
    ]
    db_candidates = [
        base / "evcentral.db",
        base / "EV_Central" / "evcentral.db",
    ]

    schema_path = next((p for p in schema_candidates if p.exists()), schema_candidates[-1])
    db_path = next((p for p in db_candidates if p.exists()), db_candidates[-1])

    return schema_path, db_path

def table_columns(con: sqlite3.Connection, table: str) -> set[str]:
    rows = con.execute(f"PRAGMA table_info({table})").fetchall()
    return {r[1] for r in rows}  # r[1] = name

def ensure_column(con: sqlite3.Connection, table: str, column: str, ddl: str):
    cols = table_columns(con, table)
    if column in cols:
        return
    con.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")

def main():
    default_schema, default_db = pick_default_paths()

    parser = argparse.ArgumentParser(description="Inicializa la BD SQLite de EVCharging (Central/Registry).")
    parser.add_argument("--db", default=os.getenv("EV_DB_PATH", str(default_db)),
                        help="Ruta al fichero .db (por defecto EV_DB_PATH o evcentral.db)")
    parser.add_argument("--schema", default=str(default_schema),
                        help="Ruta al schema.sql (por defecto schema.sql detectado)")
    parser.add_argument("--reset", action="store_true",
                        help="Borra la DB antes de crearla (pierdes datos)")
    args = parser.parse_args()

    db_path = Path(args.db).resolve()
    schema_path = Path(args.schema).resolve()

    if not schema_path.exists():
        raise SystemExit(f"[ERROR] No encuentro schema.sql en: {schema_path}")

    db_path.parent.mkdir(parents=True, exist_ok=True)

    if args.reset and db_path.exists():
        db_path.unlink()

    con = sqlite3.connect(str(db_path))
    try:
        con.execute("PRAGMA foreign_keys = ON;")

        # 1) Carga schema.sql (tablas principales + triggers)
        schema_sql = schema_path.read_text(encoding="utf-8")
        con.executescript(schema_sql)

        # 2) Tablas de auth (Central/Registry)
        con.executescript("""
        CREATE TABLE IF NOT EXISTS cp_registry_credentials (
            cp_id TEXT PRIMARY KEY,
            cred_hash TEXT NOT NULL,
            salt TEXT NOT NULL,
            issued_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            revoked INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_cp_registry_revoked
        ON cp_registry_credentials(revoked);

        CREATE TABLE IF NOT EXISTS cp_central_keys (
            cp_id TEXT PRIMARY KEY,
            secret_key TEXT NOT NULL,
            issued_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            revoked INTEGER NOT NULL DEFAULT 0
        );
        """)

        # 3) Migración: si charging_points existe sin columna ip, añádela
        #    (por si tu DB vieja era de antes)
        tables = {r[0] for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        if "charging_points" in tables:
            # ip como TEXT (si tu schema ya la trae, no hace nada)
            ensure_column(con, "charging_points", "ip", "ip TEXT")

        con.commit()
        print("[OK] DB inicializada correctamente:")
        print("     DB     =", db_path)
        print("     schema =", schema_path)

    finally:
        con.close()

if __name__ == "__main__":
    main()
