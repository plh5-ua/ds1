PRAGMA foreign_keys = ON;



-- Tabla de puntos de recarga (Charging Points)
CREATE TABLE IF NOT EXISTS charging_points (
    id TEXT PRIMARY KEY,
    location TEXT NOT NULL,
    price_eur_kwh REAL NOT NULL CHECK (price_eur_kwh >= 0),
    status TEXT NOT NULL DEFAULT 'DESCONECTADO', -- ACTIVADO, PARADO, SUMINISTRANDO, AVERIADO, DESCONECTADO
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Tabla de conductores (Drivers)
CREATE TABLE IF NOT EXISTS drivers (
    id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL
);

-- Tabla de sesiones de carga
CREATE TABLE IF NOT EXISTS sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cp_id TEXT NOT NULL REFERENCES charging_points(id),
    driver_id TEXT NOT NULL REFERENCES drivers(id),
    started_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ended_at DATETIME,
    price_eur_kwh REAL NOT NULL,
    kwh REAL NOT NULL DEFAULT 0,
    amount_eur REAL NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'RUNNING' -- RUNNING | ENDED | ABORTED | FAILED
);

-- Tabla de eventos (histórico)
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    cp_id TEXT,
    driver_id TEXT,
    type TEXT NOT NULL,   -- HEARTBEAT | STATUS | TELEMETRY | COMMAND | AUTH | ERROR
    payload TEXT NOT NULL
);

-- Índices para mejorar búsquedas
CREATE INDEX IF NOT EXISTS idx_sessions_cp ON sessions(cp_id);
CREATE INDEX IF NOT EXISTS idx_events_cp ON events(cp_id);
