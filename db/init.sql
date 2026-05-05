-- db/init.sql — Inicialización del esquema de la base de datos

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS weather_logs (
    id          UUID        PRIMARY KEY,
    station_id  VARCHAR(50) NOT NULL,
    timestamp   TIMESTAMPTZ NOT NULL,
    temperature NUMERIC(5,2) NOT NULL,
    humidity    NUMERIC(5,2) NOT NULL,
    pressure    NUMERIC(7,2) NOT NULL,
    status      VARCHAR(10)  NOT NULL CHECK (status IN ('active', 'inactive')),
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Índices para consultas frecuentes
CREATE INDEX IF NOT EXISTS idx_weather_station_id ON weather_logs (station_id);
CREATE INDEX IF NOT EXISTS idx_weather_timestamp   ON weather_logs (timestamp DESC);

-- Vista útil para monitoreo rápido
CREATE OR REPLACE VIEW latest_readings AS
SELECT DISTINCT ON (station_id)
    station_id,
    timestamp,
    temperature,
    humidity,
    pressure,
    status
FROM weather_logs
ORDER BY station_id, timestamp DESC;
