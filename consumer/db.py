"""
db.py — Conexión a PostgreSQL y persistencia de registros meteorológicos.
Maneja reconexión automática ante fallos de red.
"""

import logging
import time
import os
import psycopg2
import psycopg2.extras
import psycopg2.extensions

log = logging.getLogger("db")

DSN = (
    f"host={os.getenv('POSTGRES_HOST', 'postgres')} "
    f"port={os.getenv('POSTGRES_PORT', '5432')} "
    f"dbname={os.getenv('POSTGRES_DB', 'weatherdb')} "
    f"user={os.getenv('POSTGRES_USER', 'weather')} "
    f"password={os.getenv('POSTGRES_PASSWORD', 'weather123')}"
)

INSERT_SQL = """
    INSERT INTO weather_logs (id, station_id, timestamp, temperature, humidity, pressure, status)
    VALUES (%(msg_id)s, %(station_id)s, %(timestamp)s, %(temperature)s, %(humidity)s, %(pressure)s, %(status)s)
    ON CONFLICT (id) DO NOTHING;
"""


class Database:
    def __init__(self, retry_delay: float = 5.0):
        self._retry_delay = retry_delay
        self._conn: psycopg2.extensions.connection = None

    def connect(self) -> None:
        """Conecta indefinidamente hasta lograrlo."""
        while True:
            try:
                self._conn = psycopg2.connect(DSN)
                self._conn.autocommit = False
                log.info("Conectado a PostgreSQL — %s", os.getenv("POSTGRES_HOST", "postgres"))
                return
            except psycopg2.OperationalError as exc:
                log.warning("No se pudo conectar a PostgreSQL: %s — reintentando en %.0fs", exc, self._retry_delay)
                time.sleep(self._retry_delay)

    def save(self, record: dict) -> bool:
        """
        Persiste un registro. Reconecta si la conexión se cerró.
        Devuelve True si tuvo éxito.
        """
        for attempt in range(1, 4):
            try:
                self._ensure_connected()
                with self._conn.cursor() as cur:
                    cur.execute(INSERT_SQL, record)
                self._conn.commit()
                return True
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
                log.warning("Error DB (intento %d/3): %s", attempt, exc)
                self._reconnect()
            except psycopg2.Error as exc:
                log.error("Error SQL: %s", exc)
                self._conn.rollback()
                return False
        return False

    def close(self) -> None:
        try:
            if self._conn and not self._conn.closed:
                self._conn.close()
        except Exception:
            pass

    def _ensure_connected(self) -> None:
        if self._conn is None or self._conn.closed:
            raise psycopg2.OperationalError("Sin conexión")

    def _reconnect(self) -> None:
        log.info("Reconectando a PostgreSQL…")
        self.close()
        time.sleep(self._retry_delay)
        self.connect()
