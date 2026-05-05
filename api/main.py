"""
api/main.py — API REST para exponer datos meteorológicos al dashboard.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import psycopg2.extras
import os

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

DSN = (
    f"host={os.getenv('POSTGRES_HOST', 'postgres')} "
    f"port={os.getenv('POSTGRES_PORT', '5432')} "
    f"dbname={os.getenv('POSTGRES_DB', 'weatherdb')} "
    f"user={os.getenv('POSTGRES_USER', 'weather')} "
    f"password={os.getenv('POSTGRES_PASSWORD', 'weather123')}"
)

def get_conn():
    return psycopg2.connect(DSN)

@app.get("/latest")
def latest():
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM latest_readings ORDER BY station_id")
            return list(cur.fetchall())

@app.get("/history")
def history(limit: int = 20):
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT station_id, timestamp, temperature, humidity, pressure, status
                FROM weather_logs
                ORDER BY timestamp DESC
                LIMIT %s
            """, (limit,))
            return list(cur.fetchall())

@app.get("/history/{station_id}")
def history_station(station_id: str, limit: int = 30):
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT station_id, timestamp, temperature, humidity, pressure, status
                FROM weather_logs
                WHERE station_id = %s
                ORDER BY timestamp DESC
                LIMIT %s
            """, (station_id, limit))
            rows = list(cur.fetchall())
            rows.reverse()
            return rows

@app.get("/stats")
def stats():
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT COUNT(*) as total FROM weather_logs")
            total = cur.fetchone()["total"]
            cur.execute("SELECT COUNT(DISTINCT station_id) as stations FROM weather_logs")
            stations = cur.fetchone()["stations"]
            cur.execute("SELECT COUNT(*) as active FROM latest_readings WHERE status = 'active'")
            active = cur.fetchone()["active"]
            cur.execute("SELECT AVG(temperature) as avg_temp, AVG(humidity) as avg_hum, AVG(pressure) as avg_pres FROM latest_readings")
            avgs = cur.fetchone()
            return {
                "total_records": total,
                "total_stations": stations,
                "active_stations": active,
                "avg_temperature": round(float(avgs["avg_temp"] or 0), 2),
                "avg_humidity": round(float(avgs["avg_hum"] or 0), 2),
                "avg_pressure": round(float(avgs["avg_pres"] or 0), 2),
            }
