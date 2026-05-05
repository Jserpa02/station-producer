"""
app.py — Punto de entrada del productor.
Orquesta generación de datos y publicación en RabbitMQ.
"""

import logging
import os
import signal
import time
from typing import Optional

from generator import StationDataGenerator
from publisher import RabbitPublisher

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("producer")


# ── Config desde entorno ───────────────────────────────────────────────────────
def _cfg(key: str, default: str) -> str:
    return os.getenv(key, default)

STATION_IDS      = [s.strip() for s in _cfg("STATION_IDS", "ST-001,ST-002,ST-003").split(",")]
INTERVAL_SECONDS = float(_cfg("INTERVAL_SECONDS", "5.0"))
RABBITMQ_URL     = _cfg("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
RANDOM_SEED: Optional[int] = int(s) if (s := os.getenv("RANDOM_SEED")) else None


# ── Señal de parada ────────────────────────────────────────────────────────────
_running = True

def _stop(sig, frame):
    global _running
    log.info("Señal de parada recibida — finalizando…")
    _running = False

signal.signal(signal.SIGINT,  _stop)
signal.signal(signal.SIGTERM, _stop)


# ── Loop principal ─────────────────────────────────────────────────────────────
def main():
    log.info("Iniciando productor | estaciones=%s intervalo=%.1fs", STATION_IDS, INTERVAL_SECONDS)

    generator = StationDataGenerator(station_ids=STATION_IDS, seed=RANDOM_SEED)
    publisher = RabbitPublisher(url=RABBITMQ_URL)
    publisher.connect()

    sent = errors = 0
    try:
        while _running:
            for record in generator.generate_all():
                if not _running:
                    break
                ok = publisher.publish(record)
                if ok:
                    sent += 1
                    log.info("✔ msg_id=%s station=%s temp=%.1f°C hum=%.1f%% pres=%.1fhPa status=%s",
                             record["msg_id"], record["station_id"],
                             record["temperature"], record["humidity"],
                             record["pressure"], record["status"])
                else:
                    errors += 1
                    log.error("✘ Falló envío msg_id=%s station=%s", record["msg_id"], record["station_id"])

            if _running:
                time.sleep(INTERVAL_SECONDS)
    finally:
        publisher.close()
        log.info("Productor detenido — enviados=%d errores=%d", sent, errors)


if __name__ == "__main__":
    main()
