"""
app.py — Punto de entrada del consumer.
Consume mensajes de RabbitMQ, los valida y persiste en PostgreSQL.
ACK manual + prefetch_count=1 para garantizar procesamiento uno a uno.
"""

import json
import logging
import os
import signal
import time
import pika
import pika.exceptions

from db import Database
from processor import validate, process

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("consumer")


# ── Config ─────────────────────────────────────────────────────────────────────
RABBITMQ_URL  = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
QUEUE         = "weather_logs"
RETRY_DELAY   = float(os.getenv("RETRY_DELAY", "5.0"))


# ── Señal de parada ────────────────────────────────────────────────────────────
_running = True

def _stop(sig, frame):
    global _running
    log.info("Señal de parada — finalizando…")
    _running = False

signal.signal(signal.SIGINT, _stop)
try:
    signal.signal(signal.SIGTERM, _stop)
except (OSError, ValueError):
    pass

# ── Callback de mensaje ────────────────────────────────────────────────────────
def make_callback(db: Database):
    def on_message(channel, method, properties, body):
        try:
            record = json.loads(body)
        except json.JSONDecodeError as exc:
            log.error("JSON inválido: %s — descartando mensaje", exc)
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        error = validate(record)
        if error:
            log.warning("Validación fallida [%s]: %s — descartando", record.get("msg_id", "?"), error)
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        data = process(record)
        ok   = db.save(data)

        if ok:
            channel.basic_ack(delivery_tag=method.delivery_tag)
            log.info("✔ Guardado msg_id=%s station=%s temp=%.1f°C status=%s",
                     data["msg_id"], data["station_id"], data["temperature"], data["status"])
        else:
            # Requeue=True para no perder el mensaje si DB falla temporalmente
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            log.error("✘ Error al guardar msg_id=%s — reencolado", data.get("msg_id", "?"))

    return on_message


# ── Conexión RabbitMQ con reconexión ───────────────────────────────────────────
def connect_rabbit():
    while True:
        try:
            params = pika.URLParameters(RABBITMQ_URL)
            params.heartbeat = 60
            conn    = pika.BlockingConnection(params)
            channel = conn.channel()
            channel.basic_qos(prefetch_count=1)
            log.info("Conectado a RabbitMQ — escuchando queue=%s", QUEUE)
            return conn, channel
        except Exception as exc:
            log.warning("No se pudo conectar a RabbitMQ: %s — reintentando en %.0fs", exc, RETRY_DELAY)
            time.sleep(RETRY_DELAY)


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    log.info("Iniciando consumer")

    db = Database(retry_delay=RETRY_DELAY)
    db.connect()

    while _running:
        conn = channel = None
        try:
            conn, channel = connect_rabbit()
            channel.basic_consume(
                queue=QUEUE,
                on_message_callback=make_callback(db),
                auto_ack=False,
            )
            log.info("Esperando mensajes… (Ctrl+C para detener)")
            channel.start_consuming()

        except (pika.exceptions.AMQPConnectionError,
                pika.exceptions.AMQPChannelError) as exc:
            log.warning("Conexión RabbitMQ perdida: %s — reconectando…", exc)
            time.sleep(RETRY_DELAY)

        except KeyboardInterrupt:
            break

        finally:
            try:
                if channel and channel.is_open:
                    channel.stop_consuming()
                if conn and not conn.is_closed:
                    conn.close()
            except Exception:
                pass

    db.close()
    log.info("Consumer detenido.")


if __name__ == "__main__":
    main()