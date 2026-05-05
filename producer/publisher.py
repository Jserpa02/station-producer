"""
publisher.py — Publicación de mensajes en RabbitMQ.
Maneja conexión, declaración de exchange/queue y reconexión automática.
"""

import json
import logging
import time
import pika
import pika.exceptions

log = logging.getLogger("publisher")

EXCHANGE      = "weather"
EXCHANGE_TYPE = "direct"
ROUTING_KEY   = "station.data"
QUEUE         = "weather_logs"


class RabbitPublisher:
    def __init__(self, url: str, retry_delay: float = 5.0):
        self._url         = url
        self._retry_delay = retry_delay
        self._conn        = None
        self._channel     = None

    # ── Conexión ────────────────────────────────────────────────────────────────

    def connect(self) -> None:
        """Intenta conectar indefinidamente hasta lograrlo."""
        while True:
            try:
                params = pika.URLParameters(self._url)
                params.heartbeat = 60
                params.blocked_connection_timeout = 30
                self._conn    = pika.BlockingConnection(params)
                self._channel = self._conn.channel()
                self._declare_topology()
                log.info("Conectado a RabbitMQ — exchange=%s queue=%s", EXCHANGE, QUEUE)
                return
            except Exception as exc:
                log.warning("No se pudo conectar a RabbitMQ: %s — reintentando en %.0fs", exc, self._retry_delay)
                time.sleep(self._retry_delay)

    def close(self) -> None:
        try:
            if self._conn and not self._conn.is_closed:
                self._conn.close()
        except Exception:
            pass

    # ── Publicación ─────────────────────────────────────────────────────────────

    def publish(self, record: dict) -> bool:
        """
        Publica un mensaje como JSON persistente.
        Reconecta automáticamente si la conexión se perdió.
        """
        for attempt in range(1, 4):
            try:
                self._ensure_connected()
                self._channel.basic_publish(
                    exchange=EXCHANGE,
                    routing_key=ROUTING_KEY,
                    body=json.dumps(record).encode("utf-8"),
                    properties=pika.BasicProperties(
                        delivery_mode=2,          # persistent
                        content_type="application/json",
                        message_id=record["msg_id"],
                    ),
                )
                return True
            except (pika.exceptions.AMQPConnectionError,
                    pika.exceptions.AMQPChannelError,
                    AttributeError) as exc:
                log.warning("Error de publicación (intento %d/3): %s", attempt, exc)
                self._reconnect()
            except Exception as exc:
                log.error("Error inesperado publicando: %s", exc)
                return False
        return False

    # ── Interno ─────────────────────────────────────────────────────────────────

    def _declare_topology(self) -> None:
        self._channel.exchange_declare(
            exchange=EXCHANGE,
            exchange_type=EXCHANGE_TYPE,
            durable=True,
        )
        self._channel.queue_declare(
            queue=QUEUE,
            durable=True,
            arguments={"x-queue-type": "classic"},
        )
        self._channel.queue_bind(
            queue=QUEUE,
            exchange=EXCHANGE,
            routing_key=ROUTING_KEY,
        )

    def _ensure_connected(self) -> None:
        if self._conn is None or self._conn.is_closed:
            raise pika.exceptions.AMQPConnectionError("Sin conexión")

    def _reconnect(self) -> None:
        log.info("Reconectando a RabbitMQ…")
        self.close()
        time.sleep(self._retry_delay)
        self.connect()
