"""
processor.py — Validación y procesamiento de mensajes meteorológicos.
Separado de la lógica de conexión (RabbitMQ y DB).
"""

import logging
from typing import Optional

log = logging.getLogger("processor")

# Rangos válidos (deben coincidir con los del generador)
_VALID_RANGES = {
    "temperature":  (-10.0,  45.0),
    "humidity":     (  0.0, 100.0),
    "pressure":     (950.0, 1050.0),
}

_REQUIRED_FIELDS = {"msg_id", "station_id", "timestamp", "temperature", "humidity", "pressure", "status"}
_VALID_STATUSES  = {"active", "inactive"}


def validate(record: dict) -> Optional[str]:
    """
    Valida un registro. Devuelve None si es válido, o un mensaje de error.
    """
    # Campos obligatorios
    missing = _REQUIRED_FIELDS - record.keys()
    if missing:
        return f"Campos faltantes: {missing}"

    # Status válido
    if record["status"] not in _VALID_STATUSES:
        return f"Status inválido: {record['status']!r}"

    # Rangos numéricos
    for field, (lo, hi) in _VALID_RANGES.items():
        val = record.get(field)
        if not isinstance(val, (int, float)):
            return f"Tipo inválido en {field}: {type(val).__name__}"
        if not (lo <= val <= hi):
            return f"{field}={val} fuera de rango [{lo}, {hi}]"

    return None


def process(record: dict) -> dict:
    """
    Normaliza y enriquece el registro antes de persistir.
    Actualmente devuelve el registro tal cual (punto de extensión).
    """
    return record
