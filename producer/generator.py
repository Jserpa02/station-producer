"""
generator.py — Generación de registros simulados de estaciones meteorológicas.
Reutilizado del productor original. Valores con deriva gradual (sin saltos imposibles).
"""

import uuid
import random
from datetime import datetime, timezone
from typing import Dict, List, Optional

_RANGES = {
    "temperature":  (-10.0,  45.0),
    "humidity":     (  0.0, 100.0),
    "pressure":     (950.0, 1050.0),
}

_DELTAS = {
    "temperature": 1.5,
    "humidity":    3.0,
    "pressure":    2.0,
}

_INACTIVE_PROB = 0.05


class StationDataGenerator:
    def __init__(self, station_ids: List[str], seed: Optional[int] = None):
        self.station_ids = station_ids
        self._rng = random.Random(seed)
        self._state: Dict[str, Dict[str, float]] = {
            sid: {var: self._rng.uniform(*_RANGES[var]) for var in _RANGES}
            for sid in station_ids
        }

    def generate_all(self) -> List[dict]:
        return [self._generate_one(sid) for sid in self.station_ids]

    def _generate_one(self, station_id: str) -> dict:
        status = "inactive" if self._rng.random() < _INACTIVE_PROB else "active"
        prev = self._state[station_id]
        new_vals = {var: self._drift(var, prev[var]) for var in _RANGES}
        self._state[station_id] = new_vals
        return {
            "msg_id":      str(uuid.uuid4()),
            "station_id":  station_id,
            "timestamp":   datetime.now(timezone.utc).isoformat(),
            "temperature": new_vals["temperature"],
            "humidity":    new_vals["humidity"],
            "pressure":    new_vals["pressure"],
            "status":      status,
        }

    def _drift(self, variable: str, previous: float) -> float:
        delta = self._rng.uniform(-_DELTAS[variable], _DELTAS[variable])
        lo, hi = _RANGES[variable]
        return round(max(lo, min(hi, previous + delta)), 2)
