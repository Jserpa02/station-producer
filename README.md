
# Weather Station — Sistema distribuido de ingesta meteorológica

## Arquitectura

```
┌─────────────┐        ┌──────────────────────────────────┐        ┌─────────────┐
│             │        │           RabbitMQ                │        │             │
│  Producer   │──────▶ │  exchange: weather (direct)       │──────▶ │  Consumer   │──────▶ PostgreSQL
│  (Python)   │ AMQP   │  queue:    weather_logs (durable) │ AMQP   │  (Python)   │
│             │        │  routing:  station.data           │        │             │
└─────────────┘        └──────────────────────────────────┘        └─────────────┘
      │                          │                                        │
  genera datos             Management UI                          weather_logs
  con deriva             localhost:15672                            (PostgreSQL)
```

**Flujo:**
1. Producer genera un registro por estación cada N segundos y lo publica en RabbitMQ como mensaje persistente.
2. RabbitMQ encola los mensajes de forma durable (sobreviven a reinicios).
3. Consumer lee uno a uno (`prefetch_count=1`), valida rangos, persiste en PostgreSQL y confirma con ACK manual.

---

## Inicio rápido

```bash
# 1. Clonar / ubicarse en el directorio del proyecto
cp .env.example .env          # ajustar credenciales si es necesario

# 2. Levantar todo
docker-compose up --build

# 3. Detener
docker-compose down           # conserva volúmenes
docker-compose down -v        # elimina también los datos
```

---

## Verificación

### Logs en tiempo real
```bash
docker-compose logs -f producer   # mensajes publicados
docker-compose logs -f consumer   # mensajes recibidos y guardados
```

### RabbitMQ Management UI
Abrir en el navegador: [http://localhost:15672](http://localhost:15672)
- Usuario: `guest` / Contraseña: `guest` (o los valores del .env)
- Ver cola `weather_logs`: mensajes encolados, tasa de publicación/consumo.

### Consultar datos en PostgreSQL
```bash
# Conectarse al contenedor
docker exec -it postgres psql -U weather -d weatherdb

# Últimas lecturas por estación
SELECT * FROM latest_readings;

# Conteo total de registros
SELECT COUNT(*) FROM weather_logs;

# Histórico de una estación
SELECT timestamp, temperature, humidity, pressure, status
FROM weather_logs
WHERE station_id = 'ST-001'
ORDER BY timestamp DESC
LIMIT 20;
```

---

## Variables de entorno

| Variable            | Descripción                        | Default                                     |
|---------------------|------------------------------------|---------------------------------------------|
| `STATION_IDS`       | IDs separados por coma             | `ST-001,ST-002,ST-003`                      |
| `INTERVAL_SECONDS`  | Segundos entre ciclos              | `5.0`                                       |
| `RANDOM_SEED`       | Semilla para reproducibilidad      | _(vacío = aleatorio)_                       |
| `RABBITMQ_URL`      | URL de conexión AMQP               | `amqp://guest:guest@rabbitmq:5672/`         |
| `POSTGRES_HOST`     | Host de PostgreSQL                 | `postgres`                                  |
| `POSTGRES_DB`       | Nombre de la base de datos         | `weatherdb`                                 |
| `POSTGRES_USER`     | Usuario de PostgreSQL              | `weather`                                   |
| `POSTGRES_PASSWORD` | Contraseña de PostgreSQL           | `weather123`                                |

---

## Ejemplo de mensaje JSON

```json
{
  "msg_id": "a7fc1ee4-d1b6-4ece-8b58-39e2f81493b5",
  "station_id": "ST-002",
  "timestamp": "2026-05-04T20:12:46.865640+00:00",
  "temperature": 18.73,
  "humidity": 61.22,
  "pressure": 1012.45,
  "status": "active"
}
```

---

## Monitoreo con Prometheus/Grafana (propuesta)

El sistema está preparado para añadir métricas sin cambios mayores:

1. **Producer**: exponer un endpoint `/metrics` con `prometheus_client` contando mensajes enviados/fallidos por estación.
2. **Consumer**: métricas de mensajes procesados, rechazados y latencia de guardado en DB.
3. **RabbitMQ**: ya expone métricas en `/metrics` con el plugin `rabbitmq_prometheus` (incluido en la imagen `rabbitmq:3.13-management`).
4. **PostgreSQL**: usar `postgres_exporter` como sidecar.
5. **Grafana**: dashboards preconfigurados para RabbitMQ (ID 10991) y PostgreSQL (ID 9628) disponibles en grafana.com.

Para activarlo, añadir al `docker-compose.yml`:
```yaml
prometheus:
  image: prom/prometheus
  volumes:
    - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml

grafana:
  image: grafana/grafana
  ports:
    - "3000:3000"
```
>>>>>>> 7a872b1 (Initial commit)
