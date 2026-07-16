# Arquitectura y decisiones

## Patrón seleccionado

Se usa arquitectura **Medallion**. Es apropiada para un lote histórico grande y una
carga incremental anual: preserva la fuente, separa calidad de negocio y evita que
Power BI lea directamente cientos de Parquet heterogéneos. El pipeline es idempotente
y puede programarse; no requiere una capa de streaming para el ritmo mensual de TLC.

| Capa | Entrada | Responsabilidad | Salida |
|---|---|---|---|
| Catálogo | Página y CDN oficiales | Descubrir, comprobar disponibilidad/tamaño y exigir 2023–2025 completo | `_catalog.json` |
| Bronze | Parquet/CSV oficiales | Descarga reanudable, preservación exacta, hash, sidecar y manifiesto | Archivos particionados |
| Silver | Bronze | Esquema canónico, normalización, enriquecimiento de zonas, DQ y cuarentena | Parquet válidos + rechazados |
| Gold | Silver válido | Agregaciones descriptivas, diagnósticas y bases ML | Tablas Parquet y CSV |
| Serving | Gold + ML + auditoría | Contratos estables y modelo semántico | PBIP de 10 páginas |

## Flujo de auditoría

Cada comando abre un `run_id` y registra inicio, parámetros, estado y métricas. Cada
archivo atraviesa estados DISCOVERED, DOWNLOADING, VALIDATED/SKIPPED o FAILED. Las
reglas de calidad registran conteos y reconciliación. Cada modelo registra parámetros,
métricas, rutas de artefactos y estado. MongoDB es el registro operacional; JSONL es
un respaldo legible y exportable incluso si Mongo no está disponible temporalmente.

## Tolerancia a fallos

- Reintentos HTTP con backoff para 408/425/429/5xx.
- Descarga por bloques y continuación con `Range` desde `.part`.
- Renombrado atómico sólo después de validar tamaño, Parquet y SHA-256.
- Manifiesto y catálogo escritos atómicamente.
- Procesamiento Silver por archivo para acotar memoria y conservar trazabilidad.
- Hasta cuatro archivos Silver concurrentes en una misma sesión Spark para aprovechar
  núcleos ociosos sin perder la reconciliación global determinista.
- Cuarentena en vez de descartar filas inválidas.
- Particiones temporales, no aleatorias, en modelos de pronóstico/clasificación.

## Particionado

Bronze y Silver se organizan por `service/year/month`. Gold se guarda por tabla de
consumo. Esta estrategia permite reejecutar un mes, aplicar poda de particiones y
mantener un lineage directo desde un KPI hasta el archivo fuente. Silver/Gold usan
compresión Parquet Zstandard para reducir el espacio de almacenamiento sin perder
filas ni precisión.

## Modelo analítico de Gold

Gold implementa una **constelación de hechos (galaxy schema)** formada por nueve
marts analíticos. No es una única estrella física: varios hechos comparten las
mismas dimensiones conformadas lógicas, pero estas se materializan dentro de cada
tabla para que Power BI consuma contratos desnormalizados y no tenga que unir más
de mil millones de filas Silver.

| Tipo | Tabla Gold | Grano principal |
|---|---|---|
| Hecho descriptivo | `descriptive_daily_demand` | servicio, fecha y zona |
| Hecho descriptivo | `descriptive_hourly_profile` | servicio, fecha/hora y zona |
| Hecho descriptivo | `descriptive_service_financials` | servicio y periodo |
| Hecho diagnóstico | `diagnostic_route_performance` | origen, destino, servicio y periodo |
| Hecho diagnóstico | `diagnostic_tip_factors` | factores de servicio, pago y ubicación |
| Hecho diagnóstico | `diagnostic_daily_anomalies` | servicio, fecha y zona |
| Hecho predictivo | `model_timeseries_daily` | serie diaria de servicio |
| Hecho predictivo | `model_segmentation_zones` | zona y segmento |
| Hecho predictivo | `model_classification_demand` | observación temporal y clase de demanda |

Las dimensiones conformadas lógicas son fecha, hora, servicio TLC, zona de origen,
zona de destino, borough, tipo de pago, ruta, segmento y estado del modelo. Sus
atributos viajan en los hechos Gold; por eso no existen tablas físicas separadas
`dim_date`, `dim_zone` o `dim_service`. Esta decisión conserva el patrón de
constelación semántica y simplifica la actualización del modelo Power BI.
