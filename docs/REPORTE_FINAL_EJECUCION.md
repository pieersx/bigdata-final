# Reporte final de ejecución — TLC Trip Record Data

**Fecha de cierre:** 16 de julio de 2026  
**Estado:** aprobado por la verificación automatizada y por revisión visual en Power BI Desktop.

## Resultado ejecutivo

El proyecto implementa una arquitectura Medallion completa con ingesta oficial TLC,
reconciliación de calidad, nueve tablas Gold, tres modelos de PySpark ML, auditoría
persistida en MongoDB y diez páginas Power BI. La ejecución final terminó con 28
pruebas aprobadas, lint sin observaciones y `tlc-pipeline verify` con cero fallos.

## Cobertura y reconciliación

| Control | Resultado |
|---|---:|
| Archivos históricos 2023–2025 | 144 de 144 |
| Archivos 2026 descubiertos y cargados | 19 |
| Archivos de viajes procesados por Silver | 163 |
| Filas fuente reconciliadas | 1.037.910.841 |
| Filas Silver válidas | 982.656.023 |
| Filas en cuarentena | 55.254.818 |
| Diferencia de reconciliación | 0 |
| Bytes históricos Bronze verificados por SHA-256 | 20.418.618.240 |

Bronze conserva los binarios originales, hashes, sidecars y manifiesto. Silver aplica
el esquema canónico, normalización, enriquecimiento de zonas, reglas de calidad y
cuarentena sin descartar silenciosamente registros.

## Modelo Gold y constelación

Gold está completo y contiene nueve hechos/marts:

| Grupo | Tabla Gold | Filas/resultados auditados |
|---|---|---:|
| Descriptivo | `descriptive_daily_demand` | 1.054.951 |
| Descriptivo | `descriptive_hourly_profile` | 16.008.952 |
| Descriptivo | `descriptive_service_financials` | 523 |
| Diagnóstico | `diagnostic_route_performance` | 5.290.128 en 41 particiones mensuales |
| Diagnóstico | `diagnostic_tip_factors` | 348.931 |
| Diagnóstico | `diagnostic_daily_anomalies` | 1.054.951 |
| Predictivo | `model_timeseries_daily` | 4.957 |
| Predictivo | `model_segmentation_zones` | 1.049 |
| Predictivo | `model_classification_demand` | 16.008.952 |

El modelo es una **constelación de hechos (galaxy schema)** y no una sola estrella
física. Los hechos comparten dimensiones conformadas lógicas de fecha, hora,
servicio, zona de origen/destino, borough, pago, ruta, segmento y estado. Para el
serving de Power BI estas dimensiones están desnormalizadas dentro de cada contrato;
por ello no hay tablas físicas separadas `dim_date`, `dim_zone` o `dim_service`.

## Modelos persistidos

| Modelo | Resultado principal | Validación |
|---|---|---|
| GBT de series de tiempo | RMSE 35.302,02; MAE 14.267,37; R² 0,98557; WMAPE 6,7312 % | 4.485 entrenamiento, 360 prueba, horizonte 30 días |
| K-Means de zonas | k=4; silhouette 0,68415 | 264 zonas, 4 clústeres producidos |
| Random Forest | accuracy 0,97041; F1 0,97079; AUC ROC 0,99787 | 297.333 entrenamiento, 23.760 prueba, 100 árboles |

Los artefactos se encuentran en `artifacts/models/time_series_forecast`,
`artifacts/models/zone_segmentation` y `artifacts/models/high_demand_classifier`.

## Power BI

Se generaron diez contratos reales sin `PLACEHOLDER` y el PBIP se actualizó y guardó
en Power BI Desktop. La revisión visual recorrió las páginas 1 a 10; todas mostraron
datos y Power BI indicó **Problemas 0**.

| Página/contrato | Filas | Tamaño aproximado |
|---|---:|---:|
| D01 Resumen ejecutivo | 1.054.951 | 116,42 MB |
| D02 Demanda temporal | 16.008.952 | 1.817,82 MB |
| D03 Ingresos y tarifas | 523 | 0,07 MB |
| D04 Causas del cambio | 1.054.951 | 156,17 MB |
| D05 Rutas y congestión | 5.290.128 | 797,21 MB |
| D06 Propinas y anomalías | 348.931 | 40,80 MB |
| D07 Pronóstico de demanda | 120 | 0,02 MB |
| D08 Segmentación de zonas | 264 | 0,04 MB |
| D09 Clasificación de alta demanda | 23.760 | 3,35 MB |
| D10 Control y auditoría | 1.760 | 0,25 MB |

Cada página contiene seis visuales; el proyecto totaliza 60 visuales. El generador
usa rutas absolutas Windows para que la actualización funcione aunque se regenere el
PBIP desde Docker.

## Auditoría y pruebas

- MongoDB mantiene eventos de catálogo, página fuente, estado de archivo, ejecución,
  calidad y modelos.
- El export final contiene 1.771 eventos en CSV y JSON.
- `ruff check src tests scripts`: aprobado.
- `pytest -q`: **28 passed** en 120,67 s.
- `tlc-pipeline verify`: **8 controles aprobados, 0 fallos**; reporte generado el
  16-07-2026 a las 18:47:32 UTC.
- Verificación adicional: nombres de medidas Power BI globalmente únicos, rutas de
  contratos absolutas y cero apariciones de `PLACEHOLDER` en los diez CSV.

## Artefactos de entrega

- Proyecto Power BI: `powerbi/TLC_BigData.pbip`
- Reporte de verificación: `exports/verification_report.json`
- Auditoría: `exports/audit_events.csv` y `exports/audit_events.json`
- Contratos Power BI: `exports/powerbi/`
- Modelos: `artifacts/models/`
- Arquitectura: `docs/architecture.md`

## Criterio final

Se cumple el criterio: nueve tablas Gold, tres modelos válidos, diez dashboards con
datos reales, auditoría completa y verificación sin fallos.
