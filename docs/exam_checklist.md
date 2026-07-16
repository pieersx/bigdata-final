# Checklist de entrega y defensa

## Evidencia automática

1. `tlc-pipeline catalog`: debe informar 144 históricos y los 2026 publicados.
2. `tlc-pipeline ingest`: debe informar descargas/omisiones y lookup de zonas validado.
3. `tlc-pipeline silver`: debe devolver `reconciled=true` y conteos completos.
4. `tlc-pipeline gold`: debe materializar las nueve tablas base.
5. `tlc-pipeline models`: debe persistir tres modelos y sus métricas.
6. `tlc-pipeline audit-export`: debe producir `exports/audit_events.csv`.
7. `tlc-pipeline verify`: debe finalizar sin checks fallidos ni placeholders.
8. `pytest -q`: todas las pruebas deben pasar.

## Evidencia visual

- Abrir `powerbi/TLC_BigData.pbip`.
- Actualizar y comprobar que no aparezca `PLACEHOLDER`.
- Recorrer las páginas 01–10 y verificar filtros de año/servicio.
- Mostrar en la página 10 los runs, archivos, calidad y modelos.
- En MongoDB, mostrar las cuatro colecciones de `tlc_audit`.

## Correspondencia con la rúbrica

- Medallion y lineage: `docs/architecture.md` y carpetas Bronze/Silver/Gold.
- Pipeline automático 2026: descubrimiento desde HTML oficial y sondeo del CDN.
- Auditoría: MongoDB, JSONL, sidecars, manifiesto y dashboard 10.
- PySpark/MongoDB: código en `src/tlc_pipeline` y servicios de `compose.yaml`.
- 3 + 3 + 3 + 1 dashboards: catálogo y PBIP versionable.
- Integridad: SHA-256, tamaños remotos, magic bytes, cuarentena y reconciliación.
- Predictivo: partición temporal, semilla fija, métricas y artefactos Spark ML.

## Limpieza antes de entregar

No subir `data/`, `logs/`, `artifacts/` ni exportaciones pesadas al repositorio; están
ignorados por Git. Entregar el código, documentación, PBIP y un archivo de evidencia
con los resultados de `verify` y `pytest`. Si se requiere una copia portable, comprimir
los datos por separado porque el corpus completo ocupa decenas de GB.
