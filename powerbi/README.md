# Power BI — NYC TLC Big Data

Abra `TLC_BigData.pbip` con Power BI Desktop. El proyecto usa el formato PBIP/PBIR
versionable y contiene 10 páginas: tres descriptivas, tres diagnósticas, tres
predictivas y una de control/auditoría.

Los diez contratos de `exports/powerbi/*.csv` se conservan para auditoría y
verificación del examen. El modelo semántico profesional consume directamente las
tablas Parquet Gold y mantiene el contrato de auditoría en CSV.

## Modelo profesional Gold

La versión final carga directamente las 15 salidas Parquet de `data/gold` como
tablas de hechos, incluidas las nueve tablas analíticas base y las seis tablas de
métricas, perfiles y validación de modelos. El flujo de auditoría continúa desde
`D10_Auditoria.csv`.

Dimensiones conformadas:

- `DimFecha`: 2023-2026, año, mes, trimestre, día de semana y fin de semana.
- `DimServicio`: yellow, green, FHV y FHVHV.
- `DimZonaOrigen` y `DimZonaDestino`: catálogo oficial TLC, dimensiones de rol.
- `DimPago`, `DimHora` y `DimCluster`.

Las relaciones se definen en `TLC_BigData.SemanticModel/definition/relationships.tmdl`.
Las medidas DAX usan sumas aditivas o promedios/tasas ponderadas según el grano;
no se suman porcentajes, z-scores ni probabilidades. Las páginas predictivas
exponen WMAPE, silhouette y AUC además de los resultados operativos.

Para regenerar este modelo y sus 10 páginas:

```powershell
python scripts/generate_powerbi_professional.py
```

Para regenerar la estructura del informe en otra ruta de Windows:

```powershell
python scripts/generate_powerbi_project.py
```

`--create-placeholders` existe únicamente para probar la apertura antes de ejecutar
el procesamiento completo. Esos registros llevan `status=PLACEHOLDER` y la
verificación final los rechaza como evidencia productiva.
