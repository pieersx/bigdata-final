# Guía de notebooks para la exposición

Los siete notebooks están comentados y fueron ejecutados de principio a fin. Cada uno
incluye **Objetivo**, **Preparación**, **Pasos**, **Comprobaciones**, **QUÉ EXPLICAR** y
**Siguiente paso**. Las muestras visibles sirven para explicar el esquema; los conteos y
modelos provienen del corpus completo.

## Orden recomendado

1. `00_ingesta_automatica_2026.ipynb`: descubrimiento automático y comandos seguros.
2. `01_capa_bronze.ipynb`: originales, catálogo, sidecars y SHA-256.
3. `02_capa_silver.ipynb`: esquema PySpark, calidad y reconciliación.
4. `03_capa_gold.ipynb`: nueve tablas base, auxiliares y constelación.
5. `04_modelos_predictivos.ipynb`: GBT, K-Means y Random Forest.
6. `05_flujo_auditoria.ipynb`: MongoDB, JSONL y exportaciones.
7. `06_power_bi.ipynb`: 15 hechos, 7 dimensiones, relaciones y diez páginas.

## Ensayo de la automatización 2026

Desde PowerShell, en la raíz del proyecto:

```powershell
./scripts/run_2026_only.ps1
```

Ese modo es una demostración: imprime los pasos y no modifica datos. En la exposición:

```powershell
./scripts/run_2026_only.ps1 -Mode Execute
```

El script limita catálogo, descarga y Silver a `--years 2026`. Después reconstruye Gold,
modelos y Power BI desde Silver completo para conservar la comparación 2023–2026. No usa
`--force`, por lo que una ejecución repetida omite archivos 2026 ya validados.

## Frase de cierre sugerida

“La solución no trabaja con muestras. Bronze conserva los archivos oficiales, Silver
reconcilia cada fila como válida o cuarentena, Gold publica nueve hechos analíticos y los
tres modelos, y la auditoría demuestra el linaje hasta las diez páginas de Power BI.”
