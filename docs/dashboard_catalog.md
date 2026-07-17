# Catálogo de dashboards Power BI

El proyecto `powerbi/TLC_BigData.pbip` contiene diez páginas y 94 visuales en total.
Cada contrato es generado desde Gold o auditoría y no contiene detalle muestreado.

| Página | Tipo | Pregunta de decisión | Contrato |
|---|---|---|---|
| 01 Resumen ejecutivo | Descriptivo | ¿Cómo evolucionan viajes, ingresos y distancia por servicio? | `D01_Resumen.csv` |
| 02 Demanda temporal | Descriptivo | ¿Cuáles son los patrones por fecha, hora y día? | `D02_Demanda.csv` |
| 03 Ingresos y tarifas | Descriptivo | ¿Qué servicios generan más importes, tarifas y propinas? | `D03_Ingresos.csv` |
| 04 Causas del cambio | Diagnóstico | ¿Qué servicio, periodo o zona explica las variaciones? | `D04_Causas.csv` |
| 05 Rutas y congestión | Diagnóstico | ¿Qué rutas muestran mayor duración, velocidad o costo? | `D05_Rutas.csv` |
| 06 Propinas y anomalías | Diagnóstico | ¿Qué factores se asocian con propinas y días atípicos? | `D06_Anomalias.csv` |
| 07 Pronóstico de demanda | Predictivo | ¿Cuántos viajes se esperan en los próximos 30 días? | `D07_Pronostico.csv` |
| 08 Segmentación de zonas | Predictivo | ¿Qué perfiles operativos de zonas existen? | `D08_Segmentacion.csv` |
| 09 Clasificación alta demanda | Predictivo | ¿Qué zonas/periodos se clasifican como alta demanda? | `D09_Clasificacion.csv` |
| 10 Control y auditoría | Auditoría | ¿El pipeline está completo, conciliado y sin fallos? | `D10_Auditoria.csv` |

Los visuales incluyen tarjetas KPI, tendencias, comparaciones por categoría, matriz de
detalle y filtros de año/servicio. El modelo semántico utiliza medidas DAX para total,
promedio, máximo y recuento, con temas y formatos consistentes.
