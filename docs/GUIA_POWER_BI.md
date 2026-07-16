# Guía para entender y exponer el Power BI de TLC

## 1. Qué responde el informe

El proyecto `powerbi/TLC_BigData.pbip` contiene diez páginas. Las páginas 01–03
describen qué ocurrió; 04–06 ayudan a diagnosticar por qué ocurrió; 07–09 muestran
resultados predictivos; y la página 10 demuestra que el procesamiento fue controlado
y auditable.

Los valores cambian al seleccionar un año, servicio, hora, zona, tipo de pago o
segmento. Antes de interpretar una cifra se debe revisar siempre qué segmentadores
están activos.

## 2. Cómo leer los tipos de visual

| Visual en Power BI | Qué significa | Cómo se interpreta |
|---|---|---|
| Tarjeta o `Card` | Un indicador principal o KPI | Resume el valor correspondiente a todos los filtros activos. |
| Gráfico de líneas o `Line chart` | Evolución o secuencia | El eje horizontal muestra tiempo u orden; una subida representa crecimiento del indicador. |
| Barras agrupadas o `Clustered bar chart` | Comparación horizontal entre categorías | La barra más larga representa el valor mayor. Es útil cuando los nombres son largos. |
| Columnas agrupadas o `Clustered column chart` | Comparación vertical entre grupos | La columna más alta representa el valor mayor. No implica causalidad. |
| Dona o `Donut chart` | Participación dentro de un total | Cada porción representa la contribución de una categoría; debe leerse junto con el total. |
| Segmentador o `Slicer` | Filtro interactivo | Restringe toda la página a los valores seleccionados. No es un resultado. |

Al seleccionar una barra, punto o porción, Power BI aplica resaltado cruzado a los
demás visuales. Para volver al total se hace clic nuevamente en el elemento o en un
espacio vacío.

## 3. Explicación de las diez páginas

### 01 Resumen ejecutivo — qué tamaño tiene la operación

**Pregunta:** ¿cuántos viajes e ingresos se registraron y cómo se distribuyen?

- **Viajes Totales:** suma de `trip_count`; representa todos los viajes del contexto filtrado.
- **Ingresos Totales:** suma del importe total registrado, en dólares.
- **Línea por fecha y servicio:** permite ver crecimiento, caídas y diferencias entre Yellow, Green, FHV y FHVHV.
- **Barras por borough de origen:** compara dónde se inicia la mayor cantidad de viajes.
- **Columnas por servicio:** compara el ingreso generado por cada servicio.
- **Dona por borough:** muestra la participación de cada borough en el total de viajes.
- **Filtros:** año y servicio.

**Mensaje para exponer:** “Esta página establece la magnitud y evolución general. La
utilizo como punto de partida antes de buscar causas o aplicar modelos”.

### 02 Demanda temporal — cuándo se producen los viajes

**Pregunta:** ¿en qué fechas y horas se concentra la demanda?

- **Viajes Totales:** volumen de viajes bajo los filtros seleccionados.
- **Duración Promedio Ponderada:** minutos promedio, dando mayor peso a los grupos con más viajes.
- **Línea por fecha:** identifica tendencia, estacionalidad y días con cambios pronunciados.
- **Barras por hora (`pickup_hour`):** señala las horas con mayor demanda.
- **Columnas por servicio:** compara la duración promedio de los servicios.
- **Dona por hora:** muestra la proporción del volumen correspondiente a cada hora.
- **Filtros:** año, servicio y hora.

**Mensaje para exponer:** “Esta página ayuda a planificar capacidad porque revela las
franjas horarias en las que se concentra la demanda”.

### 03 Ingresos y tarifas — cómo se compone el resultado financiero

**Pregunta:** ¿qué servicios y medios de pago generan más ingresos y propinas?

- **Ingresos Totales:** suma del ingreso total.
- **Tasa de Propina Ponderada:** propinas divididas entre tarifas; evita promediar porcentajes de grupos con tamaños diferentes.
- **Línea mensual:** muestra la evolución de ingresos por servicio.
- **Barras por tipo de pago:** compara los ingresos asociados a cada código de pago.
- **Columnas por servicio:** compara la tasa de propina.
- **Dona por tipo de pago:** muestra la participación de cada forma de pago en los ingresos.
- **Filtros:** año, servicio y tipo de pago.

**Mensaje para exponer:** “No confundo ingresos con rentabilidad: esta página describe
recaudación y comportamiento de propinas, pero no contiene costos operativos”.

### 04 Causas del cambio — qué días se alejaron del comportamiento normal

**Pregunta:** ¿dónde se detectaron cambios atípicos de demanda?

- **Anomalías Detectadas:** cantidad de observaciones marcadas como atípicas.
- **Porcentaje de Anomalías:** anomalías divididas entre todas las observaciones analizadas.
- **Dirección de la anomalía (`anomaly_direction`):** indica si la demanda fue inusualmente alta o baja.
- **Línea temporal:** muestra cuándo aparecen las anomalías y en qué servicio.
- **Barras y dona:** distribuyen las anomalías según su dirección.
- **Columnas por servicio:** compara qué servicio presenta mayor proporción de anomalías.
- **Filtros:** año, servicio y dirección.

**Mensaje para exponer:** “Una anomalía no demuestra su causa. Señala dónde investigar
eventos, clima, feriados o cambios operativos”.

### 05 Rutas y congestión — dónde se concentra el desempeño operativo

**Pregunta:** ¿qué rutas y zonas presentan más viajes o mayores tiempos?

- **Viajes en Rutas:** suma de viajes incluidos en las combinaciones origen–destino.
- **Duración Ponderada:** duración promedio calculada considerando el número de viajes de cada ruta.
- **Línea mensual:** muestra la evolución del volumen de rutas por servicio.
- **Barras por zona de origen (`pickup_zone`):** ordena zonas según cantidad de viajes.
- **Columnas por servicio:** compara duración media ponderada.
- **Dona por zona:** representa la concentración de viajes por zona de origen.
- **Filtros:** año, servicio, borough de origen y borough de destino.

**Mensaje para exponer:** “Una duración mayor puede sugerir congestión, pero debe
analizarse junto con distancia y velocidad; por sí sola no prueba congestión”.

### 06 Propinas y anomalías — qué factores están asociados a la propina

**Pregunta:** ¿cómo cambia el comportamiento de propinas por ubicación, servicio, pago y hora?

- **Tasa de Propina Ponderada:** propinas sobre tarifa base.
- **Viajes con Propina %:** viajes con propina positiva divididos entre viajes analizados.
- **Línea mensual:** muestra si la tasa de propina sube o baja por servicio.
- **Barras por borough:** compara la tasa de propina según origen.
- **Columnas por servicio:** compara el porcentaje de viajes con propina.
- **Dona por borough:** representa la distribución de la medida entre ubicaciones.
- **Filtros:** año, servicio, tipo de pago y hora.

**Mensaje para exponer:** “La página muestra asociación, no causalidad. Además, los
servicios o pagos sin propina registrada deben interpretarse según su mecanismo de cobro”.

### 07 Pronóstico de demanda — qué volumen espera el modelo

**Pregunta:** ¿cuántos viajes se esperan durante el horizonte de 30 días?

- **Viajes Pronosticados (`forecast trips`):** suma de las predicciones del modelo GBT.
- **Límite Superior 95:** extremo superior del intervalo de predicción al 95 %.
- **WMAPE:** error absoluto ponderado porcentual. Un valor de 6,73 % significa que el error absoluto total equivale aproximadamente al 6,73 % de la demanda real total de evaluación; menor es mejor.
- **Línea por fecha de pronóstico:** muestra la trayectoria prevista por servicio.
- **Barras y dona por servicio:** comparan y distribuyen el volumen pronosticado.
- **Columnas por servicio:** comparan el límite superior del intervalo.
- **Filtros:** año, servicio y día del horizonte (`horizon_day`).

**Mensaje para exponer:** “El pronóstico es una estimación con incertidumbre, no una
cifra garantizada. Por eso se muestran límites y una métrica de error”.

### 08 Segmentación de zonas — qué perfiles de zona encontró K-Means

**Pregunta:** ¿qué grupos de zonas presentan comportamientos operativos semejantes?

- **Zonas Segmentadas:** número distinto de zonas clasificadas.
- **Ingreso por Viaje:** ingresos divididos entre viajes del segmento.
- **Silhouette:** mide separación y cohesión de los clústeres; se aproxima a 1 cuando los grupos están bien diferenciados, cerca de 0 cuando se superponen y puede ser negativa cuando la asignación es deficiente.
- **Barras por segmento (`segment_label`):** compara cuántas zonas contiene cada grupo.
- **Columnas por borough:** compara ingreso por viaje.
- **Dona por segmento:** muestra el peso de cada grupo en cantidad de zonas.
- **Visual por LocationID y borough:** permite explorar cómo se distribuyen las zonas clasificadas.
- **Filtros:** segmento y borough de origen.

**Mensaje para exponer:** “K-Means no dice qué grupo es bueno o malo. Agrupa zonas
similares; el significado de cada segmento se obtiene observando sus características”.

### 09 Clasificación de alta demanda — qué tan bien identifica el Random Forest

**Pregunta:** ¿el modelo identifica correctamente los periodos de alta demanda?

- **Accuracy o exactitud:** aciertos divididos entre todos los casos evaluados. El resultado actual es 97,04 %.
- **Casos Evaluados:** número de observaciones usadas para medir el modelo.
- **AUC-ROC:** capacidad para separar alta y no alta demanda en todos los umbrales. Un valor de 0,5 equivale aproximadamente al azar y uno cercano a 1 indica muy buena separación. El resultado actual es 0,9979.
- **Línea temporal:** muestra la exactitud por fecha y clase predicha.
- **Barras por `dataset_split`:** compara exactitud en la partición temporal de prueba.
- **Columnas por clase predicha:** muestra cuántos casos se clasificaron como 0 o 1.
- **Dona:** resume el desempeño de la partición evaluada.
- **Filtros:** año, partición del conjunto y clase predicha.

`predicted_high_demand = 1` significa alta demanda predicha; `0` significa que no se
predice alta demanda. La exactitud debe leerse junto con AUC, precision, recall, F1 y
la matriz de confusión, porque una clase mayoritaria puede inflar el accuracy.

**Mensaje para exponer:** “La evaluación respeta el tiempo: se entrena con el pasado y
se prueba con un periodo posterior, evitando mezclar información futura”.

### 10 Control y auditoría — se puede confiar en la ejecución

**Pregunta:** ¿el pipeline terminó, conservó las filas y produjo sus artefactos?

- **Eventos:** cantidad de registros de auditoría.
- **Eventos OK:** registros con estado `PASSED` u `OK`.
- **Línea por fecha y estado:** muestra la actividad del pipeline y sus estados.
- **Barras por categoría:** compara eventos de ejecución, archivos, calidad y modelos.
- **Columnas por estado:** separa resultados correctos y fallidos.
- **Dona por categoría:** muestra la composición del flujo auditado.
- **Filtros:** estado y servicio.

**Mensaje para exponer:** “Esta página conecta los resultados analíticos con la
evidencia técnica: ejecuciones, archivos, calidad, reconciliación y modelos”.

## 4. Glosario inglés–español

### Datos TLC y arquitectura

| Término | Traducción y significado |
|---|---|
| TLC | Taxi and Limousine Commission de Nueva York. |
| Trip record | Registro de viaje. |
| Pickup / drop-off | Recogida/origen y descenso/destino. |
| Pickup date | Fecha de inicio del viaje. |
| Pickup hour | Hora de inicio del viaje. |
| LocationID | Identificador oficial de zona TLC. |
| Borough | Distrito de Nueva York: Manhattan, Queens, Brooklyn, Bronx, Staten Island o EWR. |
| Yellow / Green | Servicios de taxi amarillo y taxi verde. |
| FHV | Vehículo de alquiler, `For-Hire Vehicle`. |
| FHVHV | Vehículo de alquiler de gran volumen, `High Volume For-Hire Vehicle`. |
| Bronze | Capa que conserva los archivos originales. |
| Silver | Capa normalizada y validada. |
| Gold | Capa de tablas analíticas y resultados de modelos. |
| Fact | Tabla de hechos con métricas y eventos medibles. |
| Dimension | Tabla descriptiva usada para filtrar o agrupar hechos. |
| Lineage | Trazabilidad desde el resultado hasta la fuente. |
| Data quality / DQ | Calidad de datos. |
| Quarantine | Cuarentena de registros que incumplen reglas. |
| Serving | Capa preparada para consumo por Power BI u otras aplicaciones. |

### Power BI y métricas

| Término | Traducción y significado |
|---|---|
| Dashboard / report page | Panel o página del informe. |
| KPI | Indicador clave de desempeño. |
| Measure / DAX measure | Cálculo dinámico que responde a filtros. |
| Slicer | Segmentador o filtro visual. |
| Filter context | Conjunto de filtros activos que determina un resultado. |
| Weighted average | Promedio ponderado; considera el tamaño de cada grupo. |
| Revenue | Ingresos. |
| Fare | Tarifa base. |
| Tip | Propina. |
| Trip count | Cantidad de viajes. |
| Dataset split | División de datos en entrenamiento y prueba. |

### Modelos y evaluación

| Término | Traducción y significado |
|---|---|
| Forecast | Pronóstico. |
| Forecast horizon | Cantidad de días futuros pronosticados. |
| GBT | Árboles potenciados por gradiente; combina árboles secuenciales para reducir error. |
| K-Means | Algoritmo que agrupa observaciones alrededor de centroides. |
| Cluster | Grupo o segmento encontrado por K-Means. |
| Random Forest | Bosque aleatorio; combina muchos árboles de decisión. |
| Feature | Variable de entrada utilizada por un modelo. |
| Label / target | Variable real que el modelo intenta predecir. |
| Prediction | Resultado producido por el modelo. |
| Accuracy | Proporción total de predicciones correctas. |
| Precision | De los casos predichos como positivos, proporción realmente positiva. |
| Recall | De los positivos reales, proporción detectada por el modelo. |
| F1 | Media armónica de precision y recall. |
| AUC-ROC | Capacidad global para separar las dos clases. |
| Confusion matrix | Matriz de verdaderos/falsos positivos y negativos. |
| WMAPE | Error absoluto porcentual ponderado; menor es mejor. |
| RMSE | Raíz del error cuadrático medio; penaliza errores grandes. |
| R² | Proporción de variación explicada por el modelo; más cerca de 1 suele ser mejor. |
| Silhouette | Calidad de separación de los clústeres. |
| Anomaly | Observación atípica frente al patrón esperado. |
| Z-score | Distancia respecto de la media expresada en desviaciones estándar. |
| Confidence/prediction interval | Rango de incertidumbre alrededor de una estimación. |

## 5. Reglas para no interpretar mal el informe

1. Revisar los segmentadores activos antes de citar una cifra.
2. No presentar asociación como causalidad.
3. No comparar totales de periodos de distinta duración sin aclararlo.
4. Leer los porcentajes junto con su denominador o cantidad de casos.
5. En modelos, separar entrenamiento, prueba y pronóstico futuro.
6. No evaluar clasificación solamente con accuracy.
7. No interpretar un clúster como ranking de calidad.
8. Tratar límites de pronóstico como incertidumbre, no como valores garantizados.

## 6. Recorrido sugerido para la exposición

1. Abrir la página 01 y fijar año y servicio.
2. Explicar el volumen general y una tendencia.
3. Usar páginas 02 y 03 para describir cuándo ocurre la demanda y cómo se generan ingresos.
4. Pasar a páginas 04–06 para mostrar anomalías, rutas y factores asociados.
5. Presentar páginas 07–09 indicando algoritmo, objetivo y métrica de validación.
6. Finalizar en la página 10 para demostrar trazabilidad y control.
7. Limpiar los filtros antes de pasar a otra pregunta del jurado.

La conclusión central es: el informe combina análisis descriptivo, diagnóstico y
predictivo, pero mantiene separados sus alcances. Describe patrones, señala zonas de
investigación y entrega predicciones validadas; no afirma causalidad sin evidencia.
