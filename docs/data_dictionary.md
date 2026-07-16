# Diccionario y calidad de datos

## Esquema canónico Silver

| Campo | Tipo lógico | Uso |
|---|---|---|
| `service_type` | string | yellow, green, fhv o fhvhv |
| `pickup_datetime`, `dropoff_datetime` | timestamp | duración y dimensiones calendario |
| `pickup_date`, `pickup_hour`, `day_of_week` | date/int | análisis temporal y ML |
| `pickup_location_id`, `dropoff_location_id` | int | unión con Taxi Zone Lookup |
| `pickup_zone`, `dropoff_zone`, boroughs | string | geografía y rutas |
| `passenger_count` | double | perfil del viaje cuando la fuente lo informa |
| `trip_distance` | double | distancia cuando la fuente lo informa |
| `fare_amount`, `tip_amount`, `tolls_amount`, `total_amount` | double | análisis financiero |
| `cbd_congestion_fee` | double | cargo CBD presente en esquemas recientes |
| `trip_duration_minutes`, `speed_mph` | double | diagnóstico operacional |
| `source_file`, `source_year`, `source_month` | string/int | lineage y auditoría |
| `is_valid`, `quality_errors` | boolean/array | resultado explicable de reglas DQ |

Las columnas inexistentes en un servicio se conservan como `null`, no como cero, para
no confundir ausencia del atributo con un valor medido.

## Reglas principales de calidad

- Fechas de recogida/entrega válidas y entrega posterior a recogida cuando existe.
- Duración entre 0 y 1,440 minutos.
- LocationID entre 1 y 265 cuando el servicio lo suministra.
- Distancia entre 0 y 500 millas.
- Total absoluto no superior a 10,000 USD.
- Campos fuente y particiones coherentes con el nombre del archivo.
- Reconciliación obligatoria: filas Bronze = Silver válidas + cuarentena.
- 12 meses × 4 servicios × 3 años = 144 archivos históricos obligatorios.

Los umbrales son configurables en `config/pipeline.yaml`; cada resultado se registra
en `quality_results` y aparece en el dashboard de auditoría.
