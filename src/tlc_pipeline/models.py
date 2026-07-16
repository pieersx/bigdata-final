"""Modelos predictivos distribuidos para las tablas Gold del caso NYC TLC.

Los tres flujos trabajan exclusivamente sobre agregados construidos con todas
las filas disponibles. No hay llamadas a ``sample`` ni conversiones del corpus
a pandas. Solo se recolectan escalares pequeños (metricas, umbrales y conteos)
despues de que Spark ha realizado las agregaciones.

Salidas principales acordadas con la capa Gold:

* ``model_timeseries_daily``: pronostico recursivo por servicio.
* ``model_segmentation_zones``: cluster y perfil de cada zona.
* ``model_classification_demand``: evaluacion temporal de alta demanda.

Cada salida se publica como Parquet en Gold y CSV en exports. Los modelos Spark
ML se guardan con MLWriter bajo artifacts/models.
"""

from __future__ import annotations

import math
import re
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    ClusteringEvaluator,
    MulticlassClassificationEvaluator,
    RegressionEvaluator,
)
from pyspark.ml.feature import OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

if TYPE_CHECKING:
    from .audit import AuditLogger
    from .config import PipelineConfig


MODEL_TIMESERIES_TABLE = "model_timeseries_daily"
MODEL_TIMESERIES_METRICS_TABLE = "model_timeseries_metrics"
MODEL_SEGMENTATION_TABLE = "model_segmentation_zones"
MODEL_SEGMENTATION_PROFILES_TABLE = "model_segmentation_profiles"
MODEL_SEGMENTATION_METRICS_TABLE = "model_segmentation_metrics"
MODEL_CLASSIFICATION_TABLE = "model_classification_demand"
MODEL_CLASSIFICATION_METRICS_TABLE = "model_classification_metrics"
MODEL_CLASSIFICATION_CONFUSION_TABLE = "model_classification_confusion"
MODEL_METRICS_TABLE = "model_metrics"


DAILY_SERVICE_TABLE_CANDIDATES = (
    "daily_service",
    "service_daily",
    "fact_daily_service",
    "daily_service_metrics",
    "service_day",
    "trips_daily_service",
    "descriptive_daily_demand",
    "model_timeseries_daily",
)
ZONE_DAILY_TABLE_CANDIDATES = (
    "zone_daily",
    "daily_zone",
    "fact_zone_daily",
    "zone_day",
    "daily_zone_metrics",
    "pickup_zone_daily",
    "descriptive_daily_demand",
    "model_classification_demand",
)
ZONE_FEATURE_TABLE_CANDIDATES = (
    "zone_features",
    "zone_feature",
    "zone_profile_features",
    "zone_analytics",
    *ZONE_DAILY_TABLE_CANDIDATES,
    "model_segmentation_zones",
)


TIME_FEATURE_COLUMNS = (
    "day_of_week",
    "month",
    "day_of_month",
    "day_of_year",
    "week_of_year",
    "is_weekend",
    "sin_day_of_year",
    "cos_day_of_year",
    "trend_days",
    "lag_1",
    "lag_7",
    "lag_14",
    "lag_28",
    "rolling_mean_7",
    "rolling_mean_28",
)

ZONE_FEATURE_COLUMNS = (
    "total_trips",
    "avg_daily_trips",
    "demand_stddev",
    "active_days",
    "avg_fare_amount",
    "avg_trip_distance",
    "avg_duration_minutes",
    "total_revenue",
)


@dataclass(frozen=True, slots=True)
class ModelSettings:
    """Parametros reproducibles de los tres modelos."""

    seed: int = 42
    forecast_horizon_days: int = 30
    forecast_test_days: int = 90
    forecast_max_iter: int = 80
    forecast_max_depth: int = 6
    segmentation_k: int = 4
    segmentation_max_iter: int = 40
    classification_test_days: int = 90
    classification_num_trees: int = 100
    classification_max_depth: int = 10
    high_demand_quantile: float = 0.75
    csv_single_file: bool = True

    @classmethod
    def from_config(cls, config: PipelineConfig | Mapping[str, Any]) -> ModelSettings:
        if hasattr(config, "section"):
            models = dict(config.section("models"))
            try:
                gold = dict(config.section("gold"))
            except Exception:
                gold = {}
        else:
            raw = dict(config)
            models = dict(raw.get("models", raw))
            gold = dict(raw.get("gold", {}))

        forecast = dict(models.get("forecast", {}))
        segmentation = dict(models.get("segmentation", {}))
        classification = dict(models.get("classification", {}))
        return cls(
            seed=int(models.get("seed", 42)),
            forecast_horizon_days=int(forecast.get("horizon_days", 30)),
            forecast_test_days=int(forecast.get("test_days", 90)),
            forecast_max_iter=int(forecast.get("max_iter", 80)),
            forecast_max_depth=int(forecast.get("max_depth", 6)),
            segmentation_k=int(segmentation.get("k", 4)),
            segmentation_max_iter=int(segmentation.get("max_iter", 40)),
            classification_test_days=int(classification.get("test_days", 90)),
            classification_num_trees=int(classification.get("num_trees", 100)),
            classification_max_depth=int(classification.get("max_depth", 10)),
            high_demand_quantile=float(classification.get("high_demand_quantile", 0.75)),
            csv_single_file=bool(gold.get("csv_single_file", True)),
        )

    def validate(self) -> None:
        integer_values = {
            "forecast_horizon_days": self.forecast_horizon_days,
            "forecast_test_days": self.forecast_test_days,
            "forecast_max_iter": self.forecast_max_iter,
            "forecast_max_depth": self.forecast_max_depth,
            "segmentation_k": self.segmentation_k,
            "segmentation_max_iter": self.segmentation_max_iter,
            "classification_test_days": self.classification_test_days,
            "classification_num_trees": self.classification_num_trees,
            "classification_max_depth": self.classification_max_depth,
        }
        invalid = [name for name, value in integer_values.items() if value <= 0]
        if invalid:
            raise ValueError(f"Los parametros deben ser positivos: {', '.join(invalid)}")
        if not 0.0 < self.high_demand_quantile < 1.0:
            raise ValueError("high_demand_quantile debe estar entre 0 y 1")


@dataclass(slots=True)
class ModelRunResult:
    """Resultado materializable de un entrenamiento."""

    model_name: str
    metrics: dict[str, Any]
    frames: dict[str, DataFrame]
    paths: dict[str, str]


def _normalise_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.casefold())


def _resolve_column(
    frame: DataFrame,
    candidates: Sequence[str],
    *,
    required: bool = True,
    purpose: str = "columna",
) -> str | None:
    by_name = {_normalise_name(column): column for column in frame.columns}
    for candidate in candidates:
        resolved = by_name.get(_normalise_name(candidate))
        if resolved is not None:
            return resolved
    if required:
        raise ValueError(
            f"No se encontro {purpose}. Candidatas={list(candidates)}; disponibles={frame.columns}"
        )
    return None


def _finite(value: float | int | None, default: float = 0.0) -> float:
    if value is None:
        return default
    number = float(value)
    return number if math.isfinite(number) else default


class PredictiveModelsPipeline:
    """Entrena, evalua y publica los tres modelos Spark ML del examen."""

    def __init__(
        self,
        spark: SparkSession,
        *,
        gold_path: str | Path,
        models_path: str | Path,
        exports_path: str | Path,
        settings: ModelSettings | None = None,
        audit: AuditLogger | None = None,
        run_id: str | None = None,
    ) -> None:
        self.spark = spark
        self.gold_path = Path(gold_path)
        self.models_path = Path(models_path)
        self.exports_path = Path(exports_path)
        self.settings = settings or ModelSettings()
        self.settings.validate()
        self.audit = audit
        self.run_id = run_id

    @classmethod
    def from_config(
        cls,
        spark: SparkSession,
        config: PipelineConfig,
        *,
        audit: AuditLogger | None = None,
        run_id: str | None = None,
    ) -> PredictiveModelsPipeline:
        return cls(
            spark,
            gold_path=config.path("gold"),
            models_path=config.path("models"),
            exports_path=config.path("exports"),
            settings=ModelSettings.from_config(config),
            audit=audit,
            run_id=run_id,
        )

    def _spark_path_exists(self, path: Path) -> bool:
        try:
            jvm_path = self.spark._jvm.org.apache.hadoop.fs.Path(str(path))
            filesystem = jvm_path.getFileSystem(self.spark._jsc.hadoopConfiguration())
            return bool(filesystem.exists(jvm_path))
        except Exception:
            return path.exists()

    def _read_first_gold(self, candidates: Sequence[str], purpose: str) -> DataFrame:
        for table_name in candidates:
            table_path = self.gold_path / table_name
            if self._spark_path_exists(table_path):
                return self.spark.read.parquet(str(table_path))

        if self.gold_path.is_dir():
            normalised = {
                _normalise_name(child.name): child
                for child in self.gold_path.iterdir()
                if child.is_dir()
            }
            for table_name in candidates:
                candidate = normalised.get(_normalise_name(table_name))
                if candidate is not None:
                    return self.spark.read.parquet(str(candidate))

        raise FileNotFoundError(
            f"No se encontro una tabla Gold para {purpose}. "
            f"Se buscaron: {', '.join(candidates)} bajo {self.gold_path}"
        )

    def _resolve_input(
        self,
        provided: DataFrame | None,
        candidates: Sequence[str],
        purpose: str,
    ) -> DataFrame:
        return provided if provided is not None else self._read_first_gold(candidates, purpose)

    def _save_model(self, model: PipelineModel, relative_name: str) -> str:
        destination = self.models_path / relative_name
        destination.parent.mkdir(parents=True, exist_ok=True)
        model.write().overwrite().save(str(destination))
        return str(destination)

    def _write_frame(self, frame: DataFrame, table_name: str) -> dict[str, str]:
        parquet_path = self.gold_path / table_name
        csv_path = self.exports_path / table_name
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        frame.write.mode("overwrite").parquet(str(parquet_path))
        csv_writer_frame = frame.coalesce(1) if self.settings.csv_single_file else frame
        csv_writer_frame.write.mode("overwrite").option("header", "true").csv(str(csv_path))
        return {f"{table_name}_parquet": str(parquet_path), f"{table_name}_csv": str(csv_path)}

    def _metrics_frame(self, model_name: str, metrics: Mapping[str, Any]) -> DataFrame:
        generated_at = datetime.now(UTC).replace(tzinfo=None)
        rows: list[tuple[str, str, float | None, str | None, datetime]] = []
        for metric_name, raw_value in sorted(metrics.items()):
            if isinstance(raw_value, bool):
                metric_value: float | None = float(raw_value)
                metric_text = str(raw_value).lower()
            elif isinstance(raw_value, int | float):
                metric_value = _finite(raw_value)
                metric_text = None
            else:
                metric_value = None
                metric_text = None if raw_value is None else str(raw_value)
            rows.append((model_name, metric_name, metric_value, metric_text, generated_at))
        schema = T.StructType(
            [
                T.StructField("model_name", T.StringType(), False),
                T.StructField("metric_name", T.StringType(), False),
                T.StructField("metric_value", T.DoubleType(), True),
                T.StructField("metric_text", T.StringType(), True),
                T.StructField("generated_at_utc", T.TimestampType(), False),
            ]
        )
        return self.spark.createDataFrame(rows, schema=schema)

    def _record_model(
        self,
        model_name: str,
        metrics: Mapping[str, Any],
        paths: Mapping[str, str],
    ) -> None:
        if self.audit is None:
            return
        self.audit.record_model(
            {
                "model_run_id": f"{model_name}-{uuid.uuid4()}",
                "model_name": model_name,
                "status": "SUCCEEDED",
                "metrics": dict(metrics),
                "artifacts": dict(paths),
            },
            run_id=self.run_id,
        )

    @staticmethod
    def _require_rows(frame: DataFrame, purpose: str, minimum: int = 1) -> int:
        count = frame.count()
        if count < minimum:
            raise ValueError(f"{purpose} requiere al menos {minimum} filas; se encontraron {count}")
        return count

    def _complete_daily_panel(
        self,
        frame: DataFrame,
        *,
        date_column: str,
        entity_column: str,
        value_column: str,
    ) -> DataFrame:
        bounds = frame.groupBy(entity_column).agg(
            F.min(date_column).alias("_min_date"), F.max(date_column).alias("_max_date")
        )
        calendar = bounds.select(
            entity_column,
            F.explode(
                F.sequence(F.col("_min_date"), F.col("_max_date"), F.expr("interval 1 day"))
            ).alias(date_column),
        )
        return (
            calendar.join(frame, [entity_column, date_column], "left")
            .withColumn(value_column, F.coalesce(F.col(value_column), F.lit(0.0)))
            .select(entity_column, date_column, value_column)
        )

    def _add_time_features(
        self,
        frame: DataFrame,
        *,
        date_column: str,
        entity_column: str,
        value_column: str,
    ) -> DataFrame:
        ordered = Window.partitionBy(entity_column).orderBy(F.col(date_column))
        entity_window = Window.partitionBy(entity_column)
        rolling_7 = ordered.rowsBetween(-7, -1)
        rolling_28 = ordered.rowsBetween(-28, -1)
        two_pi = 2.0 * math.pi
        return (
            frame.withColumn("day_of_week", (F.dayofweek(date_column) - F.lit(1)).cast("double"))
            .withColumn("month", F.month(date_column).cast("double"))
            .withColumn("day_of_month", F.dayofmonth(date_column).cast("double"))
            .withColumn("day_of_year", F.dayofyear(date_column).cast("double"))
            .withColumn("week_of_year", F.weekofyear(date_column).cast("double"))
            .withColumn(
                "is_weekend",
                F.when(F.dayofweek(date_column).isin(1, 7), F.lit(1.0)).otherwise(F.lit(0.0)),
            )
            .withColumn(
                "sin_day_of_year", F.sin(F.lit(two_pi) * F.dayofyear(date_column) / F.lit(365.25))
            )
            .withColumn(
                "cos_day_of_year", F.cos(F.lit(two_pi) * F.dayofyear(date_column) / F.lit(365.25))
            )
            .withColumn(
                "trend_days",
                F.datediff(F.col(date_column), F.min(date_column).over(entity_window)).cast(
                    "double"
                ),
            )
            .withColumn("lag_1", F.lag(value_column, 1).over(ordered).cast("double"))
            .withColumn("lag_7", F.lag(value_column, 7).over(ordered).cast("double"))
            .withColumn("lag_14", F.lag(value_column, 14).over(ordered).cast("double"))
            .withColumn("lag_28", F.lag(value_column, 28).over(ordered).cast("double"))
            .withColumn("rolling_mean_7", F.avg(value_column).over(rolling_7).cast("double"))
            .withColumn("rolling_mean_28", F.avg(value_column).over(rolling_28).cast("double"))
        )

    def _temporal_split(
        self,
        frame: DataFrame,
        *,
        date_column: str,
        entity_column: str,
        test_days: int,
    ) -> tuple[DataFrame, DataFrame]:
        entity_window = Window.partitionBy(entity_column)
        tagged = frame.withColumn(
            "_series_max_date", F.max(date_column).over(entity_window)
        ).withColumn("_cutoff_date", F.date_sub(F.col("_series_max_date"), test_days))
        return (
            tagged.where(F.col(date_column) <= F.col("_cutoff_date")),
            tagged.where(F.col(date_column) > F.col("_cutoff_date")),
        )

    def _prepare_daily_service(self, source: DataFrame) -> DataFrame:
        date_column = _resolve_column(
            source,
            ("trip_date", "pickup_date", "service_date", "date", "day", "pickup_day"),
            purpose="fecha diaria de servicio",
        )
        service_column = _resolve_column(
            source,
            ("service_type", "service", "taxi_type", "trip_type", "service_name"),
            required=False,
        )
        target_column = _resolve_column(
            source,
            (
                "trip_count",
                "total_trips",
                "trips",
                "ride_count",
                "num_trips",
                "trips_total",
                "pickup_count",
            ),
            purpose="cantidad de viajes diarios",
        )
        selected = source.select(
            F.to_date(F.col(date_column)).alias("trip_date"),
            (
                F.coalesce(F.col(service_column).cast("string"), F.lit("UNKNOWN"))
                if service_column
                else F.lit("ALL")
            ).alias("service_type"),
            F.col(target_column).cast("double").alias("trip_count"),
        ).where(F.col("trip_date").isNotNull() & F.col("trip_count").isNotNull())
        return (
            selected.where(F.col("trip_count") >= 0)
            .groupBy("trip_date", "service_type")
            .agg(F.sum("trip_count").alias("trip_count"))
        )

    def _forecast_pipeline(self) -> Pipeline:
        indexer = StringIndexer(
            inputCol="service_type",
            outputCol="service_index",
            handleInvalid="keep",
            stringOrderType="alphabetAsc",
        )
        encoder = OneHotEncoder(
            inputCols=["service_index"],
            outputCols=["service_vector"],
            handleInvalid="keep",
            dropLast=False,
        )
        assembler = VectorAssembler(
            inputCols=[*TIME_FEATURE_COLUMNS, "service_vector"],
            outputCol="features",
            handleInvalid="error",
        )
        regressor = GBTRegressor(
            labelCol="trip_count",
            featuresCol="features",
            predictionCol="prediction",
            seed=self.settings.seed,
            maxIter=self.settings.forecast_max_iter,
            maxDepth=self.settings.forecast_max_depth,
            lossType="squared",
        )
        return Pipeline(stages=[indexer, encoder, assembler, regressor])

    def _recursive_forecast(
        self,
        model: PipelineModel,
        panel: DataFrame,
        *,
        horizon_days: int,
        rmse: float,
    ) -> DataFrame:
        history = panel.select("service_type", "trip_date", "trip_count").localCheckpoint(
            eager=True
        )
        service_count = history.select("service_type").distinct().count()
        forecasts: list[DataFrame] = []
        for horizon in range(1, horizon_days + 1):
            next_rows = (
                history.groupBy("service_type")
                .agg(F.date_add(F.max("trip_date"), 1).alias("trip_date"))
                .withColumn("trip_count", F.lit(None).cast("double"))
            )
            working = history.unionByName(next_rows)
            candidate = (
                self._add_time_features(
                    working,
                    date_column="trip_date",
                    entity_column="service_type",
                    value_column="trip_count",
                )
                .where(F.col("trip_count").isNull())
                .dropna(subset=list(TIME_FEATURE_COLUMNS))
            )
            prediction = (
                model.transform(candidate)
                .select(
                    F.col("trip_date").alias("forecast_date"),
                    "service_type",
                    F.greatest(F.lit(0.0), F.col("prediction")).alias("forecast_trips"),
                )
                .withColumn("horizon_day", F.lit(horizon))
                .withColumn(
                    "forecast_lower_95",
                    F.greatest(F.lit(0.0), F.col("forecast_trips") - F.lit(1.96 * rmse)),
                )
                .withColumn("forecast_upper_95", F.col("forecast_trips") + F.lit(1.96 * rmse))
                .withColumn("model_name", F.lit("gbt_calendar_lag"))
                .withColumn("pickup_date", F.col("forecast_date"))
                .withColumn("service", F.col("service_type"))
                .withColumn("ds", F.col("forecast_date"))
                .withColumn("yhat", F.col("forecast_trips"))
                .withColumn("generated_at_utc", F.current_timestamp())
                .localCheckpoint(eager=True)
            )
            produced = prediction.count()
            if produced != service_count:
                raise ValueError(
                    f"No fue posible pronosticar todas las series en horizonte {horizon}: "
                    f"esperadas={service_count}, obtenidas={produced}. "
                    "Se requieren al menos 28 dias."
                )
            forecasts.append(prediction)
            appended = prediction.select(
                "service_type",
                F.col("forecast_date").alias("trip_date"),
                F.col("forecast_trips").alias("trip_count"),
            )
            previous = history
            history = history.unionByName(appended).localCheckpoint(eager=True)
            previous.unpersist(blocking=False)

        result = forecasts[0]
        for frame in forecasts[1:]:
            result = result.unionByName(frame)
        return result

    def run_time_series(
        self,
        daily_service: DataFrame | None = None,
        *,
        persist: bool = True,
    ) -> ModelRunResult:
        source = self._resolve_input(
            daily_service, DAILY_SERVICE_TABLE_CANDIDATES, "serie diaria por servicio"
        )
        daily = self._prepare_daily_service(source)
        panel = self._complete_daily_panel(
            daily,
            date_column="trip_date",
            entity_column="service_type",
            value_column="trip_count",
        ).localCheckpoint(eager=True)
        feature_frame = (
            self._add_time_features(
                panel,
                date_column="trip_date",
                entity_column="service_type",
                value_column="trip_count",
            )
            .dropna(subset=list(TIME_FEATURE_COLUMNS))
            .cache()
        )
        self._require_rows(feature_frame, "Serie temporal despues de lags", minimum=10)
        train, test = self._temporal_split(
            feature_frame,
            date_column="trip_date",
            entity_column="service_type",
            test_days=self.settings.forecast_test_days,
        )
        train = train.cache()
        test = test.cache()
        train_rows = self._require_rows(train, "Entrenamiento temporal", minimum=2)
        test_rows = self._require_rows(test, "Prueba temporal", minimum=1)

        evaluation_model = self._forecast_pipeline().fit(train)
        holdout = evaluation_model.transform(test).cache()
        regression_evaluator = RegressionEvaluator(
            labelCol="trip_count", predictionCol="prediction"
        )
        rmse = _finite(regression_evaluator.setMetricName("rmse").evaluate(holdout))
        mae = _finite(regression_evaluator.setMetricName("mae").evaluate(holdout))
        r2 = _finite(regression_evaluator.setMetricName("r2").evaluate(holdout))
        error_totals = holdout.agg(
            F.avg(
                F.when(
                    F.col("trip_count") > 0,
                    F.abs(F.col("prediction") - F.col("trip_count")) / F.col("trip_count"),
                )
            ).alias("mape"),
            F.sum(F.abs(F.col("prediction") - F.col("trip_count"))).alias("absolute_error"),
            F.sum(F.abs(F.col("trip_count"))).alias("actual_total"),
        ).first()
        mape = 100.0 * _finite(error_totals["mape"])
        actual_total = _finite(error_totals["actual_total"])
        wmape = (
            100.0 * _finite(error_totals["absolute_error"]) / actual_total
            if actual_total > 0
            else 0.0
        )
        cutoff = test.agg(F.min("_cutoff_date").alias("cutoff")).first()["cutoff"]
        metrics: dict[str, Any] = {
            "rmse": rmse,
            "mae": mae,
            "r2": r2,
            "mape_percent": mape,
            "wmape_percent": wmape,
            "train_rows": train_rows,
            "test_rows": test_rows,
            "series_count": panel.select("service_type").distinct().count(),
            "forecast_horizon_days": self.settings.forecast_horizon_days,
            "temporal_cutoff": cutoff,
            "seed": self.settings.seed,
        }
        # El holdout permanece fuera del ajuste usado para medir. Una vez
        # calculadas las metricas, el artefacto final se reentrena con todos los
        # dias agregados para aprovechar completamente el historial publicado.
        model = self._forecast_pipeline().fit(feature_frame)
        forecast = self._recursive_forecast(
            model,
            panel,
            horizon_days=self.settings.forecast_horizon_days,
            rmse=rmse,
        )
        metrics_frame = self._metrics_frame("time_series_gbt", metrics)
        frames = {
            MODEL_TIMESERIES_TABLE: forecast,
            MODEL_TIMESERIES_METRICS_TABLE: metrics_frame,
        }
        paths: dict[str, str] = {}
        if persist:
            paths["spark_model"] = self._save_model(model, "time_series_forecast")
            for table_name, frame in frames.items():
                paths.update(self._write_frame(frame, table_name))
        self._record_model("time_series_gbt", metrics, paths)
        holdout.unpersist(blocking=False)
        train.unpersist(blocking=False)
        test.unpersist(blocking=False)
        feature_frame.unpersist(blocking=False)
        return ModelRunResult("time_series_gbt", metrics, frames, paths)

    def run_forecast(
        self, daily_service: DataFrame | None = None, *, persist: bool = True
    ) -> ModelRunResult:
        return self.run_time_series(daily_service, persist=persist)

    def _prepare_zone_features(self, source: DataFrame) -> DataFrame:
        zone_column = _resolve_column(
            source,
            (
                "zone_id",
                "location_id",
                "pickup_zone_id",
                "pickup_location_id",
                "pulocationid",
                "PULocationID",
            ),
            purpose="identificador de zona",
        )
        date_column = _resolve_column(
            source,
            ("trip_date", "pickup_date", "date", "day", "service_date"),
            required=False,
        )
        trips_column = _resolve_column(
            source,
            (
                "trip_count",
                "total_trips",
                "trips",
                "ride_count",
                "pickup_count",
                "trips_total",
            ),
            purpose="demanda por zona",
        )
        zone_name_column = _resolve_column(
            source,
            ("zone_name", "zone", "pickup_zone", "location_name"),
            required=False,
        )
        borough_column = _resolve_column(
            source, ("borough", "pickup_borough", "borough_name"), required=False
        )
        active_days_column = _resolve_column(
            source, ("active_days", "observation_days", "days_with_trips"), required=False
        )
        avg_fare_column = _resolve_column(
            source,
            (
                "avg_fare_amount",
                "average_fare",
                "avg_fare",
                "fare_amount_avg",
                "avg_total_amount",
                "revenue_per_trip",
            ),
            required=False,
        )
        distance_column = _resolve_column(
            source,
            (
                "avg_trip_distance",
                "average_trip_distance",
                "trip_distance_avg",
                "avg_distance",
            ),
            required=False,
        )
        duration_column = _resolve_column(
            source,
            (
                "avg_duration_minutes",
                "average_duration_minutes",
                "trip_duration_minutes_avg",
                "avg_trip_duration",
                "avg_trip_duration_minutes",
            ),
            required=False,
        )
        revenue_column = _resolve_column(
            source,
            (
                "total_revenue",
                "revenue",
                "gross_revenue",
                "total_amount_sum",
                "fare_revenue",
            ),
            required=False,
        )

        expressions = [
            F.col(zone_column).cast("string").alias("zone_id"),
            F.col(trips_column).cast("double").alias("_trips"),
        ]
        optional_columns: tuple[tuple[str | None, str, str], ...] = (
            (date_column, "_date", "date"),
            (zone_name_column, "zone_name", "string"),
            (borough_column, "borough", "string"),
            (active_days_column, "_active_days", "double"),
            (avg_fare_column, "_avg_fare", "double"),
            (distance_column, "_avg_distance", "double"),
            (duration_column, "_avg_duration", "double"),
            (revenue_column, "_revenue", "double"),
        )
        for original, alias, data_type in optional_columns:
            if original is not None:
                expression = (
                    F.to_date(F.col(original))
                    if data_type == "date"
                    else F.col(original).cast(data_type)
                )
                expressions.append(expression.alias(alias))
        selected = source.select(*expressions).where(
            F.col("zone_id").isNotNull() & F.col("_trips").isNotNull() & (F.col("_trips") >= 0)
        )

        # Si la tabla aun conserva granularidad servicio-zona-dia, primero se
        # consolida a zona-dia. Asi avg/stddev diarios no quedan sesgados por el
        # numero de servicios presentes en una zona.
        if "_date" in selected.columns:
            daily_aggregations: list[Any] = [F.sum("_trips").alias("_trips")]
            if "zone_name" in selected.columns:
                daily_aggregations.append(F.first("zone_name", ignorenulls=True).alias("zone_name"))
            if "borough" in selected.columns:
                daily_aggregations.append(F.first("borough", ignorenulls=True).alias("borough"))
            if "_active_days" in selected.columns:
                daily_aggregations.append(F.max("_active_days").alias("_active_days"))
            for numeric_column in ("_avg_fare", "_avg_distance", "_avg_duration"):
                if numeric_column in selected.columns:
                    numerator = F.sum(
                        F.when(
                            F.col(numeric_column).isNotNull(),
                            F.col(numeric_column) * F.col("_trips"),
                        ).otherwise(F.lit(0.0))
                    )
                    denominator = F.sum(
                        F.when(F.col(numeric_column).isNotNull(), F.col("_trips")).otherwise(
                            F.lit(0.0)
                        )
                    )
                    daily_aggregations.append(
                        F.coalesce(
                            numerator / F.when(denominator > 0, denominator), F.lit(0.0)
                        ).alias(numeric_column)
                    )
            if "_revenue" in selected.columns:
                daily_aggregations.append(
                    F.sum(F.coalesce(F.col("_revenue"), F.lit(0.0))).alias("_revenue")
                )
            selected = selected.groupBy("zone_id", "_date").agg(*daily_aggregations)

        aggregations: list[Any] = [
            F.sum("_trips").alias("total_trips"),
            F.avg("_trips").alias("avg_daily_trips"),
            F.coalesce(F.stddev_pop("_trips"), F.lit(0.0)).alias("demand_stddev"),
        ]
        if "_date" in selected.columns:
            aggregations.append(F.countDistinct("_date").cast("double").alias("active_days"))
        elif "_active_days" in selected.columns:
            aggregations.append(F.max("_active_days").alias("active_days"))
        else:
            aggregations.append(F.count(F.lit(1)).cast("double").alias("active_days"))
        if "zone_name" in selected.columns:
            aggregations.append(F.first("zone_name", ignorenulls=True).alias("zone_name"))
        if "borough" in selected.columns:
            aggregations.append(F.first("borough", ignorenulls=True).alias("borough"))

        def weighted_average(column: str, alias: str) -> None:
            if column not in selected.columns:
                aggregations.append(F.lit(0.0).alias(alias))
                return
            numerator = F.sum(
                F.when(F.col(column).isNotNull(), F.col(column) * F.col("_trips")).otherwise(
                    F.lit(0.0)
                )
            )
            denominator = F.sum(
                F.when(F.col(column).isNotNull(), F.col("_trips")).otherwise(F.lit(0.0))
            )
            aggregations.append(
                F.coalesce(numerator / F.when(denominator > 0, denominator), F.lit(0.0)).alias(
                    alias
                )
            )

        weighted_average("_avg_fare", "avg_fare_amount")
        weighted_average("_avg_distance", "avg_trip_distance")
        weighted_average("_avg_duration", "avg_duration_minutes")
        if "_revenue" in selected.columns:
            aggregations.append(
                F.sum(F.coalesce(F.col("_revenue"), F.lit(0.0))).alias("total_revenue")
            )
        elif "_avg_fare" in selected.columns:
            aggregations.append(
                F.sum(F.coalesce(F.col("_avg_fare"), F.lit(0.0)) * F.col("_trips")).alias(
                    "total_revenue"
                )
            )
        else:
            aggregations.append(F.lit(0.0).alias("total_revenue"))

        return (
            selected.groupBy("zone_id")
            .agg(*aggregations)
            .fillna(0.0, subset=list(ZONE_FEATURE_COLUMNS))
        )

    def _segmentation_pipeline(self) -> Pipeline:
        assembler = VectorAssembler(
            inputCols=list(ZONE_FEATURE_COLUMNS),
            outputCol="raw_features",
            handleInvalid="error",
        )
        scaler = StandardScaler(
            inputCol="raw_features", outputCol="scaled_features", withMean=True, withStd=True
        )
        kmeans = KMeans(
            featuresCol="scaled_features",
            predictionCol="segment_id",
            k=self.settings.segmentation_k,
            seed=self.settings.seed,
            maxIter=self.settings.segmentation_max_iter,
            initMode="k-means||",
        )
        return Pipeline(stages=[assembler, scaler, kmeans])

    def run_segmentation(
        self,
        zone_features: DataFrame | None = None,
        *,
        persist: bool = True,
    ) -> ModelRunResult:
        source = self._resolve_input(
            zone_features, ZONE_FEATURE_TABLE_CANDIDATES, "features agregados de zonas"
        )
        features = self._prepare_zone_features(source).cache()
        zone_count = self._require_rows(
            features, "Segmentacion de zonas", minimum=self.settings.segmentation_k
        )
        distinct_vectors = features.select(*ZONE_FEATURE_COLUMNS).dropDuplicates().count()
        if distinct_vectors < self.settings.segmentation_k:
            raise ValueError(
                "KMeans requiere al menos k vectores distintos: "
                f"k={self.settings.segmentation_k}, distintos={distinct_vectors}"
            )

        model = self._segmentation_pipeline().fit(features)
        transformed = model.transform(features).cache()
        cluster_count = transformed.select("segment_id").distinct().count()
        if cluster_count < 2:
            raise ValueError(
                "KMeans produjo menos de dos clusters; no se puede calcular silhouette"
            )
        silhouette = _finite(
            ClusteringEvaluator(
                featuresCol="scaled_features",
                predictionCol="segment_id",
                metricName="silhouette",
                distanceMeasure="squaredEuclidean",
            ).evaluate(transformed)
        )

        profile_aggregations = [
            F.count(F.lit(1)).alias("zone_count"),
            F.sum("total_trips").alias("total_trips"),
            *[
                F.avg(column).alias(f"avg_{column}")
                for column in ZONE_FEATURE_COLUMNS
                if column != "total_trips"
            ],
        ]
        profiles = transformed.groupBy("segment_id").agg(*profile_aggregations)
        demand_rank = Window.orderBy(F.desc("total_trips"), F.asc("segment_id"))
        profiles = profiles.withColumn("demand_rank", F.row_number().over(demand_rank)).withColumn(
            "segment_label",
            F.when(F.col("demand_rank") == 1, F.lit("Muy alta demanda"))
            .when(F.col("demand_rank") == 2, F.lit("Alta demanda"))
            .when(F.col("demand_rank") == 3, F.lit("Demanda media"))
            .otherwise(F.lit("Baja demanda")),
        )
        network_total = _finite(profiles.agg(F.sum("total_trips").alias("total")).first()["total"])
        profiles = profiles.withColumn(
            "trip_share",
            F.when(F.lit(network_total) > 0, F.col("total_trips") / F.lit(network_total)).otherwise(
                F.lit(0.0)
            ),
        ).withColumn("model_name", F.lit("kmeans_zones"))

        segments = (
            transformed.drop("raw_features", "scaled_features")
            .join(
                profiles.select("segment_id", "segment_label", "demand_rank"),
                "segment_id",
                "left",
            )
            .withColumn("segment_id", F.col("segment_id").cast("int"))
            .withColumn("pickup_location_id", F.col("zone_id").cast("int"))
            .withColumn("model_name", F.lit("kmeans_zones"))
            .withColumn("generated_at_utc", F.current_timestamp())
        )
        if "zone_name" in segments.columns:
            segments = segments.withColumn("pickup_zone", F.col("zone_name"))
        if "borough" in segments.columns:
            segments = segments.withColumn("pickup_borough", F.col("borough"))
        profiles = profiles.withColumn("segment_id", F.col("segment_id").cast("int")).withColumn(
            "generated_at_utc", F.current_timestamp()
        )
        metrics: dict[str, Any] = {
            "silhouette": silhouette,
            "k": self.settings.segmentation_k,
            "zones": zone_count,
            "clusters_produced": cluster_count,
            "seed": self.settings.seed,
        }
        metrics_frame = self._metrics_frame("kmeans_zones", metrics)
        frames = {
            MODEL_SEGMENTATION_TABLE: segments,
            MODEL_SEGMENTATION_PROFILES_TABLE: profiles,
            MODEL_SEGMENTATION_METRICS_TABLE: metrics_frame,
        }
        paths: dict[str, str] = {}
        if persist:
            paths["spark_model"] = self._save_model(model, "zone_segmentation")
            for table_name, frame in frames.items():
                paths.update(self._write_frame(frame, table_name))
        self._record_model("kmeans_zones", metrics, paths)
        transformed.unpersist(blocking=False)
        features.unpersist(blocking=False)
        return ModelRunResult("kmeans_zones", metrics, frames, paths)

    def _prepare_zone_daily(self, source: DataFrame) -> DataFrame:
        date_column = _resolve_column(
            source,
            ("trip_date", "pickup_date", "date", "day", "service_date"),
            purpose="fecha diaria por zona",
        )
        zone_column = _resolve_column(
            source,
            (
                "zone_id",
                "location_id",
                "pickup_zone_id",
                "pickup_location_id",
                "pulocationid",
                "PULocationID",
            ),
            purpose="identificador de zona diaria",
        )
        trips_column = _resolve_column(
            source,
            (
                "trip_count",
                "total_trips",
                "trips",
                "ride_count",
                "pickup_count",
                "trips_total",
            ),
            purpose="demanda diaria por zona",
        )
        zone_name_column = _resolve_column(
            source,
            ("zone_name", "zone", "pickup_zone", "location_name"),
            required=False,
        )
        expressions = [
            F.to_date(F.col(date_column)).alias("trip_date"),
            F.col(zone_column).cast("string").alias("zone_id"),
            F.col(trips_column).cast("double").alias("trip_count"),
        ]
        if zone_name_column:
            expressions.append(F.col(zone_name_column).cast("string").alias("zone_name"))
        selected = source.select(*expressions).where(
            F.col("trip_date").isNotNull()
            & F.col("zone_id").isNotNull()
            & F.col("trip_count").isNotNull()
            & (F.col("trip_count") >= 0)
        )
        aggregations = [F.sum("trip_count").alias("trip_count")]
        if "zone_name" in selected.columns:
            aggregations.append(F.first("zone_name", ignorenulls=True).alias("zone_name"))
        return selected.groupBy("trip_date", "zone_id").agg(*aggregations)

    def _classification_pipeline(self) -> Pipeline:
        indexer = StringIndexer(
            inputCol="zone_id",
            outputCol="zone_index",
            handleInvalid="keep",
            stringOrderType="alphabetAsc",
        )
        encoder = OneHotEncoder(
            inputCols=["zone_index"],
            outputCols=["zone_vector"],
            handleInvalid="keep",
            dropLast=False,
        )
        assembler = VectorAssembler(
            inputCols=[*TIME_FEATURE_COLUMNS, "zone_vector"],
            outputCol="features",
            handleInvalid="error",
        )
        classifier = RandomForestClassifier(
            labelCol="label",
            featuresCol="features",
            predictionCol="prediction",
            probabilityCol="probability",
            rawPredictionCol="rawPrediction",
            weightCol="class_weight",
            seed=self.settings.seed,
            numTrees=self.settings.classification_num_trees,
            maxDepth=self.settings.classification_max_depth,
            subsamplingRate=1.0,
            featureSubsetStrategy="sqrt",
        )
        return Pipeline(stages=[indexer, encoder, assembler, classifier])

    def run_classification(
        self,
        zone_daily: DataFrame | None = None,
        *,
        persist: bool = True,
    ) -> ModelRunResult:
        source = self._resolve_input(
            zone_daily, ZONE_DAILY_TABLE_CANDIDATES, "demanda diaria por zona"
        )
        daily = self._prepare_zone_daily(source)
        names = (
            daily.select("zone_id", "zone_name").dropDuplicates(["zone_id"])
            if "zone_name" in daily.columns
            else None
        )
        panel = self._complete_daily_panel(
            daily.select("zone_id", "trip_date", "trip_count"),
            date_column="trip_date",
            entity_column="zone_id",
            value_column="trip_count",
        ).localCheckpoint(eager=True)
        feature_frame = (
            self._add_time_features(
                panel,
                date_column="trip_date",
                entity_column="zone_id",
                value_column="trip_count",
            )
            .dropna(subset=list(TIME_FEATURE_COLUMNS))
            .cache()
        )
        self._require_rows(feature_frame, "Clasificacion despues de lags", minimum=10)
        train_base, test_base = self._temporal_split(
            feature_frame,
            date_column="trip_date",
            entity_column="zone_id",
            test_days=self.settings.classification_test_days,
        )
        threshold_row = (
            train_base.where(F.col("trip_count") > 0)
            .agg(
                F.percentile_approx(
                    F.col("trip_count"), self.settings.high_demand_quantile, 10_000
                ).alias("threshold")
            )
            .first()
        )
        threshold = _finite(threshold_row["threshold"] if threshold_row else None)
        if threshold <= 0:
            raise ValueError("No se pudo calcular un umbral positivo de alta demanda")

        label_expression = F.when(F.col("trip_count") >= F.lit(threshold), F.lit(1.0)).otherwise(
            F.lit(0.0)
        )
        train_labeled = train_base.withColumn("label", label_expression)
        test_labeled = test_base.withColumn("label", label_expression)
        class_counts = {
            int(row["label"]): int(row["count"])
            for row in train_labeled.groupBy("label").count().collect()
        }
        if set(class_counts) != {0, 1}:
            raise ValueError(
                f"El entrenamiento temporal necesita ambas clases; conteos={class_counts}"
            )
        total_train = sum(class_counts.values())
        negative_weight = total_train / (2.0 * class_counts[0])
        positive_weight = total_train / (2.0 * class_counts[1])
        train = train_labeled.withColumn(
            "class_weight",
            F.when(F.col("label") == 1.0, F.lit(positive_weight)).otherwise(F.lit(negative_weight)),
        ).cache()
        test = test_labeled.withColumn("class_weight", F.lit(1.0)).cache()
        train_rows = self._require_rows(train, "Entrenamiento de clasificacion", minimum=2)
        test_rows = self._require_rows(test, "Prueba temporal de clasificacion", minimum=2)
        test_classes = {int(row["label"]) for row in test.select("label").distinct().collect()}
        if test_classes != {0, 1}:
            raise ValueError(f"El holdout temporal necesita ambas clases; clases={test_classes}")

        model = self._classification_pipeline().fit(train)
        prediction = model.transform(test).cache()
        auc = _finite(
            BinaryClassificationEvaluator(
                labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
            ).evaluate(prediction)
        )
        multi = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")
        accuracy = _finite(multi.setMetricName("accuracy").evaluate(prediction))
        precision = _finite(multi.setMetricName("weightedPrecision").evaluate(prediction))
        recall = _finite(multi.setMetricName("weightedRecall").evaluate(prediction))
        f1 = _finite(multi.setMetricName("f1").evaluate(prediction))

        confusion_counts = prediction.groupBy(
            F.col("label").cast("int").alias("actual_label"),
            F.col("prediction").cast("int").alias("predicted_label"),
        ).agg(F.count(F.lit(1)).alias("count"))
        combinations = self.spark.createDataFrame(
            [(0, 0), (0, 1), (1, 0), (1, 1)], ["actual_label", "predicted_label"]
        )
        confusion = (
            combinations.join(confusion_counts, ["actual_label", "predicted_label"], "left")
            .fillna(0, subset=["count"])
            .withColumn("count", F.col("count").cast("long"))
            .withColumn("model_name", F.lit("random_forest_high_demand"))
            .withColumn("generated_at_utc", F.current_timestamp())
        )
        result_columns = [
            F.col("trip_date").alias("prediction_date"),
            "zone_id",
            F.col("trip_count").alias("actual_trips"),
            F.lit(threshold).alias("high_demand_threshold"),
            F.col("label").cast("int").alias("actual_high_demand"),
            F.col("prediction").cast("int").alias("predicted_high_demand"),
            vector_to_array("probability")[1].alias("probability_high_demand"),
            F.col("_cutoff_date").alias("temporal_cutoff"),
        ]
        predictions = prediction.select(*result_columns)
        if names is not None:
            predictions = predictions.join(names, "zone_id", "left")
        predictions = (
            predictions.withColumn("model_name", F.lit("random_forest_high_demand"))
            .withColumn("dataset_split", F.lit("temporal_test"))
            .withColumn("pickup_date", F.col("prediction_date"))
            .withColumn("pickup_location_id", F.col("zone_id").cast("int"))
            .withColumn("label", F.col("actual_high_demand").cast("double"))
            .withColumn("prediction", F.col("predicted_high_demand").cast("double"))
            .withColumn("generated_at_utc", F.current_timestamp())
        )

        cutoff = test.agg(F.min("_cutoff_date").alias("cutoff")).first()["cutoff"]
        metrics: dict[str, Any] = {
            "auc_roc": auc,
            "accuracy": accuracy,
            "weighted_precision": precision,
            "weighted_recall": recall,
            "f1": f1,
            "high_demand_threshold": threshold,
            "high_demand_quantile": self.settings.high_demand_quantile,
            "train_rows": train_rows,
            "test_rows": test_rows,
            "temporal_cutoff": cutoff,
            "num_trees": self.settings.classification_num_trees,
            "seed": self.settings.seed,
        }
        metrics_frame = self._metrics_frame("random_forest_high_demand", metrics)
        frames = {
            MODEL_CLASSIFICATION_TABLE: predictions,
            MODEL_CLASSIFICATION_METRICS_TABLE: metrics_frame,
            MODEL_CLASSIFICATION_CONFUSION_TABLE: confusion,
        }
        paths: dict[str, str] = {}
        if persist:
            paths["spark_model"] = self._save_model(model, "high_demand_classifier")
            for table_name, frame in frames.items():
                paths.update(self._write_frame(frame, table_name))
        self._record_model("random_forest_high_demand", metrics, paths)
        prediction.unpersist(blocking=False)
        train.unpersist(blocking=False)
        test.unpersist(blocking=False)
        feature_frame.unpersist(blocking=False)
        return ModelRunResult("random_forest_high_demand", metrics, frames, paths)

    def run_all(
        self,
        *,
        daily_service: DataFrame | None = None,
        zone_daily: DataFrame | None = None,
        zone_features: DataFrame | None = None,
        persist: bool = True,
    ) -> dict[str, ModelRunResult]:
        results = {
            "time_series": self.run_time_series(daily_service, persist=persist),
            "segmentation": self.run_segmentation(
                zone_features if zone_features is not None else zone_daily, persist=persist
            ),
            "classification": self.run_classification(zone_daily, persist=persist),
        }
        if persist:
            metrics_frames = [
                result.frames[table_name]
                for result, table_name in (
                    (results["time_series"], MODEL_TIMESERIES_METRICS_TABLE),
                    (results["segmentation"], MODEL_SEGMENTATION_METRICS_TABLE),
                    (results["classification"], MODEL_CLASSIFICATION_METRICS_TABLE),
                )
            ]
            all_metrics = metrics_frames[0]
            for frame in metrics_frames[1:]:
                all_metrics = all_metrics.unionByName(frame)
            self._write_frame(all_metrics, MODEL_METRICS_TABLE)
        return results


def run_models(
    spark: SparkSession,
    config: PipelineConfig,
    *,
    audit: AuditLogger | None = None,
    run_id: str | None = None,
) -> dict[str, ModelRunResult]:
    """Punto de entrada para CLI/orquestacion del pipeline completo."""

    return PredictiveModelsPipeline.from_config(spark, config, audit=audit, run_id=run_id).run_all()
