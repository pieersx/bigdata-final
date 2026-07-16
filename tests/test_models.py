from __future__ import annotations

import math
from datetime import date, timedelta
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from tlc_pipeline.models import (
    MODEL_CLASSIFICATION_CONFUSION_TABLE,
    MODEL_CLASSIFICATION_TABLE,
    MODEL_SEGMENTATION_PROFILES_TABLE,
    MODEL_SEGMENTATION_TABLE,
    MODEL_TIMESERIES_TABLE,
    ModelSettings,
    PredictiveModelsPipeline,
)

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def spark(tmp_path_factory: pytest.TempPathFactory) -> SparkSession:
    warehouse = tmp_path_factory.mktemp("spark-warehouse")
    session = (
        SparkSession.builder.master("local[2]")
        .appName("tlc-model-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def settings() -> ModelSettings:
    return ModelSettings(
        seed=17,
        forecast_horizon_days=30,
        forecast_test_days=21,
        forecast_max_iter=5,
        forecast_max_depth=3,
        segmentation_k=4,
        segmentation_max_iter=12,
        classification_test_days=21,
        classification_num_trees=12,
        classification_max_depth=5,
        high_demand_quantile=0.75,
        csv_single_file=True,
    )


def make_pipeline(
    spark: SparkSession,
    tmp_path: Path,
    settings: ModelSettings,
) -> PredictiveModelsPipeline:
    return PredictiveModelsPipeline(
        spark,
        gold_path=tmp_path / "gold",
        models_path=tmp_path / "artifacts" / "models",
        exports_path=tmp_path / "exports",
        settings=settings,
    )


def synthetic_daily_service(spark: SparkSession, days: int = 150):
    start = date(2025, 1, 1)
    rows = []
    for offset in range(days):
        current = start + timedelta(days=offset)
        weekend = current.weekday() >= 5
        monthly_wave = 11.0 * math.sin(2.0 * math.pi * offset / 30.0)
        for service, base in (("yellow", 180.0), ("green", 95.0)):
            service_trend = (0.20 if service == "yellow" else 0.08) * offset
            weekly_effect = -28.0 if weekend else 13.0
            trips = max(1.0, base + service_trend + weekly_effect + monthly_wave)
            rows.append((current, service, float(round(trips, 3))))
    return spark.createDataFrame(rows, ["pickup_date", "service", "trip_count"])


def synthetic_zone_daily(spark: SparkSession, days: int = 150):
    start = date(2025, 1, 1)
    rows = []
    demand_bands = (24.0, 72.0, 155.0, 315.0)
    for offset in range(days):
        current = start + timedelta(days=offset)
        weekend_factor = 0.77 if current.weekday() >= 5 else 1.08
        seasonal = 1.0 + 0.10 * math.sin(2.0 * math.pi * offset / 14.0)
        for zone in range(1, 13):
            band = (zone - 1) // 3
            total = max(
                1,
                int(
                    demand_bands[band] * weekend_factor * seasonal + (zone % 3) * 4 + 0.04 * offset
                ),
            )
            for service, share in (("yellow", 0.72), ("green", 0.28)):
                trips = max(1, int(round(total * share)))
                rows.append(
                    (
                        current,
                        service,
                        zone,
                        f"Zone {zone:03d}",
                        "Synthetic Borough",
                        trips,
                        float(trips * (9.0 + band * 1.5)),
                        float(1.2 + band * 0.9),
                        float(8.0 + band * 3.5),
                    )
                )
    return spark.createDataFrame(
        rows,
        [
            "pickup_date",
            "service",
            "pickup_location_id",
            "pickup_zone",
            "pickup_borough",
            "trip_count",
            "total_revenue",
            "avg_trip_distance",
            "avg_trip_duration_minutes",
        ],
    )


def test_time_series_forecasts_exactly_30_days_for_every_service(
    spark: SparkSession,
    tmp_path: Path,
    settings: ModelSettings,
) -> None:
    source = synthetic_daily_service(spark)
    pipeline = make_pipeline(spark, tmp_path, settings)

    result = pipeline.run_time_series(source, persist=False)
    forecast = result.frames[MODEL_TIMESERIES_TABLE]

    assert forecast.count() == 2 * 30
    assert forecast.select("service_type").distinct().count() == 2
    assert forecast.agg(F.min("horizon_day"), F.max("horizon_day")).first() == (1, 30)
    assert forecast.where(F.col("forecast_trips") < 0).count() == 0
    assert forecast.where(F.col("forecast_lower_95") < 0).count() == 0
    assert result.metrics["train_rows"] > result.metrics["test_rows"] > 0
    assert result.metrics["rmse"] >= 0
    assert result.metrics["mae"] >= 0
    assert math.isfinite(result.metrics["r2"])


def test_kmeans_uses_all_zone_rows_and_persists_model_and_outputs(
    spark: SparkSession,
    tmp_path: Path,
    settings: ModelSettings,
) -> None:
    source = synthetic_zone_daily(spark)
    source_total = float(source.agg(F.sum("trip_count")).first()[0])
    pipeline = make_pipeline(spark, tmp_path, settings)

    result = pipeline.run_segmentation(source, persist=True)
    segments = result.frames[MODEL_SEGMENTATION_TABLE]
    profiles = result.frames[MODEL_SEGMENTATION_PROFILES_TABLE]

    assert segments.count() == 12
    assert segments.select("segment_id").distinct().count() == 4
    assert profiles.count() == 4
    assert float(segments.agg(F.sum("total_trips")).first()[0]) == pytest.approx(source_total)
    assert result.metrics["silhouette"] > 0
    assert (tmp_path / "artifacts" / "models" / "zone_segmentation" / "metadata").exists()
    assert (tmp_path / "gold" / MODEL_SEGMENTATION_TABLE).is_dir()
    assert (tmp_path / "exports" / MODEL_SEGMENTATION_TABLE).is_dir()


def test_random_forest_temporal_holdout_reports_complete_metrics_and_confusion(
    spark: SparkSession,
    tmp_path: Path,
    settings: ModelSettings,
) -> None:
    source = synthetic_zone_daily(spark)
    pipeline = make_pipeline(spark, tmp_path, settings)

    result = pipeline.run_classification(source, persist=False)
    predictions = result.frames[MODEL_CLASSIFICATION_TABLE]
    confusion = result.frames[MODEL_CLASSIFICATION_CONFUSION_TABLE]
    prediction_count = predictions.count()

    assert prediction_count == result.metrics["test_rows"]
    assert predictions.select("dataset_split").first()[0] == "temporal_test"
    assert predictions.select("actual_high_demand").distinct().count() == 2
    assert confusion.count() == 4
    assert confusion.agg(F.sum("count")).first()[0] == prediction_count
    for metric in ("auc_roc", "accuracy", "weighted_precision", "weighted_recall", "f1"):
        assert 0.0 <= result.metrics[metric] <= 1.0
    assert result.metrics["high_demand_threshold"] > 0
    assert result.metrics["train_rows"] > result.metrics["test_rows"] > 0
