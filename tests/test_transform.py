from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path

import pytest

from tlc_pipeline.transform import (
    canonicalize_trip_data,
    discover_bronze_files,
    transform_bronze_to_silver,
)


@pytest.fixture(scope="module")
def spark(tmp_path_factory: pytest.TempPathFactory):
    from pyspark.sql import SparkSession

    temporary = tmp_path_factory.mktemp("spark-transform")
    session = (
        SparkSession.builder.master("local[2]")
        .appName("test-tlc-transform")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.local.dir", str(temporary / "local"))
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _config(root: Path) -> dict:
    return {
        "source": {"services": ["yellow", "green", "fhv", "fhvhv"]},
        "paths": {
            "root": str(root),
            "bronze": str(root / "bronze"),
            "silver": str(root / "silver"),
            "quarantine": str(root / "quarantine"),
        },
        "quality": {
            "valid_location_min": 1,
            "valid_location_max": 265,
            "max_trip_duration_minutes": 1_440,
            "max_trip_distance_miles": 500,
            "max_total_amount": 10_000,
            "fail_on_row_loss": True,
        },
    }


def _write_as_named_parquet(dataframe, destination: Path) -> None:
    staging = destination.parent / f"_{destination.stem}_spark"
    dataframe.coalesce(1).write.mode("overwrite").parquet(str(staging))
    [part] = list(staging.glob("part-*.parquet"))
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(part, destination)
    shutil.rmtree(staging)


@pytest.mark.parametrize(
    ("service", "row", "expected_distance", "expected_total"),
    [
        (
            "yellow",
            {
                "VendorID": 1,
                "tpep_pickup_datetime": datetime(2023, 1, 2, 10, 0),
                "tpep_dropoff_datetime": datetime(2023, 1, 2, 10, 15),
                "PULocationID": 10,
                "DOLocationID": 20,
                "trip_distance": 3.5,
                "fare_amount": 14.0,
                "total_amount": 18.0,
                "Airport_fee": 1.25,
            },
            3.5,
            18.0,
        ),
        (
            "green",
            {
                "VendorID": 2,
                "lpep_pickup_datetime": datetime(2023, 1, 2, 10, 0),
                "lpep_dropoff_datetime": datetime(2023, 1, 2, 10, 20),
                "PUlocationID": 11,
                "DOlocationID": 21,
                "Trip_distance": 4.0,
                "Fare_amount": 15.0,
                "Total_amount": 19.0,
            },
            4.0,
            19.0,
        ),
        (
            "fhv",
            {
                "dispatching_base_num": "B00001",
                "pickup_datetime": datetime(2023, 1, 2, 10, 0),
                "dropOff_datetime": datetime(2023, 1, 2, 10, 25),
                "PUlocationID": 12,
                "DOlocationID": 22,
            },
            None,
            None,
        ),
        (
            "fhvhv",
            {
                "hvfhs_license_num": "HV0003",
                "pickup_datetime": datetime(2023, 1, 2, 10, 0),
                "dropoff_datetime": datetime(2023, 1, 2, 10, 30),
                "PULocationID": 13,
                "DOLocationID": 23,
                "trip_miles": 7.0,
                "base_passenger_fare": 20.0,
                "tolls": 2.0,
                "sales_tax": 1.0,
                "tips": 3.0,
            },
            7.0,
            26.0,
        ),
    ],
)
def test_canonicalizes_all_tlc_families_and_historical_cbd_is_nullable(
    spark, tmp_path, service, row, expected_distance, expected_total
):
    canonical = canonicalize_trip_data(
        spark.createDataFrame([row]),
        service=service,
        year=2023,
        month=1,
        source_file=f"{service}_tripdata_2023-01.parquet",
        config=_config(tmp_path),
    ).first()

    assert canonical.service == service
    assert canonical.pickup_location_id in {10, 11, 12, 13}
    assert canonical.trip_distance == expected_distance
    assert canonical.total_amount == expected_total
    assert canonical.cbd_congestion_fee is None
    assert canonical.dq_valid is True
    assert canonical.dq_errors == []


def test_recursive_full_transform_keeps_all_rows_and_reconciles_quarantine(spark, tmp_path):
    config = _config(tmp_path)
    bronze = Path(config["paths"]["bronze"])
    destination = bronze / "yellow" / "2023" / "yellow_tripdata_2023-01.parquet"
    rows = [
        {
            "tpep_pickup_datetime": datetime(2023, 1, 5, 8, 0),
            "tpep_dropoff_datetime": datetime(2023, 1, 5, 8, 10),
            "PULocationID": 10,
            "DOLocationID": 20,
            "trip_distance": 2.0,
            "fare_amount": 10.0,
            "total_amount": 13.0,
        },
        {
            "tpep_pickup_datetime": datetime(2023, 1, 5, 9, 0),
            "tpep_dropoff_datetime": datetime(2023, 1, 5, 9, 10),
            "PULocationID": 999,
            "DOLocationID": 20,
            "trip_distance": 2.0,
            "fare_amount": 10.0,
            "total_amount": 13.0,
        },
        {
            "tpep_pickup_datetime": datetime(2023, 1, 5, 10, 0),
            "tpep_dropoff_datetime": datetime(2023, 1, 5, 9, 59),
            "PULocationID": 10,
            "DOLocationID": 20,
            "trip_distance": 2.0,
            "fare_amount": 10.0,
            "total_amount": 13.0,
        },
    ]
    _write_as_named_parquet(spark.createDataFrame(rows), destination)
    (bronze / "yellow" / "README.parquet").write_text("no es parquet TLC", encoding="utf-8")

    discovered = discover_bronze_files(bronze)
    assert [(item.service, item.year, item.month) for item in discovered] == [("yellow", 2023, 1)]

    summary = transform_bronze_to_silver(spark, config, services=["yellow"])
    assert summary.files_processed == 1
    assert summary.source_rows == summary.silver_rows == 3
    assert summary.valid_rows == 1
    assert summary.quarantine_rows == 2
    assert summary.reconciled is True

    silver = spark.read.parquet(config["paths"]["silver"])
    quarantine = spark.read.parquet(config["paths"]["quarantine"])
    assert silver.count() == 3
    assert quarantine.count() == 2
    assert {row.dq_valid for row in silver.select("dq_valid").collect()} == {True, False}
    assert silver.where("cbd_congestion_fee is not null").count() == 0
    assert set(silver.select("service", "year", "month").first()) == {"yellow", 2023, 1}

    # Un reintento reemplaza la particion mensual; no duplica filas.
    rerun = transform_bronze_to_silver(spark, config, services=["yellow"])
    assert rerun.source_rows == 3
    assert spark.read.parquet(config["paths"]["silver"]).count() == 3
