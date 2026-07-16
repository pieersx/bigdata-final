"""Construccion centralizada y reproducible de la sesion PySpark."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from .config import load_config
from .utils import config_get, configured_path


def create_spark_session(
    config: Any | None = None,
    *,
    app_name: str | None = None,
    master: str | None = None,
):
    """Crea (o reutiliza) Spark con los limites definidos en ``pipeline.yaml``.

    Los imports de PySpark son locales para que utilidades como catalogo e
    ingesta sigan funcionando en procesos livianos que no inicializan Java.
    """

    from pyspark.sql import SparkSession

    current_config = config or load_config(os.getenv("PIPELINE_CONFIG"))
    selected_app_name = app_name or str(
        config_get(current_config, "spark.app_name", "TLC-BigData-Final")
    )
    selected_master = (
        master
        or os.getenv("SPARK_MASTER")
        or str(config_get(current_config, "spark.master", "local[*]"))
    )

    builder = SparkSession.builder.appName(selected_app_name).master(selected_master)
    settings = {
        "spark.driver.memory": os.getenv("SPARK_DRIVER_MEMORY")
        or str(config_get(current_config, "spark.driver_memory", "6g")),
        "spark.sql.shuffle.partitions": str(
            config_get(current_config, "spark.shuffle_partitions", 48)
        ),
        "spark.default.parallelism": str(
            config_get(current_config, "spark.default_parallelism", 12)
        ),
        "spark.sql.files.maxPartitionBytes": str(
            config_get(current_config, "spark.max_partition_bytes", 134_217_728)
        ),
        "spark.sql.adaptive.enabled": str(
            bool(config_get(current_config, "spark.adaptive_enabled", True))
        ).lower(),
        "spark.sql.session.timeZone": str(
            config_get(current_config, "spark.timezone", "America/New_York")
        ),
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
        "spark.sql.parquet.mergeSchema": "false",
        "spark.sql.parquet.compression.codec": str(
            config_get(current_config, "spark.parquet_compression", "zstd")
        ),
        "spark.sql.files.ignoreCorruptFiles": "false",
        "spark.sql.legacy.timeParserPolicy": "CORRECTED",
    }

    try:
        temp_path = configured_path(current_config, "temp")
    except (KeyError, TypeError, ValueError):
        temp_path = None
    local_dirs = os.getenv("SPARK_LOCAL_DIRS")
    if local_dirs:
        settings["spark.local.dir"] = local_dirs
    elif temp_path is not None:
        Path(temp_path).mkdir(parents=True, exist_ok=True)
        settings["spark.local.dir"] = str(temp_path)

    for key, value in settings.items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark


def get_spark_session(config: Any | None = None, **kwargs: Any):
    """Alias descriptivo conservado para consumidores del pipeline."""

    return create_spark_session(config, **kwargs)
