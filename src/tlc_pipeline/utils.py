"""Utilidades compartidas por las capas Silver y Gold.

Las rutas del proyecto son locales (tambien dentro del contenedor Docker), por
lo que los helpers de exportacion convierten la salida por directorio de Spark
en un archivo real.  Esto permite que Power BI consuma directamente
``exports/<tabla>.csv`` sin pasos manuales ni archivos ``part-*`` ambiguos.
"""

from __future__ import annotations

import re
import shutil
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any

_SNAKE_CASE_1 = re.compile(r"(.)([A-Z][a-z]+)")
_SNAKE_CASE_2 = re.compile(r"([a-z0-9])([A-Z])")
_NON_WORD = re.compile(r"[^0-9A-Za-z]+")


def config_get(config: Any, dotted_key: str, default: Any = None) -> Any:
    """Lee configuracion tanto de ``PipelineConfig`` como de un mapping.

    Aceptar ambos formatos mantiene las funciones faciles de probar y evita
    duplicar configuraciones temporales en los tests de Spark.
    """

    getter = getattr(config, "get", None)
    if callable(getter) and not isinstance(config, Mapping):
        return getter(dotted_key, default)

    current: Any = config
    for part in dotted_key.split("."):
        if not isinstance(current, Mapping) or part not in current:
            return default
        current = current[part]
    return current


def configured_path(config: Any, name: str, override: str | Path | None = None) -> Path:
    """Resuelve una ruta de ``paths`` y crea un objeto ``Path`` absoluto.

    ``PipelineConfig.path`` conoce la ubicacion del YAML; para mappings usados
    en pruebas las rutas relativas se resuelven contra el directorio actual.
    """

    if override is not None:
        return Path(override).expanduser().resolve()

    path_method = getattr(config, "path", None)
    if callable(path_method):
        return Path(path_method(name)).expanduser().resolve()

    raw = config_get(config, f"paths.{name}")
    if raw is None:
        raise KeyError(f"Falta la ruta paths.{name}")
    return Path(str(raw)).expanduser().resolve()


def snake_case(value: str) -> str:
    """Normaliza nombres TLC con camelCase, espacios y casing historico."""

    text = _SNAKE_CASE_1.sub(r"\1_\2", value.strip())
    text = _SNAKE_CASE_2.sub(r"\1_\2", text)
    return _NON_WORD.sub("_", text).strip("_").lower()


def column_key(value: str) -> str:
    """Clave tolerante a casing y separadores para resolver una columna."""

    return re.sub(r"[^0-9a-z]", "", value.casefold())


def quote_spark_identifier(value: str) -> str:
    """Escapa un nombre de columna para ``pyspark.sql.functions.col``."""

    return f"`{value.replace('`', '``')}`"


def hive_partition_path(root: str | Path, service: str, year: int, month: int) -> Path:
    """Ruta Hive determinista usada por Silver y cuarentena."""

    return Path(root) / f"service={service}" / f"year={year}" / f"month={month}"


def safe_remove_tree(path: str | Path, *, allowed_root: str | Path) -> None:
    """Elimina un subdirectorio solo si esta estrictamente dentro de su raiz."""

    target = Path(path).resolve()
    root = Path(allowed_root).resolve()
    if target == root or root not in target.parents:
        raise ValueError(f"El borrado solicitado sale de la raiz permitida: {target}")
    if target.exists():
        shutil.rmtree(target)


def _replace_file_from_spark_directory(temporary: Path, destination: Path, suffix: str) -> Path:
    parts = sorted(temporary.glob(f"part-*{suffix}"))
    if len(parts) != 1:
        raise RuntimeError(
            f"Spark genero {len(parts)} archivos {suffix} en {temporary}; se esperaba uno"
        )
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.unlink(missing_ok=True)
    shutil.move(str(parts[0]), str(destination))
    shutil.rmtree(temporary, ignore_errors=True)
    return destination


def write_single_spark_file(
    dataframe: Any,
    destination: str | Path,
    *,
    file_format: str,
    header: bool = True,
) -> Path:
    """Escribe un DataFrame como un unico archivo CSV o Parquet real.

    No usa ``collect`` ni Pandas: ``coalesce(1)`` conserva el procesamiento en
    Spark y solo reduce el numero de archivos de la tabla Gold ya agregada.
    """

    destination_path = Path(destination).expanduser().resolve()
    fmt = file_format.casefold()
    if fmt not in {"csv", "parquet"}:
        raise ValueError(f"Formato de exportacion no soportado: {file_format}")
    expected_suffix = f".{fmt}"
    if destination_path.suffix.casefold() != expected_suffix:
        raise ValueError(f"El destino debe terminar en {expected_suffix}: {destination_path}")

    destination_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination_path.parent / f".{destination_path.name}.{uuid.uuid4().hex}.spark"
    shutil.rmtree(temporary, ignore_errors=True)

    writer = dataframe.coalesce(1).write.mode("overwrite")
    if fmt == "csv":
        (
            writer.option("header", str(bool(header)).lower())
            .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
            .csv(str(temporary))
        )
    else:
        writer.parquet(str(temporary))
    return _replace_file_from_spark_directory(temporary, destination_path, expected_suffix)


def export_dataframe(dataframe: Any, exports_root: str | Path, table_name: str) -> dict[str, str]:
    """Exporta una tabla completa en Parquet y CSV single-file."""

    root = Path(exports_root).expanduser().resolve()
    parquet = write_single_spark_file(
        dataframe, root / f"{table_name}.parquet", file_format="parquet"
    )
    csv = write_single_spark_file(dataframe, root / f"{table_name}.csv", file_format="csv")
    return {"parquet": str(parquet), "csv": str(csv)}
