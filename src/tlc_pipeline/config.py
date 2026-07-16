"""Carga y validacion de la configuracion del pipeline TLC.

El modulo mantiene la configuracion como un diccionario (para que sea sencillo
serializarla en la auditoria), pero ofrece accesos tipados a secciones, claves
con notacion de puntos y rutas.  No hay valores de datos inventados: el YAML es
la unica fuente de configuracion y los overrides de entorno son explicitos.
"""

from __future__ import annotations

import copy
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


class ConfigError(ValueError):
    """La configuracion no existe o no satisface el contrato minimo."""


_MISSING = object()


@dataclass(frozen=True, slots=True)
class PipelineConfig:
    """Configuracion validada del pipeline.

    ``raw`` se conserva como ``dict`` por compatibilidad con PySpark, MongoDB y
    herramientas de linea de comandos. ``get`` acepta claves como
    ``"source.download_workers"`` y ``path`` resuelve una ruta de ``paths``.
    """

    raw: dict[str, Any]
    config_file: Path

    @property
    def source(self) -> Mapping[str, Any]:
        return self.section("source")

    @property
    def paths(self) -> Mapping[str, Any]:
        return self.section("paths")

    @property
    def spark(self) -> Mapping[str, Any]:
        return self.section("spark")

    @property
    def quality(self) -> Mapping[str, Any]:
        return self.section("quality")

    @property
    def audit(self) -> Mapping[str, Any]:
        return self.section("audit")

    def section(self, name: str) -> Mapping[str, Any]:
        value = self.raw.get(name)
        if not isinstance(value, dict):
            raise ConfigError(f"La seccion '{name}' no existe o no es un mapping")
        return value

    def get(self, dotted_key: str, default: Any = None) -> Any:
        """Obtiene una clave anidada mediante notacion de puntos."""

        current: Any = self.raw
        for part in dotted_key.split("."):
            if not isinstance(current, Mapping) or part not in current:
                return default
            current = current[part]
        return current

    def require(self, dotted_key: str) -> Any:
        value = self.get(dotted_key, _MISSING)
        if value is _MISSING:
            raise ConfigError(f"Falta la clave obligatoria '{dotted_key}'")
        return value

    def path(self, name: str, *, create: bool = False) -> Path:
        """Devuelve una ruta configurada, expandida y absoluta.

        Las rutas relativas se interpretan respecto del directorio que contiene
        el YAML, no respecto del directorio desde el que se invoque el comando.
        """

        raw_path = self.require(f"paths.{name}")
        if not isinstance(raw_path, str | os.PathLike):
            raise ConfigError(f"paths.{name} debe ser una ruta")
        expanded = Path(os.path.expandvars(os.path.expanduser(str(raw_path))))
        if not expanded.is_absolute():
            expanded = (self.config_file.parent / expanded).resolve()
        if create:
            expanded.mkdir(parents=True, exist_ok=True)
        return expanded

    def as_dict(self) -> dict[str, Any]:
        """Copia defensiva apta para serializar o pasar a otro proceso."""

        return copy.deepcopy(self.raw)


def _default_config_path() -> Path:
    explicit = os.getenv("TLC_PIPELINE_CONFIG")
    if explicit:
        return Path(explicit).expanduser()

    cwd_candidate = Path.cwd() / "config" / "pipeline.yaml"
    if cwd_candidate.is_file():
        return cwd_candidate

    # src/tlc_pipeline/config.py -> raiz del proyecto
    package_candidate = Path(__file__).resolve().parents[2] / "config" / "pipeline.yaml"
    return package_candidate


def _apply_environment_overrides(raw: dict[str, Any], env: Mapping[str, str]) -> None:
    paths = raw.setdefault("paths", {})
    raw.setdefault("audit", {})
    source = raw.setdefault("source", {})

    data_root = env.get("TLC_DATA_ROOT")
    if data_root:
        root = Path(data_root).expanduser()
        paths.update(
            {
                "root": str(root),
                "bronze": str(root / "bronze"),
                "silver": str(root / "silver"),
                "quarantine": str(root / "quarantine"),
                "gold": str(root / "gold"),
                "temp": str(root / "tmp"),
            }
        )

    scalar_overrides: tuple[tuple[str, str, str], ...] = (
        ("TLC_BRONZE_PATH", "paths", "bronze"),
        ("TLC_SILVER_PATH", "paths", "silver"),
        ("TLC_QUARANTINE_PATH", "paths", "quarantine"),
        ("TLC_GOLD_PATH", "paths", "gold"),
        ("TLC_TEMP_PATH", "paths", "temp"),
        ("TLC_MODELS_PATH", "paths", "models"),
        ("TLC_EXPORTS_PATH", "paths", "exports"),
        ("TLC_LOGS_PATH", "paths", "logs"),
        ("TLC_AUDIT_JSONL", "audit", "local_jsonl"),
        ("TLC_SOURCE_BASE_URL", "source", "base_url"),
    )
    for env_name, section_name, key in scalar_overrides:
        if env_name in env:
            raw.setdefault(section_name, {})[key] = env[env_name]

    if workers := env.get("TLC_DOWNLOAD_WORKERS"):
        try:
            source["download_workers"] = int(workers)
        except ValueError as exc:
            raise ConfigError("TLC_DOWNLOAD_WORKERS debe ser un entero") from exc


def _validate(raw: dict[str, Any]) -> None:
    required_sections = ("source", "paths", "spark", "quality", "models", "audit")
    for section in required_sections:
        if not isinstance(raw.get(section), dict):
            raise ConfigError(f"Falta la seccion obligatoria '{section}'")

    source = raw["source"]
    for key in ("base_url", "zone_lookup_url", "services", "historical_years", "current_year"):
        if key not in source:
            raise ConfigError(f"Falta la clave obligatoria 'source.{key}'")

    services = source["services"]
    if (
        not isinstance(services, list)
        or not services
        or not all(isinstance(service, str) and service.strip() for service in services)
    ):
        raise ConfigError("source.services debe ser una lista no vacia de nombres")
    if len(set(services)) != len(services):
        raise ConfigError("source.services contiene duplicados")

    years = source["historical_years"]
    if not isinstance(years, list) or not years or not all(isinstance(year, int) for year in years):
        raise ConfigError("source.historical_years debe ser una lista no vacia de enteros")
    if len(set(years)) != len(years):
        raise ConfigError("source.historical_years contiene duplicados")
    current_year = source["current_year"]
    if not isinstance(current_year, int):
        raise ConfigError("source.current_year debe ser un entero")
    if current_year in years:
        raise ConfigError("source.current_year no debe repetirse en historical_years")

    positive_integer_keys = (
        "connect_timeout_seconds",
        "read_timeout_seconds",
        "discovery_timeout_seconds",
        "retries",
        "chunk_size_bytes",
        "download_workers",
        "discovery_workers",
        "throttle_retries",
        "throttle_backoff_seconds",
    )
    for key in positive_integer_keys:
        value = source.get(key)
        if not isinstance(value, int) or value <= 0:
            raise ConfigError(f"source.{key} debe ser un entero positivo")

    silver_workers = raw["spark"].get("silver_file_workers", 1)
    if not isinstance(silver_workers, int) or silver_workers <= 0:
        raise ConfigError("spark.silver_file_workers debe ser un entero positivo")

    required_paths = ("root", "bronze", "silver", "quarantine", "gold", "temp")
    for key in required_paths:
        if not isinstance(raw["paths"].get(key), str) or not raw["paths"][key]:
            raise ConfigError(f"paths.{key} debe ser una ruta no vacia")


def load_config(
    path: str | os.PathLike[str] | None = None,
    *,
    env: Mapping[str, str] | None = None,
) -> PipelineConfig:
    """Carga ``pipeline.yaml``, aplica overrides seguros y valida el contrato."""

    config_path = Path(path) if path is not None else _default_config_path()
    config_path = config_path.expanduser().resolve()
    if not config_path.is_file():
        raise ConfigError(f"No se encontro la configuracion: {config_path}")

    try:
        loaded = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ConfigError(f"YAML invalido en {config_path}: {exc}") from exc
    if not isinstance(loaded, dict):
        raise ConfigError(f"La raiz de {config_path} debe ser un mapping")

    raw = copy.deepcopy(loaded)
    _apply_environment_overrides(raw, os.environ if env is None else env)
    _validate(raw)
    return PipelineConfig(raw=raw, config_file=config_path)
