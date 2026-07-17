from __future__ import annotations

from pathlib import Path

import nbformat

EXPECTED = {
    "00_ingesta_automatica_2026.ipynb",
    "01_capa_bronze.ipynb",
    "02_capa_silver.ipynb",
    "03_capa_gold.ipynb",
    "04_modelos_predictivos.ipynb",
    "05_flujo_auditoria.ipynb",
    "06_power_bi.ipynb",
}


def test_notebooks_are_valid_and_presentation_ready() -> None:
    root = Path(__file__).parents[1] / "notebooks"
    assert {path.name for path in root.glob("*.ipynb")} == EXPECTED

    required_sections = ("Objetivo", "Preparación", "Pasos", "Comprobaciones", "Siguiente paso")
    for path in root.glob("*.ipynb"):
        notebook = nbformat.read(path, as_version=4)
        nbformat.validate(notebook)
        markdown = "\n".join(cell.source for cell in notebook.cells if cell.cell_type == "markdown")
        code = "\n".join(cell.source for cell in notebook.cells if cell.cell_type == "code")
        assert all(section in markdown for section in required_sections), path.name
        assert "QUÉ EXPLICAR" in markdown, path.name
        assert "#" in code, path.name
        assert notebook.metadata.kernelspec.name == "python3"
