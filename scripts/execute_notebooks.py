from __future__ import annotations

import argparse
from pathlib import Path

import nbformat
from nbclient import NotebookClient

ROOT = Path(__file__).parents[1]


def main() -> int:
    parser = argparse.ArgumentParser(description="Ejecuta y valida los notebooks de exposición")
    parser.add_argument("names", nargs="*", help="Nombres sin ruta; por defecto ejecuta todos")
    parser.add_argument("--timeout", type=int, default=900, help="Segundos máximos por celda")
    args = parser.parse_args()

    source = ROOT / "notebooks"
    output = ROOT / "artifacts" / "notebooks-executed"
    output.mkdir(parents=True, exist_ok=True)
    paths = [source / name for name in args.names] if args.names else sorted(source.glob("*.ipynb"))

    for path in paths:
        print(f"Ejecutando {path.name}...", flush=True)
        notebook = nbformat.read(path, as_version=4)
        client = NotebookClient(
            notebook,
            timeout=args.timeout,
            kernel_name="python3",
            resources={"metadata": {"path": str(ROOT)}},
        )
        client.execute(cwd=str(ROOT))
        nbformat.validate(notebook)
        nbformat.write(notebook, output / path.name)
        print(f"OK: {path.name}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
