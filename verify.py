#!/usr/bin/env python3
"""Verifica la integridad de la distribución Parquet de Bicentenario UC 2026.

Uso desde la raíz del repositorio:
    python verify.py
    python verify.py --manifest data/manifest.json

Dependencia:
    python -m pip install pyarrow

El verificador comprueba:
- SHA-256 de cada archivo Parquet contra manifest.json.
- Número de filas y columnas registrado en el manifiesto.
- Integridad de los metadatos Parquet.
- Controles específicos de esta distribución: 14 regiones, UTF-8 sin U+FFFD,
  ponderador POND positivo/no nulo y suma de POND próxima a 1.594.
- Etiquetas regionales en categorias.parquet, cuando están disponibles.

No modifica ni reescribe los archivos.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from pathlib import Path
from typing import Any

EXPECTED_TABLES = {"respuestas", "variables", "categorias", "meta"}
EXPECTED_REGIONS = {
    "Antofagasta",
    "Arica y Parinacota",
    "Atacama",
    "Biobío",
    "Coquimbo",
    "La Araucanía",
    "Los Lagos",
    "Los Ríos",
    "Maule",
    "Metropolitana",
    "O'Higgins",
    "Tarapacá",
    "Valparaíso",
    "Ñuble",
}
REPLACEMENT_CHARACTER = "\uFFFD"


class VerificationError(Exception):
    """Error esperado durante la validación del repositorio."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def find_manifest(repo_root: Path, requested: str | None) -> Path:
    if requested:
        path = Path(requested)
        if not path.is_absolute():
            path = repo_root / path
        if path.is_file():
            return path.resolve()
        raise VerificationError(f"No se encontró el manifiesto: {path}")

    candidates = [repo_root / "data" / "manifest.json", repo_root / "manifest.json"]
    for candidate in candidates:
        if candidate.is_file():
            return candidate.resolve()

    raise VerificationError(
        "No se encontró manifest.json. Se buscó en data/manifest.json y manifest.json."
    )


def resolve_data_file(repo_root: Path, manifest_path: Path, filename: str) -> Path:
    # El manifiesto debe contener solo nombres de archivo, no rutas arbitrarias.
    if not filename or Path(filename).name != filename or filename in {".", ".."}:
        raise VerificationError(f"Nombre de archivo inseguro en el manifiesto: {filename!r}")

    candidates = [
        manifest_path.parent / filename,
        repo_root / "data" / filename,
        repo_root / filename,
    ]
    seen: set[Path] = set()
    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        if resolved.is_file():
            return resolved

    raise VerificationError(
        f"No se encontró {filename}. Se esperaba junto al manifiesto o dentro de data/."
    )


def load_pyarrow():
    try:
        import pyarrow as pa  # type: ignore
        import pyarrow.parquet as pq  # type: ignore
    except ImportError as exc:
        raise VerificationError(
            "Falta PyArrow. Instálalo con: python -m pip install pyarrow"
        ) from exc
    return pa, pq


def check_manifest_structure(manifest: Any) -> dict[str, Any]:
    if not isinstance(manifest, dict):
        raise VerificationError("El manifiesto debe ser un objeto JSON.")

    tables = manifest.get("tablas")
    if not isinstance(tables, dict):
        raise VerificationError("El manifiesto no contiene el objeto 'tablas'.")

    missing = EXPECTED_TABLES - set(tables)
    if missing:
        raise VerificationError(
            "Faltan tablas obligatorias en el manifiesto: " + ", ".join(sorted(missing))
        )

    return tables


def verify_table_metadata(
    table_name: str,
    entry: dict[str, Any],
    repo_root: Path,
    manifest_path: Path,
    pq: Any,
) -> tuple[Path, Any]:
    required = ("archivo", "filas", "columnas", "sha256")
    missing = [key for key in required if key not in entry]
    if missing:
        raise VerificationError(
            f"La entrada '{table_name}' no contiene: {', '.join(missing)}"
        )

    filename = str(entry["archivo"])
    path = resolve_data_file(repo_root, manifest_path, filename)

    expected_hash = str(entry["sha256"]).lower()
    if len(expected_hash) != 64 or any(ch not in "0123456789abcdef" for ch in expected_hash):
        raise VerificationError(f"SHA-256 inválido en el manifiesto para {filename}.")

    actual_hash = sha256_file(path)
    if actual_hash != expected_hash:
        raise VerificationError(
            f"SHA-256 no coincide para {filename}.\n"
            f"  Esperado: {expected_hash}\n"
            f"  Obtenido: {actual_hash}"
        )

    try:
        parquet_file = pq.ParquetFile(path)
        actual_rows = parquet_file.metadata.num_rows
        actual_columns = len(parquet_file.schema_arrow.names)
    except Exception as exc:  # PyArrow puede reportar varios tipos de error de lectura.
        raise VerificationError(f"No se pudo leer {filename} como Parquet: {exc}") from exc

    try:
        expected_rows = int(entry["filas"])
        expected_columns = int(entry["columnas"])
    except (TypeError, ValueError) as exc:
        raise VerificationError(f"Dimensiones inválidas en el manifiesto para {filename}.") from exc

    if actual_rows != expected_rows or actual_columns != expected_columns:
        raise VerificationError(
            f"Dimensiones no coinciden para {filename}: "
            f"manifiesto={expected_rows}×{expected_columns}, "
            f"archivo={actual_rows}×{actual_columns}."
        )

    print(
        f"[OK] {table_name:<12} {actual_rows:>6,} filas × "
        f"{actual_columns:<3} columnas | SHA-256 correcto | {filename}"
    )
    return path, parquet_file


def verify_domain_data(paths: dict[str, Path], pq: Any) -> None:
    responses_path = paths["respuestas"]

    try:
        responses = pq.read_table(responses_path, columns=["nom_region", "POND"])
    except Exception as exc:
        raise VerificationError(
            "No se pudieron leer las columnas nom_region y POND de respuestas.parquet: "
            f"{exc}"
        ) from exc

    region_values = responses.column("nom_region").combine_chunks().to_pylist()
    pond_values = responses.column("POND").combine_chunks().to_pylist()

    null_regions = sum(value is None for value in region_values)
    damaged_regions = [
        value for value in region_values
        if isinstance(value, str) and REPLACEMENT_CHARACTER in value
    ]
    unique_regions = {value for value in region_values if value is not None}

    if null_regions:
        raise VerificationError(f"nom_region contiene {null_regions} valores nulos.")
    if damaged_regions:
        raise VerificationError(
            f"nom_region contiene {len(damaged_regions)} valores con el carácter U+FFFD."
        )

    if unique_regions != EXPECTED_REGIONS:
        missing = sorted(EXPECTED_REGIONS - unique_regions)
        unexpected = sorted(unique_regions - EXPECTED_REGIONS)
        details = []
        if missing:
            details.append("faltan: " + ", ".join(missing))
        if unexpected:
            details.append("inesperadas: " + ", ".join(map(str, unexpected)))
        raise VerificationError(
            "El conjunto de regiones no coincide con las 14 regiones esperadas ("
            + "; ".join(details)
            + ")."
        )

    if any(value is None for value in pond_values):
        raise VerificationError("POND contiene valores nulos.")

    numeric_ponds: list[float] = []
    for value in pond_values:
        try:
            number = float(value)
        except (TypeError, ValueError) as exc:
            raise VerificationError(f"POND contiene un valor no numérico: {value!r}") from exc
        if not math.isfinite(number) or number <= 0:
            raise VerificationError(f"POND contiene un valor inválido: {value!r}")
        numeric_ponds.append(number)

    pond_sum = math.fsum(numeric_ponds)
    expected_sum = float(len(region_values))
    if not math.isclose(pond_sum, expected_sum, rel_tol=0.0, abs_tol=0.01):
        raise VerificationError(
            f"La suma de POND ({pond_sum:.6f}) no está próxima al número de entrevistas "
            f"({expected_sum:.0f})."
        )

    print(
        f"[OK] Regiones      {len(unique_regions)} regiones distintas; "
        "sin nulos ni caracteres U+FFFD"
    )
    print(
        f"[OK] POND          {len(numeric_ponds):,} valores positivos y no nulos; "
        f"suma={pond_sum:.6f}"
    )

    # Comprobar las etiquetas regionales en el diccionario de categorías, si están presentes.
    categories_path = paths.get("categorias")
    if categories_path:
        try:
            categories = pq.read_table(
                categories_path, columns=["variable", "etiqueta"]
            )
            variables = categories.column("variable").combine_chunks().to_pylist()
            labels = categories.column("etiqueta").combine_chunks().to_pylist()
            region_labels = [
                label for variable, label in zip(variables, labels)
                if variable == "nom_region" and label is not None
            ]
            damaged_labels = [
                label for label in region_labels
                if isinstance(label, str) and REPLACEMENT_CHARACTER in label
            ]
            if damaged_labels:
                raise VerificationError(
                    "categorias.parquet contiene etiquetas regionales con U+FFFD."
                )
            if region_labels:
                print(
                    f"[OK] Categorías    {len(region_labels)} etiquetas de nom_region; "
                    "sin caracteres U+FFFD"
                )
            else:
                print(
                    "[AVISO] No se encontraron filas de nom_region en categorias.parquet; "
                    "se omitió ese control."
                )
        except VerificationError:
            raise
        except Exception as exc:
            raise VerificationError(
                f"No se pudieron comprobar las etiquetas regionales de categorias.parquet: {exc}"
            ) from exc


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verifica SHA-256, dimensiones y controles de calidad de los Parquet."
    )
    parser.add_argument(
        "--manifest",
        help="Ruta al manifest.json (por defecto busca data/manifest.json y luego manifest.json).",
    )
    parser.add_argument(
        "--skip-domain-checks",
        action="store_true",
        help="Omite los controles específicos de regiones y POND; mantiene SHA-256 y dimensiones.",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent

    print("Encuesta Nacional Bicentenario UC 2026")
    print("Verificador de integridad Parquet")
    print("=" * 68)

    try:
        manifest_path = find_manifest(repo_root, args.manifest)
        with manifest_path.open("r", encoding="utf-8") as stream:
            manifest = json.load(stream)
        entries = check_manifest_structure(manifest)
        _, pq = load_pyarrow()

        print(f"Manifiesto: {manifest_path.relative_to(repo_root) if manifest_path.is_relative_to(repo_root) else manifest_path}")
        print()

        resolved_paths: dict[str, Path] = {}
        for table_name in sorted(entries):
            entry = entries[table_name]
            if not isinstance(entry, dict):
                raise VerificationError(f"La entrada '{table_name}' debe ser un objeto JSON.")
            path, _ = verify_table_metadata(
                table_name, entry, repo_root, manifest_path, pq
            )
            resolved_paths[table_name] = path

        if not args.skip_domain_checks:
            print()
            verify_domain_data(resolved_paths, pq)

        print("\n" + "=" * 68)
        print("RESULTADO: TODAS LAS VALIDACIONES SUPERADAS")
        print("Los archivos no fueron modificados.")
        return 0

    except (VerificationError, json.JSONDecodeError, OSError) as exc:
        print(f"\nERROR: {exc}", file=sys.stderr)
        print("RESULTADO: VALIDACIÓN FALLIDA", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
