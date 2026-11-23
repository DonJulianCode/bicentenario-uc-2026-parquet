# Encuesta Nacional Bicentenario UC 2026 — Datos en Parquet

Repositorio de distribución de la versión analítica en **Apache Parquet** de la Encuesta Nacional Bicentenario UC 2026. Los archivos se generaron a partir de la base SQLite reconstruida de Economista.cl / IndieLab y se validaron mediante comparación de contenido entre las cuatro tablas exportadas.

> **Estado de validación:** las cuatro tablas (`respuestas`, `variables`, `categorias` y `meta`) superaron la comparación de contenido SQLite → Parquet. La base contiene 1.594 entrevistas y 251 variables. Los nombres de las 14 regiones están normalizados en UTF-8.

## Contenido del repositorio

```text
.
├── README.md
├── index.html
├── verify.py
└── data/
    ├── respuestas.parquet
    ├── variables.parquet
    ├── categorias.parquet
    ├── meta.parquet
    └── manifest.json
```

### Archivos de datos

| Archivo | Registros | Columnas | Descripción |
|---|---:|---:|---|
| `respuestas.parquet` | 1.594 | 251 | Microdatos de la encuesta, incluido el ponderador `POND`. |
| `variables.parquet` | 251 | 10 | Diccionario de variables: nombres, etiquetas, bloques, enunciados, tipos y clasificación. |
| `categorias.parquet` | 1.564 | 4 | Códigos, etiquetas y marcadores de valores perdidos por variable. |
| `meta.parquet` | 12 | 2 | Metadatos de importación, trazabilidad y procedencia de la base. |
| `manifest.json` | — | — | Manifiesto de exportación con dimensiones y huellas SHA-256 de los Parquet. |

Los archivos `.parquet` utilizan compresión **Zstandard (ZSTD)**. El manifiesto permite verificar la integridad de cada archivo después de descargarlo.

### Verificar la integridad de los archivos

El script `verify.py` comprueba los hashes SHA-256 y las dimensiones de los cuatro archivos Parquet contra `data/manifest.json`. También ejecuta controles específicos de esta distribución: las 14 regiones, ausencia del carácter de reemplazo Unicode (` `) en los nombres regionales, y validez del ponderador `POND`.

Requiere Python 3.9 o posterior y PyArrow:

```bash
python -m pip install pyarrow
python verify.py
```

Si el manifiesto está en otra ubicación, puede indicarse explícitamente:

```bash
python verify.py --manifest data/manifest.json
```

El verificador es de solo lectura: no modifica ni reescribe los archivos. Termina con código de salida `0` cuando todas las comprobaciones se superan y con un código distinto de cero si detecta un problema.

## Fuente y descripción del estudio

**Fuente original:** Dirección de Estudios Sociales, Pontificia Universidad Católica de Chile, *Encuesta Nacional Bicentenario UC 2026*, versión 2.0, Datos de investigación UC.

- **DOI:** [10.60525/04TEYE511/DYNMEV](https://doi.org/10.60525/04TEYE511/DYNMEV)
- **Licencia del conjunto de datos original:** [Creative Commons Attribution 4.0 International (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/)
- **Población objetivo:** personas de 18 años y más, chilenas o residentes en Chile por más de seis meses, que habitan viviendas particulares ocupadas en zonas urbanas a nivel nacional.
- **Modo de aplicación:** entrevistas personales cara a cara mediante dispositivo electrónico (CAPI).
- **Entrevistas completas:** 1.594.
- **Trabajo de campo:** 30 de julio al 16 de septiembre de 2026.
- **Margen de error de referencia informado por la UC:** ±2,5 puntos porcentuales, bajo el supuesto de muestreo aleatorio simple, máxima varianza y 95 % de confianza.
- **Diseño y selección muestral:** cartografía del Censo 2024. Para la calibración se utilizaron proyecciones INE 2026, base Censo 2017, para el universo de comunas del estudio, dado que las proyecciones poblacionales basadas en el Censo 2024 aún no estaban disponibles a nivel regional y comunal.

La descripción metodológica anterior corresponde a los metadatos del conjunto de datos original de la UC. Para detalles adicionales, consulte la ficha y los documentos del repositorio de investigación UC.

## Preparación y validación de esta distribución

1. Se importó el archivo tabular original `.tab` a SQLite junto con el libro de códigos.
2. Se reconstruyeron las tablas `respuestas`, `variables`, `categorias` y `meta`.
3. Durante la importación se normalizaron seis etiquetas regionales que contenían el carácter de reemplazo Unicode (` `): Biobío, La Araucanía, Los Ríos, Tarapacá, Valparaíso y Ñuble. La corrección afectó a 552 valores de `nom_region`.
4. Se verificó que la base reconstruida tuviera 1.594 registros, 251 variables, 14 regiones y suma de `POND` igual a 1.594. No se encontraron ponderadores nulos ni menores o iguales a cero.
5. Se exportaron las cuatro tablas a Parquet con compresión ZSTD.
6. Se comparó el contenido leído desde cada Parquet con su tabla SQLite de origen. Las cuatro tablas superaron la comparación exacta de contenido.
7. Se comprobó que los microdatos y las frecuencias servidas por la API devolvieran las 14 regiones sin caracteres de reemplazo Unicode.

La normalización descrita se limita a etiquetas de región. No se modificaron las respuestas sustantivas ni los valores del ponderador `POND`.

## Uso de los archivos

### Python y pandas

```python
import pandas as pd

respuestas = pd.read_parquet("data/respuestas.parquet")
variables = pd.read_parquet("data/variables.parquet")
categorias = pd.read_parquet("data/categorias.parquet")
meta = pd.read_parquet("data/meta.parquet")

print(respuestas.shape)  # (1594, 251)
print(respuestas["nom_region"].value_counts())
```

### DuckDB

```sql
SELECT
    nom_region,
    COUNT(*) AS n,
    SUM(POND) AS ponderado
FROM read_parquet('data/respuestas.parquet')
GROUP BY nom_region
ORDER BY nom_region;
```

También es posible consultar el archivo directamente desde Python:

```python
import duckdb

consulta = duckdb.sql("""
    SELECT nom_region, COUNT(*) AS n, SUM(POND) AS ponderado
    FROM read_parquet('data/respuestas.parquet')
    GROUP BY nom_region
    ORDER BY nom_region
""").df()
```

### Relación entre las tablas

- `respuestas`: contiene una fila por entrevista y una columna por variable.
- `variables`: `nombre` identifica la variable y permite recuperar su etiqueta, bloque, enunciado, tipo y clasificación.
- `categorias`: `variable` identifica la variable y `codigo` el valor al que corresponde la etiqueta. `es_perdido` señala códigos negativos clasificados como perdidos durante la importación.
- `meta`: pares `clave` / `valor` con información de trazabilidad de la importación.

Los porcentajes de análisis deben calcularse con el ponderador `POND` cuando corresponda. La variable `POND` es un factor de expansión; no debe confundirse el tamaño muestral sin ponderar con una suma de ponderadores. El tratamiento de valores perdidos debe ajustarse a la pregunta y a su libro de códigos.

## Cita

Para citar la fuente original de los datos:

> Dirección de Estudios Sociales, Pontificia Universidad Católica de Chile. (2026). *Encuesta Nacional Bicentenario UC 2026* (versión 2.0) [Conjunto de datos]. Datos de investigación UC. https://doi.org/10.60525/04TEYE511/DYNMEV

Al reutilizar los archivos de esta distribución, mantenga la atribución a la fuente original y cite también este repositorio cuando corresponda. La licencia indicada es la declarada para el conjunto de datos original: **CC BY 4.0**. Revise la ficha original para las condiciones aplicables y conserve la atribución y los avisos de licencia.

## Aviso metodológico

Los datos corresponden a una encuesta con diseño muestral y ponderación. Los análisis, estimaciones, errores estándar e intervalos de confianza elaborados por terceros deben considerar el diseño muestral y las limitaciones de la información disponible. El margen de error de referencia informado por la fuente no debe interpretarse como un intervalo aplicable automáticamente a cualquier estimación o subgrupo.

## Publicación del sitio

El archivo `index.html` es una portada estática para GitHub Pages. Para que los enlaces de descarga funcionen con la estructura mostrada, mantenga los cuatro archivos Parquet y `manifest.json` dentro de `data/`, en la raíz del repositorio.
