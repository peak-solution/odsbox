# GitHub Copilot Instructions — ODSBox

## Project Purpose

ODSBox is a lightweight Python wrapper for the [ASAM ODS HTTP API](https://www.asam.net/standards/detail/ods/wiki/).
It enables intuitive access to ASAM ODS measurement servers using:
- [JAQuel](https://peak-solution.github.io/odsbox/jaquel.html) queries (MongoDB-style JSON/dict syntax)
- [pandas](https://pandas.pydata.org/) DataFrames as the primary data representation
- [Protocol Buffers (protobuf)](https://protobuf.dev/) for efficient HTTP communication

## Toolchain

| Tool | Purpose | Command |
|------|---------|---------|
| [uv](https://docs.astral.sh/uv/) | Package manager & build backend | `uv sync --all-groups` |
| [ruff](https://docs.astral.sh/ruff/) | Linting + formatting (replaces black, flake8, isort, bandit) | `uv run ruff check src/ tests/` |
| [mypy](https://mypy.readthedocs.io/) | Static type checking (strict mode) | `uv run mypy src/` |
| [pytest](https://pytest.org/) | Testing | `uv run pytest tests/` |
| [python-semantic-release](https://python-semantic-release.readthedocs.io/) | Automated versioning & PyPI publishing | Runs on CI |
| [pip-audit](https://pypi.org/project/pip-audit/) | Dependency vulnerability scanning | Runs on CI |

**Do not use**: pip, flit, black, flake8, pylint, isort, bandit, tox. These are fully replaced.

## Code Style

- **Line length**: 120 characters
- **Ruff rules**: `E, W, F, I, UP, B` (errors, warnings, pyflakes, isort, pyupgrade, bugbear)
- **Python minimum**: 3.10 — every source module **must** start with `from __future__ import annotations`
- **Type annotations**: Strict mypy; annotate all function parameters and return types
- **Docstrings**: Google-style

### Type Annotation Conventions

```python
from __future__ import annotations  # Required in all modules

from typing import Any  # Use for proto message interactions

# Use built-in generics (Python 3.10+ with __future__):
def foo(items: list[str]) -> dict[str, Any]: ...

# For proto objects, Any is acceptable to avoid no-any-return errors
```

### Proto-generated Files

Files matching `*_pb2.py`, `*_pb2.pyi`, `*_pb2_grpc.py`, `*_pb2_grpc.pyi` are **auto-generated**.
- **Never modify or reformat them**
- They are excluded from ruff and mypy checks
- When interacting with proto objects, use `Any` type annotations as needed

## Project Structure

```
src/odsbox/        # Main package
  con_i.py         # ConI — main ODS server session class
  con_i_factory.py # ConIFactory — convenience factory for auth flows
  bulk_reader.py   # BulkReader — efficient quantity data access
  jaquel.py        # JAQuel query language converter
  datamatrices_to_pandas.py  # Proto DataMatrices → pandas DataFrame
  submatrix_to_pandas.py     # Submatrix → DataFrame (compatibility wrapper)
  model_cache.py   # ODS application model cache
  model_suggestions.py       # Typo suggestions for model names
  unit_utils.py    # SI unit / physical dimension queries
  unit_catalog.py  # Unit creation and lookup
  asam_time.py     # ASAM ODS datetime ↔ pandas Timestamp conversion
  transaction.py   # Transaction context manager
  security.py      # Security rights management
  proto/           # Auto-generated protobuf stubs (never edit)
tests/             # pytest tests (mypy strict errors suppressed)
docs/              # Sphinx documentation + Jupyter notebooks
```

## Key Patterns

### JAQuel Queries

JAQuel queries are Python dicts converted to ASAM ODS `SelectStatement` protobuf objects:

```python
# Query with JAQuel dict
df = con_i.query({
    "AoMeasurement": {"name": {"$like": "test*"}},
    "$attributes": {"name": 1, "id": 1},
    "$options": {"$rowlimit": 50},
})
```

### ConI Context Manager

Always use `ConI` as a context manager:

```python
from odsbox import ConI

with ConI(url="https://MY_SERVER/api", auth=("user", "pass")) as con_i:
    df = con_i.query({"AoMeasurement": {}})
```

### Authentication Variants

Use `ConIFactory` for different auth flows:

```python
from odsbox import ConIFactory

# Basic auth
con_i = ConIFactory.basic("https://server/api", "user", "pass")

# OAuth2 Machine-to-Machine
con_i = ConIFactory.m2m("https://server/api", token_endpoint="...", client_id="...", client_secret="...")

# OIDC browser login
con_i = ConIFactory.oidc("https://server/api", client_id="...")
```

## Testing

- Unit tests live in `tests/` and use pytest
- Integration tests are marked `@pytest.mark.integration` and excluded from default runs
- mypy strict errors are suppressed for `tests.*` via pyproject.toml override
- Run only unit tests: `uv run pytest tests/ -m "not integration"`
- Windows-specific: use `NamedTemporaryFile(delete=False)` with manual cleanup

## Git & Release Workflow

- **Branches**: `main` (stable), `dev` (integration), `feature/`, `bugfix/`, `release/`, `hotfix/`
- **PRs**: Always target `dev`; only releases merge into `main`
- **Commits**: Follow [Conventional Commits](https://www.conventionalcommits.org/):
  - `feat:` — new feature → bumps minor version
  - `fix:` — bug fix → bumps patch version
  - `feat!:` / `fix!:` — breaking change → bumps major version
  - `chore:`, `style:`, `docs:`, `refactor:`, `test:` — no version bump
- **Releases**: Automated via `python-semantic-release` on push to `main`

## CI/CD

The CI pipeline (`.github/workflows/CI.yml`) runs 4 jobs:

1. **lint** — `ruff check`, `ruff format --check`, `mypy src/` (Python 3.13)
2. **test** — `pytest tests/` matrix: Python 3.10 + 3.13 × pandas 2.x + 3.x
3. **audit** — `pip-audit` dependency vulnerability scan
4. **release** — semantic-release version bump + `uv build` + `uv publish` to PyPI (main branch only)
