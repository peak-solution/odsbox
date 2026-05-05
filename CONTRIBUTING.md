# Contributing to ODSBox

Thank you for your interest in contributing to ODSBox! We welcome contributions from the community. This document provides guidelines to help you get started.

## Code of Conduct

Please read and follow our [Code of Conduct](CODE_OF_CONDUCT.md) in all interactions.

## How to Contribute

1. **Fork the Repository**: Create a fork of the [ODSBox repository](https://github.com/peak-solution/odsbox) on GitHub.

2. **Clone Your Fork**:
   ```bash
   git clone https://github.com/your-username/odsbox.git
   cd odsbox
   ```

3. **Set Up the Development Environment**:
   - We recommend using a dev container for a consistent development environment. If using VS Code, reopen the project in a dev container via "Dev Containers: Reopen in Container" from the command palette. The dev container installs all dependencies automatically via `uv sync --all-groups`.
   - If not using a dev container, ensure you have Python 3.10.12 or higher and [uv](https://docs.astral.sh/uv/) installed.
   - Install all dependencies (including dev tools):
     ```bash
     uv sync --all-groups
     ```

4. **Create a Branch**:
   ```bash
   git checkout dev
   git checkout -b feature/your-feature-name
   ```

5. **Discuss Major Changes**: For significant features or changes, start a discussion on GitHub first to get feedback from maintainers.

6. **Make Changes**:
   - Follow the coding standards (see below).
   - Write tests for new features or bug fixes.
   - Ensure all tests pass.

7. **Run Tests and Checks**:
   ```bash
   uv run pytest tests/           # Run unit tests
   uv run ruff check src/ tests/  # Lint code
   uv run ruff format src/ tests/ # Format code
   uv run mypy src/               # Type check
   ```

8. **Commit Your Changes**:
   ```bash
   git add .
   git commit -m "Description of your changes"
   ```

9. **Push and Create a Pull Request**:
   ```bash
   git push origin feature/your-feature-name
   ```
   Then, open a pull request on GitHub **against the `dev` branch** with a clear description of your changes.

## Development Setup Details

- **Python Version**: >= 3.10.12
- **Package Manager**: [uv](https://docs.astral.sh/uv/)
- **Dependencies**: protobuf, requests, pandas (core); grpcio (for exd-data); pip-system-certs, requests-oauthlib (for oidc)
- **Dev Tools**: ruff, mypy, pytest, pytest-cov, python-semantic-release, pip-audit

To install all dev tools:
```bash
uv sync --all-groups
```

## Coding Standards

- **Formatting & Linting**: Use [Ruff](https://docs.astral.sh/ruff/) with line length 120.
- **Type Hints**: Use strict type hints throughout. All modules must include `from __future__ import annotations`.
- **Type Checking**: Run [mypy](https://mypy.readthedocs.io/) in strict mode (`uv run mypy src/`).
- **Docstrings**: Follow Google-style docstrings.

## Testing

- Write unit tests for new code.
- Aim for 100% code coverage.
- Run tests with `pytest`.
- Use `pytest-cov` for coverage reports.

## Pre-commit Hooks

We use [pre-commit](https://pre-commit.com/) hooks to automate checks before each commit:
```bash
uv run pre-commit install
```

Hooks run: ruff (lint + format), mypy, codespell, and various file checks.

## Building Documentation

Documentation is built with Sphinx and AutoAPI. To build locally:

```bash
uv run sphinx-build -b html docs docs/_build/html
```

Then open `docs/_build/html/index.html` in your browser.

## Reporting Issues

If you find a bug or have a feature request, please open an issue on GitHub with:
- A clear title and description.
- Steps to reproduce (for bugs).
- Expected vs. actual behavior.

## Questions?

If you have questions, feel free to open a discussion on GitHub or contact the maintainers.
