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
   - We recommend using a dev container for a consistent development environment. If using VS Code, reopen the project in a dev container via "Dev Containers: Reopen in Container" from the command palette. The dev container's Dockerfile installs all dependencies automatically, so you can skip the pip install step below.
   - If not using a dev container, ensure you have Python 3.10.12 or higher installed.
   - Install dependencies:
     ```bash
     pip install -e .[test]
     ```
     This installs the package in editable mode along with test dependencies.

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
   pytest  # Run tests
   black --check src/ tests/  # Check code formatting
   flake8 src/ tests/  # Lint code
   bandit -r src/  # Security check
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
- **Dependencies**: protobuf, requests, pandas (core); grpcio (for exd-data)
- **Test Dependencies**: pytest, black, flake8, bandit, pylint, pre-commit, etc.

To install all dev tools:
```bash
pip install -e .[test]
```

## Coding Standards

- **Formatting**: Use [Black](https://black.readthedocs.io/) with line length 120.
- **Linting**: Follow [Flake8](https://flake8.pycqa.org/) rules.
- **Security**: Run [Bandit](https://bandit.readthedocs.io/) for security checks.
- **Type Hints**: Use type hints where appropriate.
- **Docstrings**: Follow Google-style docstrings.

## Testing

- Write unit tests for new code.
- Aim for 100% code coverage.
- Run tests with `pytest`.
- Use `pytest-cov` for coverage reports.

## Pre-commit Hooks

We recommend using pre-commit hooks to automate checks:
```bash
pip install pre-commit
pre-commit install
```

This will run formatting and linting before each commit.

## Reporting Issues

If you find a bug or have a feature request, please open an issue on GitHub with:
- A clear title and description.
- Steps to reproduce (for bugs).
- Expected vs. actual behavior.

## Questions?

If you have questions, feel free to open a discussion on GitHub or contact the maintainers.
