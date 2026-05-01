# Contributing

Thank you for considering a contribution to **Automated JSON Pipeline — Snowflake Streams & Tasks**!

## Getting Started

```bash
git clone https://github.com/atharvadevne123/Automated-json-pipeline-snowflake-streams-tasks
cd Automated-json-pipeline-snowflake-streams-tasks
pip install -e ".[dev]"
pre-commit install
```

## Development Workflow

1. Fork the repository and create a feature branch from `main`.
2. Make your changes and add tests.
3. Run `make lint` and `make test` — both must pass.
4. Push your branch and open a Pull Request.

## Code Standards

- **Formatter / linter**: [Ruff](https://github.com/astral-sh/ruff) — run `make lint-fix` to auto-fix.
- **Type annotations**: All public functions must be annotated.
- **Docstrings**: Google-style, on every public class and function.
- **Test coverage**: New code must be covered by tests in `tests/`.

## Running Tests

```bash
make test          # run pytest
make test-cov      # run with coverage report
```

## Commit Messages

Use [Conventional Commits](https://www.conventionalcommits.org/):

```
feat: add XYZ feature
fix: correct ABC bug
docs: update README
test: add tests for XYZ
refactor: rename helper
chore: update dependencies
```

## Reporting Issues

Please open an issue with:
- A clear description of the problem
- Steps to reproduce
- Expected vs actual behaviour
- Python version and OS

## Code of Conduct

Be respectful and constructive. We follow the [Contributor Covenant](https://www.contributor-covenant.org/).
