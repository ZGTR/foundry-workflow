# Contributing

## Setup

Requires Python 3.12 or newer.

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -e ".[aws,jwt,mysql,dev]" ruff pytest
```

## Checks before a pull request

Run from the repository root:

```bash
ruff check src tests
PYTHONPATH=.:src:vendor python -m pytest tests
bash scripts/check_service_iac.sh      # needs terraform and npx on PATH
```

The suite uses real SQLite databases and a real HTTP process, not mocks. A
skipped test is not a pass. Do not add `-q` to the pytest command in CI: the
workflow parses pytest's summary lines.

When a change touches the container or the deployment, also build the image and
run the probes in `deploy/standalone/` (they need Docker); see
[`deploy/standalone/README.md`](deploy/standalone/README.md).

## Rules

- `config.py` is the only module that reads the environment; everything else
  receives a `ServiceConfig`. `tests/shape/test_layering.py` enforces it.
- A change to `vendor/shepherd_workflow` must update the hashes in
  `vendor/workflow-provenance.json` in the same commit;
  `tests/contract/test_workflow_provenance.py` refuses a mismatch.
- After changing `requirements/service.in`, regenerate `requirements/service.lock`
  with `pip-compile --generate-hashes`; never edit the lock by hand.
- Keep the code generic: no product names, account IDs, hostnames or
  per-product code paths. Examples use reserved names such as `example.com`
  and `example.invalid`.
- Do not commit credentials, `.env` files, local databases, probe receipts or
  logs containing personal data.

## License

By contributing you agree that your contribution is licensed under the MIT
License in `LICENSE`.
