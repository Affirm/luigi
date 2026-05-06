# Luigi Development Guide

## Overview
Luigi is a Python package for building complex pipelines of batch jobs. It handles dependency resolution, workflow management, visualization, and more.

## Development Setup

### Virtual Environment
```bash
source .venv/bin/activate
```

### Running Tests
```bash
# Run all tests and log results (default: Py312)
./run_tests.sh

# Run against Py39 (.venv39) or Py312 (.venv) explicitly
./run_tests.sh --py39
./run_tests.sh --py312

# Run specific test file
python -m pytest test/some_test.py -v

# Run specific test
python -m pytest test/some_test.py::TestClass::test_method -v

# Run with config (required for some tests)
LUIGI_CONFIG_PATH=test/testconfig/luigi.cfg python -m pytest test/some_test.py -v
```

### Running Race-Condition-Sensitive Tests in Isolation

Some tests use multiprocessing, real network ports, or timing-dependent scheduler state. They pass reliably in isolation but may flake when run alongside the full suite due to port conflicts or shared resources. Run them individually:

```bash
# Multiprocess worker tests (spawn new processes; sensitive to system load)
python -m pytest test/worker_multiprocess_test.py -v

# Dynamic dependency tests with multiple workers (timing-sensitive)
python -m pytest "test/worker_test.py::DynamicDependenciesWithMultipleWorkersTest" -v

# Scheduler tests that start a real server process
python -m pytest test/scheduler_test.py -v

# RPC / server tests (bind to real ports)
python -m pytest test/rpc_test.py test/server_test.py -v

# Remote scheduler tests
python -m pytest test/remote_scheduler_test.py -v
```

If a test fails in the full suite but passes in isolation, it is a pre-existing race condition — not a regression.

**Known macOS-only failures (pass on Linux CI):** `worker_multiprocess_test` and `rpc_test::RequestsFetcherTest::test_fork_changes_session` fail on macOS because Python 3.8+ changed the default multiprocessing start method from `fork` to `spawn`. Spawn requires all subprocess targets to be picklable at the top level, which these tests are not. Do not attempt to fix these locally.

## Project Structure
- `luigi/` - Main package source code
- `test/` - Test files
- `test/contrib/` - Tests for contrib modules (AWS, databases, etc.)
- `test/testconfig/` - Test configuration files

## Key Files
- `luigi/worker.py` - Task execution worker
- `luigi/scheduler.py` - Central scheduler
- `luigi/task.py` - Base Task class
- `luigi/parameter.py` - Parameter types
- `luigi/contrib/` - Integration modules (S3, ECS, Batch, etc.)

## Python 3.9 / 3.12 Dual Compatibility

This branch targets **both Python 3.9 and 3.12**. All changes use Python 3.3+ APIs.

### Py39 Test Environment

```bash
# Create a Py39 virtualenv (requires pyenv 3.9.18 installed)
PYENV_VERSION=3.9.18 python -m venv .venv39
source .venv39/bin/activate
pip install -e ".[toml]"
pip install psutil six sqlalchemy mock boto3 hypothesis pygments  # test deps from tox.ini

# Run the test suite
python -m pytest test/ --ignore=test/contrib/mysqldb_test.py --ignore=test/visualiser \
    --continue-on-collection-errors -x -q 2>&1 | tee /tmp/luigi-test-py39.log
```

### Py312 Compatibility Notes
- `random.seed()` no longer accepts tuples — use `hash()` to convert
- `random.randrange()` no longer accepts floats — use `int(1e10)` instead of `1e10`
- `pickle.dump()` requires binary mode (`"wb"`)
- `pickle.dump()` for scheduler state uses `protocol=3` for cross-version portability
- `collections.Mapping/MutableSet/Iterable` → `collections.abc.*` (removed from top-level in Py312)
- `inspect.getargspec()` → `inspect.getfullargspec()` (removed in Py312)
- `pkg_resources.resource_filename()` → `importlib.resources.files()` (pkg_resources deprecated)
- `nose` module uses removed `imp` module — use pytest marks instead
- `logging.config.fileConfig()` raises `FileNotFoundError` for missing files (was `KeyError`)
- External `six` package (e.g. `from six.moves.urllib...`) → native `urllib.*` (Python 3.0+)
- `six.PY3` checks can be removed entirely — always `True` on any supported Python 3.x

### Known Pre-existing Test Failures (not caused by Py312 changes)
- `test/contrib/mysqldb_test.py` — requires MySQL connector not installed in dev env
- `test/visualiser/` — requires Selenium not installed in dev env
- ~39 other failures confirmed pre-existing on both Py39 and Py312 baselines

## Running Luigi

### Quick Test with Local Scheduler (No Server)
For quick testing without starting a server, use `--local-scheduler` which runs an in-memory scheduler:
```bash
# Run the hello world example with in-memory scheduler (no web UI)
# Note: PYTHONPATH=. is needed to find the examples module from project root
PYTHONPATH=. luigi --module examples.hello_world examples.HelloWorldTask --local-scheduler
```

### Central Scheduler with Web UI (luigid)
The `luigid` daemon provides a central scheduler with web interface at http://localhost:8082

#### Run in Foreground
```bash
# Create log directory first
mkdir -p /tmp/luigi-logs

# Start the scheduler with web UI (http://localhost:8082)
luigid --logdir /tmp/luigi-logs

# Or with state persistence (survives restarts)
luigid --port 8082 --logdir /tmp/luigi-logs --state-path /tmp/luigi-state.pickle

# In another terminal, run a task against the central scheduler
PYTHONPATH=. luigi --module examples.hello_world examples.HelloWorldTask
```

#### Run in Background
```bash
mkdir -p /tmp/luigi-logs

# Start scheduler in background
luigid --background --logdir /tmp/luigi-logs --pidfile /tmp/luigi.pid

# Run a task
PYTHONPATH=. luigi --module examples.hello_world examples.HelloWorldTask

# Kill the scheduler
kill $(cat /tmp/luigi.pid)
# Or if pidfile not used
pkill -f luigid
```

## Building and Publishing

### Build the package
```bash
source .venv/bin/activate
python setup.py sdist bdist_wheel
twine check dist/*
```

### Publish to Artifactory
Credentials are in `.pypirc`. Upload using the `pypi-local` index:
```bash
twine upload --config-file .pypirc -r pypi-local dist/*
```

## Common Test Issues
- boto3 tests require AWS region configuration or proper mocking
- SQLAlchemy tests need eager loading for relationships to avoid DetachedInstanceError
- Process-related tests may need small delays for `/proc` filesystem to be ready

## S3 Module: boto3-Only Default + Boto1 Legacy Shim

`luigi/contrib/s3.py` defaults to boto3: `S3Client = S3ClientBoto3` (and `ReadableS3File = ReadableS3FileBoto3`). The legacy `S3ClientBoto1` class is still defined and importable for callers that need it explicitly, but boto1 is no longer a runtime dependency for the default path.

`S3PathTask`, `S3EmrTask`, and `S3FlagTask` do **not** accept a `client=` constructor argument — they always use the module default. Callers needing a non-default client (e.g. region-aware boto3) should subclass and override `output()`. (A `client=` parameter was briefly added in commit `05c71137` while the default was still boto1; it was reverted on May 6 2026 once the default flipped to boto3 made it redundant.)

### Running S3 Tests with uv

`test/contrib/s3_test.py` is structured for one ephemeral env per Python+moto+boto combo. Use `uv run --no-project --with-editable .` and pin the moto/boto versions you want to validate:

```bash
# Modern stack — py3.12 + moto5 + boto3 (recommended)
PYTHONPATH=test uv run --python 3.12 --no-project --with-editable . \
  --with pytest --with sqlalchemy --with mock --with hypothesis --with pygments \
  --with 'moto>=5,<6' --with boto3 \
  python -m pytest test/contrib/s3_test.py -q --override-ini addopts=''

# Legacy stack — py3.9 + moto1 + boto1 + boto3
arch -x86_64 env PYTHONPATH=test uv run --python 3.9 --no-project --with-editable . \
  --with pytest --with sqlalchemy --with mock --with hypothesis --with pygments \
  --with 'moto==1.3.16' --with boto3 --with boto \
  python -m pytest test/contrib/s3_test.py -q --override-ini addopts=''
```

### Compatibility matrix

| Python | moto | boto1 | boto3 | Result |
| --- | --- | --- | --- | --- |
| 3.12 | 1.x | yes | yes | **Broken**: moto1 calls `ssl.wrap_socket` (removed in 3.12); boto1's vendored `six` also fails to import. |
| 3.12 | ≥5  | —   | yes | **60 passed, 13 skipped** (boto1 tests skip cleanly). |
| 3.9  | 1.x | yes | yes | **33 passed, 40 skipped** — boto1 tests run; boto3 round-trip tests skip under `MOTO_LT_2` because moto<2 mishandles boto3 chunked Transfer-Encoding. |
| 3.9  | 1.x | —   | yes | Identical to row above — `moto==1.3.16` declares `boto` as a hard runtime dep, so boto1 is always installed transitively. |
| 3.9  | ≥5  | —   | yes | **60 passed, 13 skipped**. |

### Test gating flags

Defined at the top of `test/contrib/s3_test.py`:

- `MOTO_LT_2` — true if `moto.__version__` is `<2`. Skips the boto3 round-trip tests (multipart, copy, `test_get`, `test_get_as_string`, the whole `TestS3Target` class) because moto<2 corrupts uploads with raw chunk-size markers.
- `BOTO1_AVAILABLE` — true only if both `boto<3` AND moto's `mock_s3_deprecated`/`mock_sts_deprecated` are importable (the latter exists only in moto<2). Gates `TestS3TargetBoto1` and `TestS3ClientBoto1`.

### macOS / Apple Silicon notes

- Python 3.9 builds available locally are x86_64 only (pyenv 3.9.18, CommandLineTools 3.9.6, uv-managed 3.9.x). On arm64 hardware, prefix py3.9 invocations with `arch -x86_64` to load the matching x86_64 wheels via Rosetta. Without it, `cryptography`'s `_cffi_backend.so` fails to load with `incompatible architecture (have 'x86_64', need 'arm64')`.
- Python 3.12 runs natively in either arch; no prefix needed.
