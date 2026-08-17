---
name: upgrade-airflow-version
description: >-
  Upgrade Mokelumne's Apache Airflow dependency and Docker base image to a
  specified release, upgrade the Berkeley TIND and LDC provider distributions,
  refresh compatibility constraints and hash-pinned requirements, rebuild and
  recreate Compose services, and run package, DAG, and full-suite verification.
  Use when asked to bump, upgrade, or test Mokelumne against a new Airflow
  version.
---

# Upgrade Airflow Version

Upgrade Mokelumne to a requested Airflow release while keeping its compiled
dependencies compatible with the packages preinstalled in the upstream Airflow
image.

## Inspect the Current State

1. Read the repository's `AGENTS.md` and follow its current dependency,
   testing, security, and commit instructions.
2. Run `git status --short`. Preserve unrelated tracked and untracked work.
3. Locate the current versions and dependency inputs:

   ```sh
   grep -nE "AIRFLOW_VERSION|apache-airflow|task-sdk|constraints" \
     Dockerfile pyproject.toml constraints.txt README.md requirements.txt
   ```

4. Review the most recent Airflow-upgrade commit when useful. Use it to
   understand repository conventions, not as a substitute for inspecting the
   new image.
5. Record whether the Compose stack is running so its original state can be
   restored after verification.

## Derive Versions from the Target Image

Pull or run the requested upstream image and inspect its installed packages:

```sh
docker run --rm --entrypoint python apache/airflow:<version> -m pip freeze
```

Inspect the image metadata rather than assuming the task SDK has the same patch
version as Airflow:

```sh
docker run --rm --entrypoint python apache/airflow:<version> -c \
  "from importlib.metadata import version; \
print('apache-airflow', version('apache-airflow')); \
print('apache-airflow-core', version('apache-airflow-core')); \
print('apache-airflow-task-sdk', version('apache-airflow-task-sdk'))"
```

Use the image's default Python variant, which matches the Dockerfile build and
CI environment. Treat the image's `pip freeze` output as the authority for
compatibility constraints.

## Upgrade the Berkeley Providers First

Treat compatible new releases of both `mokelumne-providers-tind` and
`mokelumne-providers-ldc` as required parts of every Airflow upgrade. Their
source lives in separate upstream repositories, not in Mokelumne.

For each provider repository:

1. Update its Airflow or `apache-airflow-task-sdk` dependency for the target
   Airflow release.
2. Refresh its constraints and hash-pinned requirements according to that
   repository's instructions.
3. Run its focused and complete tests plus `pip check` against the target
   Airflow version.
4. Release a new provider version through its tag-triggered PyPI workflow.

Do not copy provider implementation into Mokelumne or reference an unreleased
provider version. Publishing releases or modifying upstream repositories
requires the user's separate authorization. If compatible releases do not
already exist and that authorization is absent, report the provider release
chain as a blocker before compiling Mokelumne's requirements.

## Update Dependency Inputs

1. Change `AIRFLOW_VERSION` in `Dockerfile`.
2. Change the exact `apache-airflow-task-sdk` dependency in `pyproject.toml` to
   the version installed in the target image.
3. Update the minimum versions of both `mokelumne-providers-tind` and
   `mokelumne-providers-ldc` in `pyproject.toml` to their newly released,
   Airflow-compatible versions.
4. Update the Airflow version example in `README.md` when it is stale.
5. Update the base-image version in the `constraints.txt` header.
6. Refresh the selective constraints in `constraints.txt` from the target
   image. Keep exact pins or narrow ranges for packages whose independent
   resolution can conflict with preinstalled image packages.

Keep the provider dependency update in the same Mokelumne change as the Airflow
update so the generated requirements cannot retain older provider releases by
accident.

## Regenerate Hash-Pinned Requirements

Never hand-edit generated pins. Run the repository command:

```sh
UV_CACHE_DIR=/tmp/mokelumne-uv-cache \
  uv pip compile pyproject.toml --extra test -c constraints.txt \
  --generate-hashes -o requirements.txt
```

Confirm that `requirements.txt` resolves `apache-airflow`,
`apache-airflow-core`, `apache-airflow-task-sdk`,
`mokelumne-providers-tind`, and `mokelumne-providers-ldc` to the intended
versions. Verify that the two provider pins changed to the new releases. Update
lock data only when it is tracked or otherwise part of the repository's current
dependency workflow; `uv.lock` is currently ignored.

## Resolve Base-Image Conflicts

Build the Compose images:

```sh
docker compose build
```

Keep the Dockerfile's `pip check` guard enabled. If it reports that generated
pins conflict with packages preinstalled in the Airflow image:

1. Compare each conflict with the target image's `pip freeze` output and
   installed-package requirements.
2. Add the widest appropriate constraint to `constraints.txt`. For example,
specify `cryptography >= 49, < 51` instead of `cryptography == 50.0.0` if it is known that major versions 49 and 50 of `cryptography` 
work with the specified Airflow version.
3. Regenerate `requirements.txt` with hashes as specified above.
4. Rebuild until `pip check` reports no broken requirements.

Common conflict families include `cryptography`/`pyOpenSSL`,
`grpcio`/`grpcio-status`, `importlib-metadata`/`litellm`, and aligned
OpenTelemetry packages. Derive versions from the current target image; do not
reuse old version numbers blindly.

## Recreate and Verify the Stack

If `.env` is missing, generate development secrets without displaying or
committing them:

```sh
docker compose run \
  --entrypoint /opt/airflow/scripts/setup_dev.py \
  --no-deps --rm airflow-init
```

Start or recreate the stack and wait for it to become healthy:

```sh
docker compose up --detach
docker compose ps
```

Verify the running container's versions and dependency consistency:

```sh
docker compose exec airflow-cli python -c \
  "from importlib.metadata import version; \
print('apache-airflow', version('apache-airflow')); \
print('apache-airflow-core', version('apache-airflow-core')); \
print('apache-airflow-task-sdk', version('apache-airflow-task-sdk')); \
print('mokelumne-providers-tind', version('mokelumne-providers-tind')); \
print('mokelumne-providers-ldc', version('mokelumne-providers-ldc'))"
docker compose exec airflow-cli python -m pip check
```

Run the complete test suite:

```sh

docker compose exec airflow-cli python -m pytest
```

Run `pylint`, `pydoclint`, and `mypy` as directed by `AGENTS.md` only when
Python modules changed. Do not fix unrelated failures or warnings; report them
separately.

## Review and Restore State

1. Run `git diff --check`.
2. Review `git status --short`, the complete source diff, and the
   changed-version inventory in `requirements.txt`.
3. Confirm that only intended dependency and documentation files changed.
   Ensure `.env`, credentials, test artifacts, and generated secrets are not
   tracked.
4. If the stack was stopped initially, restore that state with
   `docker compose down`. Never add `-v` unless the user explicitly requests
   deletion of persistent volumes.
5. Report updated Airflow, task SDK, TIND provider, and LDC provider versions;
   build and `pip check` results; focused and full test counts; warnings; and
   final Compose state.

Do not commit, push, publish, or create a pull request unless the user
separately requests each action.
