# AGENTS guidance

## Project overview

Mokelumne is the UC Berkeley Library's [Apache Airflow](https://airflow.apache.org)
deployment. It contains Dags, shared Python utilities, custom providers and plugins
local Docker Compose infrastructure, and the tests used to validate them.

Dependencies are declared in  `pyproject.toml`, constrained by 
`constraints.txt`, and pinned with hashes in `requirements.txt`. 
Airflow and its supporting services run through Docker Compose;
prefer testing in those containers so the Python and Airflow versions
match CI and our deployment environments.

## Build and development commands

Create a local `.env` containing generated development secrets before the
first startup:

```sh
docker compose run \
  --entrypoint /opt/airflow/scripts/setup_dev.py \
  --no-deps \
  --rm \
  airflow-init
```

Build and start the stack with:

```sh
docker compose build
docker compose up --detach
docker compose ps
```

Rebuild and recreate the relevant containers after changing application code
if an existing container imports the packaged image instead of the bind-mounted
working tree. Inspect service logs with `docker compose logs <service>`. Stop
the stack with `docker compose down`; do not add `-v` unless deleting the local
Postgres and other Compose volumes is explicitly intended.

When dependencies change, update `pyproject.toml` and regenerate the hashed
requirements file:

```sh
uv pip compile pyproject.toml --extra test -c constraints.txt \
  --generate-hashes -o requirements.txt
```

Do not hand-edit generated dependency pins. Keep `constraints.txt` aligned with
the packages supplied by the configured Airflow base image.

## Repository structure

* `mokelumne/` — runtime code
  * `dags/` - Airflow Dags (workflows)
  * `oidc/` — OIDC integration for CalNet SSO (deployed) and Keycloak (development)
  * `plugins/` — Airflow integrations
  * `util/` - Reusable business logic
* `test/` — test suite
  * `dags/` – actual Dag loading/test code
  * `e2e/` — end to end, browser-based UI tests written using Playwright
  * `fixtures/` - test fixtures used by the tests in other directories
  * `unit/` - fast, isolated unit tests with no external dependencies

## Mokelumne provider dependencies

This repository consumes two UC Berkeley Library-maintained Airflow providers as
released Python distributions. Their source does not live in this repository;
`mokelumne/providers/.keep` only reserves the shared namespace. Make provider
changes in the corresponding upstream repository, release the package, and
then update Mokelumne to consume that release.

- [`mokelumne-providers-tind`](https://github.com/BerkeleyLibrary/mokelumne-providers-tind)
  supplies the `mokelumne.providers.tind` Airflow provider, connection, and
  hook for the TIND DA digital collections platform used by the UC Berkeley Library.
- [`mokelumne-providers-ldc`](https://github.com/BerkeleyLibrary/mokelumne-providers-ldc)
  supplies the `mokelumne.providers.ldc` Airflow provider, connection, and
  hook.
- [`python-tind-client`](https://github.com/BerkeleyLibrary/python-tind-client)
  is the BerkeleyLibrary-maintained TIND API client used by
  `mokelumne-providers-tind`. Its import package is `tind_client`.

Treat this as a release chain. Changes to TIND API behavior generally begin in
`python-tind-client`, followed by a tagged PyPI release, a tested provider
release, and finally a Mokelumne dependency update. Provider-only TIND or LDC
changes begin in the corresponding provider repository. Each upstream project
has its own tests and tag-triggered PyPI release workflow, and both provider
repositories maintain hashed requirements. Do not copy upstream implementation
into this repository as a shortcut.

When adopting an upstream release, update the applicable requirement in
`pyproject.toml`, regenerate `requirements.txt` and any affected lock data, and
rebuild the image. Verify the resolved versions and hashes in
`requirements.txt`, run `pip check`, run the provider's upstream tests when its
source changed, and run Mokelumne's focused Dag tests plus its complete suite.
Because these distributions share the `mokelumne.providers` namespace, always
test from a freshly rebuilt image rather than relying on a stale installed copy.

## Code style and Dag conventions

- Write modern, typed Python compatible with the supported Python versions:
  - Prefer `pathlib.Path`, explicit return types, and module-level loggers over
    ad hoc path manipulation and `print`.
- Author Dags with the public Airflow Task SDK (`airflow.sdk`)
  and use Airflow 3 conventions.
  - Do not import implementation details from Airflow core.
  - Prefer TaskFlow `@task` functions or the Python operator
    for data processing; this deployment does not use
    Kubernetes, so do not introduce `KubernetesPodOperator`.
- Keep Dag parsing lightweight and deterministic.
  - Do filesystem, network, connection, and runtime-context work
    inside tasks.
  - Use environment variables for deployment-level top-level
    configuration and validated Dag `Param` values for per-run
    overrides.
  - Do not use top-level Airflow Variable lookups.
- Keep Airflow context access in Dag/task code and pass ordinary values into
  reusable helpers. Utility modules should remain independently unit-testable.
- Use `pymarc` for MARC parsing and writing
- Treat file-producing tasks as retryable transactions: do not expose
  partial final files, do not overwrite  existing outputs without an
  explicit requirement, and archive inputs only after output 
  publication succeeds.
- Use `run_dir()` for run-scoped intermediate artifacts where appropriate. If
  staged files are atomically renamed into a final directory, the staging and
  final paths must be on the same filesystem; pass an explicit `base_dir` when
  the default storage root cannot guarantee that.
- Write Sphinx/reStructuredText-style docstrings, as configured for
  `pydoclint`. Follow the surrounding module's established conventions and
  avoid unrelated refactors or formatting churn without approval.

Run the configured source checks for changed Python modules:

```sh
python -m pylint --persistent=n <changed-python-files>
pydoclint <changed-python-files>
python -m mypy --explicit-package-bases <changed-python-files>
```

## Testing instructions

Every behavior change needs a focused regression test. Put pure helper tests in
`test/unit`; use real `pymarc` records and temporary directories for MARC and
filesystem behavior. Put Dag loading, task topology, mapping, scheduling,
parameters, tags, and concurrency assertions in `test/dags`. Tests in these
directories are automatically marked `unit` and `dags`; browser tests in
`test/e2e` require the running Airflow and Playwright services.

Run the narrowest relevant tests while developing, followed by the full suite
before handoff:

```sh
docker compose exec airflow-cli python -m pytest test/unit/test_example.py
docker compose exec airflow-cli python -m pytest test/dags/test_example.py
docker compose exec airflow-cli python -m pytest -m unit
docker compose exec airflow-cli python -m pytest
```

Pytest writes its JUnit, Playwright, and coverage artifacts below `artifacts/`.
Also run `git diff --check`. Do not fix unrelated failures or warnings as part
of a scoped change; report them separately.

## Security considerations

- Never commit `.env`, credentials, API keys, tokens, production MARC
  data, or generated secret values.
- Keep placeholders in `example.env` visibly fake and
  use Airflow Connections or an approved secrets backend for credentials.
- Preserve hash-pinned dependencies and run `pip check` when changing them.
- Review new packages for necessity and compatibility with the Airflow image.
- Validate user-controlled paths and filenames. Reject traversal outside
  configured roots, avoid following unsafe symlinks, and do not log secrets or
  sensitive record contents.
- Shared-volume tasks may execute on different Celery workers. Required paths
  must be mounted consistently and use permissions suitable for the Airflow
  runtime UID.
- Cleanup jobs must exclude active runs and apply an explicit age
  or retention policy before deleting run-scoped data.
- Treat authentication, authorization, OIDC, static-file serving, and archive
  or overwrite behavior as security-sensitive. Add negative tests and avoid
  broadening access as a side effect of another change.
- Do not push branches, publish artifacts, modify pull requests, or contact
  external services unless the user explicitly requests it.

## Commits and PRs

Do not commit or amend unless requested. Stage only the intended files and
inspect the staged diff before committing. Use the associated ticket followed
by a concise imperative summary. Never push a commit unless the user separately authorizes the push. Never create a pull request unless the
user separately authorizes you to do so.

### Commit message formatting

```text
AP-783: Address GOBI Dag review feedback

Optional body explaining why the change is needed and any operational impact.

Co-authored-by: Codex GPT-5.6-sol <noreply@openai.com>
```

Keep the subject focused, put explanatory detail in the body, and place Git
trailers at the end after a blank line. For Codex-assisted commits, use the
requested `Co-authored-by` trailer with the actual Codex model name, version,
and size when it differs from the example. For other agents, use 
similar formatting; specify the agent's name, and the model name,
version, and size, as well as the pseudo-email address used by the
agent.

Mokelumne does not use Conventional Commits (`feat:`, `fix:`, `chore:`,
...). Do not use such prefixes in the commit message.
