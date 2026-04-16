# mokelumne

UC Berkeley Library's Airflow installation, libraries, and Dags.

Consult [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html) for more information. The compose file in the initial commit reflects the composed file linked from those docs.

## Dependencies

Dependencies are declared in `pyproject.toml` and pinned in `requirements.txt` with hashes for supply-chain security. The Dockerfile installs from `requirements.txt` using plain `pip` — no additional tooling is needed at build time.

When adding or changing dependencies, regenerate the pins with [uv](https://docs.astral.sh/uv/):

```sh
uv pip compile pyproject.toml --extra test -c constraints.txt \
  --no-emit-package python-tind-client --generate-hashes \
  -o requirements.txt
```

`constraints.txt` contains upper bounds derived from the base Airflow Docker image to prevent version conflicts with pre-installed packages. Regenerate it when bumping `AIRFLOW_VERSION`:

```sh
docker run --rm --entrypoint python apache/airflow:<version> -m pip freeze
```

`python-tind-client` is excluded from `requirements.txt` because `pip` cannot verify hashes on git-sourced packages. It is installed separately in the Dockerfile.

## Development

Spin up the application using Docker Compose. There are a number of dependencies (Postgres, Keycloak, and Redis) as well as Airflow components (api/web, processor, scheduler, triggerer), so it's hardly lightweight. For now you'll need to sequence startup so that core services are setup before the ones that depend on them:

```sh
# Mint a short term API key for AWS Bedrock. You'll add it to
# AWS_BEARER_TOKEN_BEDROCK in the .env file created in the next step.
# (This will probably change in the future.)

# Generate `.env` with unique secrets specific to your local machine
docker compose run \
  --entrypoint /opt/airflow/scripts/setup_dev.py \
  --no-deps \
  --rm \
  airflow-init

# Start the stack
docker compose up --detach

# Open airflow in the browser (see below for test credentials)
open http://localhost:8080/
```

Airflow keeps ephemeral information in Redis but persists virtually all state (audit logs, user records, etc.) in Postgres. Thus if you want to start from a clean slate, you'll need to cleanup volumes when downing your stack:

```sh
docker compose down -v --remove-orphans
```

## Testing

With the stack running, execute the tests by exec-ing pytest in one of the airflow containers (cli works well). A full rundown of pytest flags is out-of-scope for this README, but here are some common use-cases to get you going:

```sh
# Run all the tests
docker compose exec airflow-cli python3 -m pytest

# Run tests with a specific marker
# Example: Only run the end-to-end (browser) tests
docker compose exec airflow-cli python3 -m pytest -m e2e

# Run a specific test file / folder
# Example: Only run examples in the ./tests sub-folder
docker compose exec airflow-cli python3 -m pytest test/tests

# Run a test by name
docker compose exec airflow-cli python3 -m pytest -k test_dags_load_with_no_errors
```

Test results / reports are written to the `./artifacts/pytest` directory.

## Configuration

Airflow's configuration is propagated by environment variables [defined upstream by Airflow](https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html) and that are defined and handled in the base image. You can use a `.env` file or pass them.

Important environment variables for our build/environment:

| Variable | Purpose | Usage |
| -------- | ------- | ----- |
| `AIRFLOW_UID` | Set the `uid` the container runs as. | `AIRFLOW_UID="40093"` |
| `AIRFLOW_VERSION` | Sets the Airflow release version. Used to identify the base Airflow image and to define it as a Python constraint | `AIRFLOW_VERSION="3.1.7"` |
| `AIRFLOW_IMAGE_NAME` | Sets an alternate base image for Airflow, e.g. for `slim` images | `AIRFLOW_IMAGE_NAME="apache/airflow:slim-latest"` |
| `AIRFLOW__CORE__FERNET_KEY` | [Fernet](https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/fernet.html) encryption key used to encrypt Airflow secrets | `AIRFLOW__CORE__FERNET_KEY="somebase64value="` |
| `AIRFLOW__API_AUTH__JWT_SECRET` | Secret key used to sign JWT tokens for Airflow's API authentication. The default value used in development and testing should be replaced in production. | `AIRFLOW__API_AUTH__JWT_SECRET="some32bytesecret"` |
| `OIDC_CLIENT_SECRET` | Client secret for OIDC authentication.  Used by the Airflow webserver to authenticate OIDC token requests.  In development, also used by `keycloak-config-cli` to configure the client secret.  This should match Keycloak configuration in development and testing, and CalNet in production. | `OIDC_CLIENT_SECRET="some32charactersecret"` |
| `OIDC_NAME` | Name appended to the OIDC login button | `OIDC_NAME="keycloak"` |
| `OIDC_CLIENT_ID` | Client ID specified in the OIDC provider. | `OIDC_CLIENT_ID="mokelumne"` |
| `OIDC_WELL_KNOWN` | URL for the OIDC provider's well-known configuration. Used by the Airflow webserver to fetch the OIDC provider's public key for validating OIDC tokens in development and testing. Dev should be configured to point at keycloak's well known and prod points to CAS OIDC well known | `OIDC_WELL_KNOWN="http://keycloak:8180/realms/berkeley-local/.well-known/openid-configuration"` |
| `OIDC_ADMIN_GROUP` | Name of the OIDC group whose members should be mapped to the "Admin" role in Airflow. Used by keycloak-config-cli to configure group membership for the 'testadmin' user and by the Airflow webserver to map OIDC groups to Airflow roles in development and testing. For simplicity this should match the what we use for prod | `OIDC_ADMIN_GROUP="cn=edu:berkeley:org:libr:mokelumne:admins,ou=campus groups,dc=berkeley,dc=edu"` |
| `OIDC_USER_GROUP` | Similar to admin group. This group is for users in both admin and user roles.| `OIDC_USER_GROUP="cn=edu:berkeley:org:libr:mokelumne:users,ou=campus groups,dc=berkeley,dc=edu"` |
| `TIND_API_KEY` | API key for TIND access | `TIND_API_KEY="..."` |
| `TIND_API_URL` | URL for TIND access | `TIND_API_URL="https://digicoll.lib.berkeley.edu/api/v1"` |
| `MOKELUMNE_TIND_DOWNLOAD_DIR` | Path for downloaded image cache | `MOKELUMNE_TIND_DOWNLOAD_DIR="/some/path/to/download/to"` |
|`LANGFUSE_HOST`|Host for Langfuse|`LANGFUSE_HOST="https://us.cloud.langfuse.com"`|
|`LANGFUSE_SECRET_KEY`|sets langfuse secret key|`LANGFUSE_SECRET_KEY="sk-lf-blah-blah-blah"`|
|`LANGFUSE_PUBLIC_KEY`|sets langfuse public key|`LANGFUSE_PUBLIC_KEY="pk-lf-blah-blah-blah"`|
|`AWS_ENDPOINT_URL`|AWS endpoint (don't forget the `https://`!)|`AWS_ENDPOINT_URL="https://bedrock-runtime.us-west-1.amazonaws.com"`|
|`AWS_DEFAULT_REGION`|The AWS region to use; you probably want `us-west-1`.|`AWS_DEFAULT_REGION=us-west-1`|
|`AWS_BEARER_TOKEN_BEDROCK`|The IAM credential to use to access AWS. Use a short-term API key.<br>The key will expire after AWS console logout or 12 hours (whichever comes first).<br>Make sure that your region for the key matches the region above.|`AWS_BEARER_TOKEN_BEDROCK="bedrock-api-key-blah-blah-blah"`|
|`AWS_MODEL_ID`|The model to use. Make sure it's supported on the ARN.|`AWS_MODEL_ID="us.anthropic.claude-haiku-4-5-20251001-v1:0"`|
|`AWS_MODEL_LABEL`|A human friendly label for the model. Will eventually be displayed in the Tind record.|`AWS_MODEL_LABEL="Claude Haiku 4.5"`|
|`AWS_MODEL_PROVIDER`|The provider for the model. |`AWS_MODEL_PROVIDER=anthropic`|
|`MOKELUMNE_PUBLIC_STORAGE`|Path for public assets|`MOKELUMNE_PUBLIC_STORAGE=/opt/airflow/public`|
|`MOKELUMNE_PUBLIC_URL`|URL to access public assets - must end in `/`|`MOKELUMNE_PUBLIC_URL=https://mokelumne-assets.ucblib.org/`|

Note: The `AIRFLOW_UID` example in `example.env` maps to the reserved `uid` for the `airflow` user in [lap/workflow](https://git.lib.berkeley.edu/lap/workflow/-/wikis/UIDs).

### Dev Keycloak credentials
The `keycloak-config-cli` container will create a user/pass of `admin`/`admin` for the `master` realm.
It will also create the following users with roles mapped from calnet groups in the `berkeley-local` realm:

| Username     | Password     | Role(s)    |
|--------------|--------------|------------|
| `testadmin`  | `testadmin`  | Admin/User |
| `testuser`   | `testuser`   | User       |
| `testpublic` | `testpublic` | Public     |
