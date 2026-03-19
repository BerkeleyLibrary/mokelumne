# mokelumne

UC Berkeley Library's Airflow installation, libraries, and Dags.

This is a proof of concept repo for Airflow under Docker Compose/Swarm. As is, it should be deployed with caution.

Consult [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html) for more information. The compose file in the initial commit reflects the composed file linked from those docs.

## Configuration

Airflow's configuration is propagated by environment variables [defined upstream by Airflow](https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html) and that are defined and handled in the base image. You can use a `.env` file or pass them.

Important environment variables for our build/environment:

| Variable | Purpose | Usage |
| -------- | ------- | ----- |
| `AIRFLOW_UID` | Set the `uid` the container runs as. | `AIRFLOW_UID="49003"` |
| `AIRFLOW_VERSION` | Sets the Airflow release version. Used to identify the base Airflow image and to define it as a Python constraint | `AIRFLOW_VERSION="3.1.7"` |
| `AIRFLOW_IMAGE_NAME` | Sets an alternate base image for Airflow, e.g. for `slim` images | `AIRFLOW_IMAGE_NAME="apache/airflow:slim-latest"` |
| `FERNET_KEY` | [Fernet](https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/fernet.html) encryption key used to encrypt Airflow secrets | `FERNET_KEY="somebase64value="` |
| `AIRFLOW__API_AUTH__JWT_SECRET` | Secret key used to sign JWT tokens for Airflow's API authentication. The default value used in development and testing should be replaced in production. | `AIRFLOW__API_AUTH__JWT_SECRET="some32bytesecret"` |
| `OIDC_CLIENT_SECRET` | Client secret for OIDC authentication.  Used by the Airflow webserver to authenticate OIDC token requests.  In development, also used by `keycloak-config-cli` to configure the client secret.  This should match Keycloak configuration in development and testing, and CalNet in production. | `OIDC_CLIENT_SECRET="some32charactersecret"` |
| `OIDC_NAME` | Name of the OIDC provider. "keycloak" or "CAS" | `OIDC_NAME="keycloak"` |
| `OIDC_CLIENT_ID` | Client ID specified in the OIDC provider. CAS will probably have a unique client_id | `OIDC_CLIENT_ID="airflow"` |
| `OIDC_WELL_KNOWN` | URL for the OIDC provider's well-known configuration. Used by the Airflow webserver to fetch the OIDC provider's public key for validating OIDC tokens in development and testing. Dev should be configured to point at keycloak's well known and prod points to CAS OIDC well known | `OIDC_WELL_KNOWN="http://keycloak:8180/realms/berkeley-local/.well-known/openid-configuration"` |
| `OIDC_ADMIN_GROUP` | Name of the OIDC group whose members should be mapped to the "Admin" role in Airflow. Used by keycloak-config-cli to configure group membership for the 'testadmin' user and by the Airflow webserver to map OIDC groups to Airflow roles in development and testing. For simplicity this should match the what we use for prod | `OIDC_ADMIN_GROUP="cn=edu:berkeley:app:auth-cas:lib-mokelumne:lib-mokelumne-admin,ou=campus groups,dc=berkeley,dc=edu"` |
| `OIDC_USER_GROUP` | Similar to admin group. This group is for users in both admin and user roles.| `OIDC_USER_GROUP="cn=edu:berkeley:app:auth-cas:lib-mokelumne:lib-mokelumne-allow,ou=campus groups,dc=berkeley,dc=edu"` |
| `TIND_API_KEY` | API key for TIND access | `TIND_API_KEY="..."` |
| `TIND_API_URL` | URL for TIND access | `ITND_API_URL="https://digicoll.lib.berkeley.edu/api/v1"` |
| `MOKELUMNE_TIND_DOWNLOAD_DIR` | Path for downloaded image cache | `MOKELUMNE_TIND_DOWNLOAD_DIR="/some/path/to/download/to"` |

Note: The `AIRFLOW_UID` example in `example.env` maps to the reserved `uid` for the `airflow` user in [lap/workflow](https://git.lib.berkeley.edu/lap/workflow/-/wikis/UIDs).

### Dev keycloak credentials
The `keycloak-config-cli` container will create a user/pass of `admin`/`admin` for the 'master' realm.
It will also create the following users with roles mapped from calnet groups in the `berkeley-local` realm:

| Username | Password | Role(s) |
| -------- | -------- | ---- |
| `testadmin` | `testadmin` | Admin/User |
| `testuser` | `testuser` | User |
