CREATE DATABASE keycloak;

CREATE USER keycloak WITH PASSWORD 'keycloak';
GRANT ALL PRIVILEGES ON DATABASE keycloak TO keycloak;
GRANT USAGE ON SCHEMA public TO keycloak;
ALTER DATABASE keycloak OWNER TO keycloak;
