#!/bin/bash

set -euo pipefail

create_database() {
    local database="$1"
    local username="$2"
    echo "Creating database '$database'..."

    psql \
        -v ON_ERROR_STOP=1 \
        --username "$POSTGRES_USER" \
        --dbname postgres \
        -v username="$username" \
        -v password="$password" \
        -v database="$database" \
        <<'EOSQL'

CREATE DATABASE :"database";
GRANT ALL PRIVILEGES ON DATABASE :"database" TO :"username";

EOSQL

    echo "User '$username' and database '$database' created successfully."
}
create_user(){
    local username="$1"
    local password="$2"

        psql \
        -v ON_ERROR_STOP=1 \
        --username "$POSTGRES_USER" \
        --dbname postgres \
        -v username="$username" \
        -v password="$password" \
        -v database="$database" \
        <<'EOSQL'
    
    CREATE USER :"username" WITH PASSWORD :'password';

EOSQL
}

echo "Starting PostgreSQL initialization..."
create_user \
    "$METADATA_DATABASE_USERNAME" \
    "$METADATA_DATABASE_PASSWORD"
create_database \
    "$METADATA_DATABASE_NAME" \
    "$METADATA_DATABASE_USERNAME" 

create_database \
    "$CELERY_BACKEND_NAME" \
    "$METADATA_DATABASE_USERNAME"

create_database \
    "$ELT_DATABASE_NAME" \
    "$METADATA_DATABASE_USERNAME"

echo "All databases and users created successfully."
