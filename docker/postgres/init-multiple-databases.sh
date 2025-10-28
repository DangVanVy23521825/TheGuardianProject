#!/bin/bash
set -e

create_db_if_not_exists() {
    local db=$1
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
        SELECT 'CREATE DATABASE $db'
        WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$db')
        \gexec
EOSQL
}

create_db_if_not_exists "airflow"
create_db_if_not_exists "guardian_dw"