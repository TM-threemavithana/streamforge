#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SELECT 'CREATE DATABASE streamforge_alerts'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'streamforge_alerts')\gexec
EOSQL
