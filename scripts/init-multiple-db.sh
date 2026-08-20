#!/bin/bash
# Crée les bases applicatives au premier démarrage du conteneur PostgreSQL.
# À monter dans /docker-entrypoint-initdb.d/ du service postgres
# (ce dossier est exécuté automatiquement par l'image officielle postgres
# UNIQUEMENT lors de la toute première initialisation du volume de données).
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE ${AIRFLOW_DB};
    CREATE DATABASE ${NESSIE_DB};
EOSQL