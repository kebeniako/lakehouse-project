# Étape 1 — Infrastructure Docker

## Objectif

Déployer, via un unique `docker-compose.yml`, l'ensemble des services nécessaires à la plateforme data lakehouse : stockage objet, catalogue de tables, cluster de traitement, moteur SQL, orchestrateur et outil d'ingestion. À l'issue de cette étape, l'infrastructure complète doit démarrer avec une seule commande (`docker-compose up -d`) et chaque interface web doit être accessible.

## Services déployés

| # | Service | Image | Rôle | Dépend de |
|---|---|---|---|---|
| 1 | PostgreSQL | `postgres:15` | Base de métadonnées pour Airflow et Nessie | — |
| 2 | MinIO | `minio/minio` | Stockage objet S3-compatible (zone brute + warehouse Iceberg) | — |
| 3 | Nessie | `projectnessie/nessie` | Catalogue versionné des tables Iceberg | PostgreSQL |
| 4 | Spark Master | `bitnami/spark` | Pilotage du cluster de traitement | — |
| 5 | Spark Worker | `bitnami/spark` | Exécution des jobs Spark | Spark Master |
| 6 | Dremio | `dremio/dremio-oss` | Moteur de requête SQL sur les tables Bronze/Silver/Gold | Nessie, MinIO |
| 7 | Airflow (init / webserver / scheduler) | `apache/airflow:2.8.0` | Orchestration du pipeline complet | PostgreSQL |
| 8 | NiFi | `apache/nifi` | Ingestion des données depuis FakeStoreAPI vers MinIO | MinIO |

*(Prometheus et Grafana sont ajoutés séparément, en bonus, à l'étape 8.)*

## Choix de conception

- **Réseau unique** : un réseau Docker dédié (`data-platform-network`) permet à tous les services de se résoudre entre eux par leur nom (ex. `minio:9000`, `nessie:19120`), sans dépendre de `localhost`.
- **Volumes persistants** : les données de MinIO, la base PostgreSQL, ainsi que l'état interne de NiFi (repository de flowfiles, provenance) sont montés sur des volumes nommés, pour survivre à un redémarrage des conteneurs.
- **Ordonnancement du démarrage** : géré via `depends_on` associés à des `healthcheck` (notamment sur PostgreSQL et MinIO), afin d'éviter les échecs de connexion liés à un démarrage trop précoce des services dépendants (Nessie, Dremio, Airflow).
- **Variables d'environnement centralisées** : identifiants, ports et URLs de service sont définis dans un fichier `.env` séparé, référencé par `docker-compose.yml`, pour ne pas coder en dur ces valeurs.

## Fichiers produits à cette étape

| Fichier | Contenu |
|---|---|
| `.env` | Identifiants et ports de chaque service |
| `docker-compose.yml` | Définition des 8 services, réseau et volumes |
| `config/spark-defaults.conf` | Configuration Spark pour l'accès à MinIO (S3A) et au catalogue Nessie |
| `scripts/init_minio.py` | Création automatique des buckets `raw` et `warehouse` |

## Vérifications effectuées

- [ ] `docker-compose ps` : tous les services au statut `Up`
- [ ] Console MinIO accessible : `http://localhost:9001`
- [ ] API Nessie accessible : `http://localhost:19120`
- [ ] Spark Master UI accessible : `http://localhost:8080`
- [ ] Spark Worker UI accessible : `http://localhost:8081`
- [ ] Dremio accessible : `http://localhost:9047`
- [ ] Airflow accessible : `http://localhost:8082`
- [ ] NiFi accessible : `https://localhost:8443`
- [ ] Aucune erreur bloquante dans les logs (`docker-compose logs`)

## Difficultés rencontrées et solutions

| Difficulté | Solution retenue |
|---|---|
| NiFi impose HTTPS et une authentification par défaut | Récupération de l'identifiant/mot de passe générés via `docker logs nifi` |
| Ordre de démarrage des services | Ajout de `depends_on` combinés à des `healthcheck` |
| Connexion Spark → MinIO | Configuration `s3a` avec `path.style.access` activé |
| Connexion Nessie → PostgreSQL | Vérification des variables d'environnement de connexion (host, base, identifiants) |

## Notes

*(espace réservé aux observations faites pendant le déploiement)*