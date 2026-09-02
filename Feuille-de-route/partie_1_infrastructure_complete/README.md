# Partie 1 — Conception et déploiement de l'infrastructure complète 

## Exigences du sujet (rappel exact)

1. Rédiger un `docker-compose.yml` regroupant l'ensemble des services de la pile imposée (section 1.1) : MinIO, PostgreSQL, cluster Spark, Nessie, Dremio, Airflow, Prometheus, Grafana, et Apache NiFi.
2. Configurer le réseau Docker, les volumes persistants nécessaires et les ports exposés, en évitant les conflits.
3. Faire démarrer l'ensemble des services et vérifier leur bon fonctionnement (interfaces web accessibles, connectivité inter-services).
4. Créer dans MinIO les buckets nécessaires (zone brute et warehouse Iceberg) et configurer Nessie comme catalogue pour Spark et Dremio.

## Statut : Fait

## Services déployés

| # | Service | Image | Rôle | Port exposé |
|---|---|---|---|---|
| 1 | PostgreSQL | `postgres:15` | Métadonnées Airflow + Nessie | 5432 |
| 2 | MinIO | `minio/minio` | Stockage objet S3-compatible | 9000 (API), 9001 (Console) |
| 3 | Nessie | `ghcr.io/projectnessie/nessie:0.77.1` | Catalogue versionné des tables Iceberg | 19120 |
| 4 | Spark Master | `apache/spark:3.5.6-scala2.12-java17-python3-ubuntu` | Pilotage du cluster Spark | 8080 (UI), 7077 (comm) |
| 5 | Spark Worker | `apache/spark:3.5.6-scala2.12-java17-python3-ubuntu` | Exécution des jobs Spark | 8081 (UI) |
| 6 | Dremio | `dremio/dremio-oss:26.0.5` | Moteur de requête SQL | 9047 (UI), 31010 (coord) |
| 7 | Airflow (init/webserver/scheduler) | `apache/airflow:2.8.0` | Orchestration du pipeline | 8082 |
| 8 | NiFi | `apache/nifi:2.8.0` | Ingestion depuis FakeStoreAPI | 8443 (HTTPS), 8880 (HTTP) |

## Choix de conception

- **Réseau unique** : `data-platform-network` (bridge) permet à tous les services de se résoudre entre eux par leur nom (ex. `minio:9000`, `nessie:19120`)
- **Volumes persistants** : données MinIO, base PostgreSQL, état interne NiFi (flowfile/content/provenance repository), données Spark et Dremio montées sur des volumes nommés
- **Ordonnancement du démarrage** : géré via `depends_on` associés à des `healthcheck` sur PostgreSQL et MinIO
- **Variables d'environnement centralisées** : fichier `.env` pour ne pas coder en dur identifiants et ports
- **Port NiFi HTTP décalé à 8880** pour éviter le conflit avec Spark Master UI (8080)
- **Une seule instance PostgreSQL, deux bases applicatives** (`airflow_db`, `nessie_db`) créées via un script d'initialisation monté au démarrage

## Buckets MinIO et catalogue Nessie (point 4)

- Buckets `raw` (zone brute) et `warehouse` (warehouse Iceberg) créés via un script Python idempotent (`scripts/init_minio.py`, `boto3`)
- Nessie configuré comme catalogue pour **Spark** (`config/spark-defaults.conf`) — validé par la création réelle d'une table Iceberg de test (écriture + lecture réussies via `spark-sql`)
- Nessie configuré comme catalogue pour **Dremio** (source "Nessie" ajoutée dans l'UI, connectée au même catalogue et au même stockage MinIO) — validé par la visibilité des namespaces créés par Spark

## Identifiants des services

| Service | Identifiant | Mot de passe |
|---|---|---|
| MinIO | `minioadmin` | `minioadmin123` |
| PostgreSQL | `postgres` | `postgres` |
| Airflow | `admin` | `admin` |
| NiFi | généré automatiquement au premier démarrage | généré automatiquement (voir commande ci-dessous) |

## Commandes utiles

```bash
# Démarrer l'infrastructure
docker-compose up -d
```

```bash
# Arrêter l'infrastructure
docker-compose down

# Voir les logs d'un service
docker-compose logs -f <service>

# Récupérer les identifiants NiFi (générés au 1er démarrage)
docker-compose logs nifi | grep -i username
docker-compose logs nifi | grep -i password
```

## Difficultés rencontrées et solutions

| Difficulté | Cause | Solution |
|---|---|---|
| Image `bitnami/spark` introuvable | Bitnami a fermé l'accès gratuit à ses images (offre payante) début 2025 | Bascule vers l'image officielle `apache/spark:3.5.6-scala2.12-java17-python3-ubuntu` |
| Image `projectnessie/nessie` introuvable sur Docker Hub | Dépôt Docker Hub abandonné par le projet | Bascule vers `ghcr.io/projectnessie/nessie:0.77.1` |
| `airflow-webserver` et `airflow-scheduler` plantaient au démarrage (`Exit 1`) | Race condition : `depends_on` attend seulement que le conteneur `airflow-init` démarre, pas qu'il termine sa migration DB | Ajout de `restart: on-failure` |
| Bug `KeyError: 'ContainerConfig'` sur `docker-compose up --force-recreate` | Bug connu de `docker-compose` 1.29.2 (ancienne version Python, dépréciée) | `docker-compose rm -sf <service>` puis `up -d <service>` à la place de `--force-recreate` |
| Erreur Ivy au démarrage de Spark (`/home/spark/.ivy2` introuvable en écriture) | L'image officielle Apache Spark ne garantit pas un répertoire personnel inscriptible | Cache Ivy redirigé vers `/tmp/.ivy2` (`spark.jars.ivy`) |
| Échec de lancement des exécuteurs Spark (`/opt/spark/work` non inscriptible) | Même cause que ci-dessus | Répertoire de travail du Worker redirigé vers `/tmp/spark-work` (`SPARK_WORKER_DIR`) |
| `SHOW NAMESPACES IN nessie` échouait (`SCHEMA_NOT_FOUND`) | Ligne de configuration malformée dans `spark-defaults.conf` | Correction de la ligne `spark.sql.catalog.nessie` |
| Dremio s'arrêtait spontanément (`Node lost its master status`) | Mémoire JVM par défaut trop élevée pour la RAM disponible → pauses GC → timeout Zookeeper interne | Heap JVM plafonné à 2048 Mo (`config/dremio-env`) + `mem_limit: 4g` |
| Connexion Dremio → Nessie refusée (`StsException`) | Le connecteur Nessie de Dremio valide les identifiants via un appel AWS STS, inexistant sur MinIO | Propriété de connexion `dremio.s3.compat = true` |

## Fichiers produits à cette partie

| Fichier | Contenu |
|---|---|
| `.env` | Identifiants et ports de chaque service |
| `docker-compose.yml` | Définition des 9 services, réseau et volumes |
| `scripts/init-multiple-db.sh` | Création automatique des bases `airflow_db` et `nessie_db` |
| `scripts/init_minio.py` | Création des buckets `raw` et `warehouse` |
| `config/spark-defaults.conf` | Configuration Spark pour l'accès à MinIO (S3A) et au catalogue Nessie |
| `config/dremio-env` | Limitation mémoire JVM Dremio |

