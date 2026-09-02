

# Partie 6 — Orchestration Airflow et démonstration de bout en bout 

## Exigences du sujet (rappel exact)

Concevoir un DAG Airflow orchestrant l'ensemble du pipeline, avec un choix
justifié entre deux options de déclenchement :
- **Option A** — Déclenchement planifié : le DAG vérifie périodiquement la
  zone brute de MinIO et lance les jobs Spark dès que de nouvelles données
  y sont détectées.
- **Option B** — Déclenchement événementiel : NiFi appelle l'API REST
  d'Airflow à la fin de l'ingestion.

Le passage complet d'un appel API NiFi jusqu'à sa visibilité dans une
table Gold interrogeable via Dremio doit être démontré en vidéo.

## Statut

Terminé et validé de bout en bout

## Option retenue : A (déclenchement planifié)

Décidée dès l'étape 0 du projet. Justification : plus simple à mettre en
œuvre et à déboguer qu'un couplage NiFi→API Airflow (Option B), qui aurait
nécessité de configurer un appel HTTP sortant depuis NiFi vers l'API REST
Airflow avec authentification, sans bénéfice réel pour ce projet où le
volume de données et la fréquence d'ingestion ne justifient pas un
déclenchement événementiel temps réel.

## Mise en place technique préalable

Airflow tourne dans son propre conteneur, séparé de `spark-master`. Pour
qu'une tâche Airflow puisse déclencher un job Spark, deux choses ont été
mises en place :

1. **Image Airflow personnalisée** (`Dockerfile.airflow`, basée sur
   `apache/airflow:2.8.0`) : ajout du client Docker CLI (pour exécuter
   `docker exec spark-master ...` depuis l'intérieur du conteneur Airflow)
   et de `boto3` (pour interroger MinIO et vérifier la présence de
   nouvelles données).
2. **Socket Docker de l'hôte monté** dans `airflow-scheduler` et
   `airflow-webserver` (`/var/run/docker.sock`), avec l'utilisateur
   `airflow` ajouté au groupe `docker` de l'hôte (variable `DOCKER_GID`
   dans `.env`) pour avoir les permissions nécessaires.

Validé par un test manuel avant d'écrire le DAG :
```bash
docker exec -it airflow-scheduler docker exec spark-master \
  /opt/spark/bin/spark-submit /opt/spark/jobs/bronze/bronze_job.py --domain products
```
→ succès, confirmant qu'Airflow peut effectivement piloter Spark de cette
manière.

## Structure du DAG (`dag_daily_ingestion.py`)

```
check_raw_zone
      │
      ├──► bronze_products ──► silver_products ──┐
      ├──► bronze_users    ──► silver_users    ──┼──► gold_product_performance
      └──► bronze_carts    ──► silver_carts    ──┼──► gold_cart_summary
                                                   └──► gold_category_trends
```

- **`check_raw_zone`** (PythonOperator) : interroge MinIO via `boto3` et
  vérifie qu'au moins un fichier existe pour chacun des 3 domaines dans
  `raw/snapshots/`. Si un domaine manque, la tâche est marquée `skipped`
  (`AirflowSkipException`), ce qui bloque automatiquement toute la suite
  du DAG pour cette exécution — cohérent avec l'esprit de l'Option A
  ("dès que de nouvelles données sont détectées").
- **Bronze/Silver** : une tâche par domaine (BashOperator), exécutables en
  parallèle entre elles, mais chaque Silver dépend de la Bronze du même
  domaine.
- **Gold** : chaque table dépend des 3 tables Silver, puisque les
  agrégations Gold combinent plusieurs domaines.
- **Planification** : `schedule_interval="@hourly"`.

## Difficultés rencontrées et solutions

| Difficulté | Cause | Solution |
|---|---|---|
| Le DAG n'apparaissait pas dans l'interface Airflow malgré un fichier valide dans `dags/` | Le bind mount du dossier `dags/` ne se synchronisait pas en temps réel avec le conteneur (instantané figé au moment de sa création) | Recréation des conteneurs `airflow-scheduler`/`airflow-webserver` (`docker-compose rm -sf` puis `up -d`) pour qu'ils reprennent un montage à jour |
| Deux exécutions du DAG lancées simultanément (planifiée + manuelle) ont provoqué des échecs partiels sur un run | Déclenchement manuel juste avant l'heure du déclenchement planifié | Non bloquant : le run suivant, sans conflit, s'est terminé en succès complet |
| Perte d'accès à l'interface NiFi (`"The supplied username and password are not valid"`) après plusieurs redémarrages du conteneur | Les identifiants générés au tout premier démarrage n'étaient plus dans l'historique des logs Docker (rotation/troncature), alors que le volume d'état avait entre-temps changé | Réinitialisation complète des volumes NiFi (`nifi_state`, `nifi_db`, `nifi_flowfile`, `nifi_content`, `nifi_provenance`) pour forcer la régénération d'un nouveau couple identifiant/mot de passe ; le flow NiFi (déjà exporté en `.json` en Partie 3) a dû être réimporté |

## Démonstration de bout en bout réalisée

Un run complet du DAG (déclenché manuellement puis confirmé par le
déclenchement planifié suivant) a abouti aux 10 tâches en `success`.
Vérification finale : requête `SELECT * FROM nessie.gold.category_trends;`
exécutée dans Dremio après l'exécution automatique du DAG, renvoyant un
résultat cohérent — preuve que la chaîne complète (NiFi → MinIO → Airflow
→ Spark → Nessie → Dremio) fonctionne sans intervention manuelle.

## Fichiers produits à cette partie

| Fichier | Rôle |
|---|---|
| `Dockerfile.airflow` | Image Airflow personnalisée (Docker CLI + boto3) |
| `dags/dag_daily_ingestion.py` | DAG principal d'orchestration du médaillon |
| `docker-compose.yml` (modifié) | Build de l'image personnalisée, montage du socket Docker |
| `.env` (variable `DOCKER_GID` ajoutée) | GID du groupe Docker de l'hôte, pour les permissions |

