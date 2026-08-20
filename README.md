#  Projet Data Lakehouse - Examen

##  Objectif du projet

Construire une plateforme **Data Lakehouse** complète, de bout en bout, en utilisant les technologies Big Data modernes. La plateforme doit :

- **Ingérer** des données depuis une API publique (FakeStoreAPI) via Apache NiFi
- **Stocker** les données dans un lac de données (MinIO)
- **Transformer** les données en couches Bronze → Silver → Gold avec Apache Spark
- **Catalogue** les tables avec Project Nessie (format Iceberg)
- **Orchestrer** le pipeline avec Apache Airflow
- **Interroger** les données avec Dremio (SQL)

---

##  Ce que le professeur demande

| Exigence | Description |
|---|---|
| **Point d'ingestion unique** | Apache NiFi uniquement (pas de script externe) |
| **Source de données** | FakeStoreAPI (products, users, carts) |
| **Historique** | Reconstituer 6 mois de données (snapshots) |
| **Médaillon** | Bronze → Silver → Gold en Iceberg |
| **Catalogue** | Project Nessie |
| **Orchestration** | Airflow (DAG) |
| **Moteur SQL** | Dremio (obligatoire) |
| **Supervision** | Prometheus + Grafana (bonus) |


## Technologies imposées

| Technologie | Rôle |
|---|---|
| Apache NiFi | Ingestion des données depuis l'API |
| Apache Airflow | Orchestration du pipeline |
| Apache Spark | Traitement des données (médaillon) |
| Apache Iceberg | Format de table transactionnel |
| Project Nessie | Catalogue versionné des tables |
| MinIO | Stockage objet S3-compatible |
| Dremio OSS | Moteur de requête SQL |
| PostgreSQL | Métadonnées Airflow/Nessie |

---

##  Planning des étapes

| Étape | Sujet | Durée estimée |
|---|---|---|
| 0 | Analyse et Conception | 1 jour |
| 1 | Infrastructure Docker | 2-3 jours |
| 2 | Configuration Initiale | 1 jour |
| 3 | Flux NiFi - Ingestion | 2 jours |
| 4 | Backfill 6 mois | 1-2 jours |
| 5 | Spark - Bronze/Silver/Gold | 3-4 jours |
| 6 | Orchestration Airflow | 2 jours |
| 7 | Validation Dremio | 1-2 jours |
| 8 | Bonus - Supervision | 1-2 jours |
| 9 | Préparation de la vidéo | 2 jours |

---

## 📁 Structure du projet

```
lakehouse-project/
├── README.md                    # Ce fichier
├── docker-compose.yml           # Définit tous les services Docker
├── .env                         # Variables d'environnement
├── backfill/                    # Scripts pour le backfill
├── spark-jobs/                  # Jobs Spark (Bronze/Silver/Gold)
├── dags/                        # DAGs Airflow
├── nifi-flow/                   # Export du flux NiFi
├── scripts/                     # Scripts utilitaires
├── config/                      # Fichiers de configuration
├── docs/                        # Documentation
└── etapes/                      # READMEs de chaque étape
```

---

##  Auteur
[Votre nom]

##  Date
[Date]
EOF

---