

#  ÉTAPE 0 : ANALYSE ET CONCEPTION

## Ce qu'on a  fais

On a créé la structure de dossiers dans VSCode. Mais avant d'aller plus loin, on va comprendre **POURQUOI** cette structure et ce qu'elle va contenir.

---

##  Explication de la Structure

```
lakehouse-project/
├── README.md                          # Présentation du projet
├── docker-compose.yml                 # Définit tous les services Docker
├── .env                               # Variables d'environnement (identifiants, ports)
├── backfill/                          # Scripts pour générer les 6 mois d'historique
├── spark-jobs/                        # Jobs Spark (Bronze → Silver → Gold)
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── dags/                              # DAGs Airflow (orchestration)
├── nifi-flow/                         # Export du flux NiFi (.json)
├── scripts/                           # Scripts utilitaires (init MinIO, etc.)
├── config/                            # Fichiers de configuration (spark, nessie)
├── docs/                              # Documentation (architecture, API)
└── etapes/                            # READMEs de chaque étape
    ├── etape_0_analyse_et_conception/
    ├── etape_1_infrastructure_docker/
    ├── etape_2_configuration_initial/
    ├── etape_3_nifi_ingestion/
    ├── etape_4_backfill_6_mois/
    ├── etape_5_spark_bronze_silver_gold/
    ├── etape_6_airflow_orchestration/
    ├── etape_7_dremio_validation/
    └── etape_8_bonus_supervision/
```