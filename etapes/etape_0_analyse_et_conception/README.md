# ETAPE 0 : ANALYSE ET CONCEPTION

## Objectif de l'etape

Comprendre le sujet, analyser l'API FakeStoreAPI, prendre les decisions d'architecture, et creer la structure du projet dans VSCode.

## Ce qu'on a fait dans cette etape

### 1. Analyse du sujet
- Lecture complete du sujet d'examen
- Identification des technologies imposees
- Identification des contraintes obligatoires

### 2. Analyse de l'API FakeStoreAPI
- Test des endpoints : /products, /users, /carts
- Analyse de la structure des donnees JSON
- Identification des relations entre les domaines

### 3. Prises de decision d'architecture

| Question | Notre decision |
|---|---|
| Buckets MinIO | raw (donnees brutes) et warehouse (tables Iceberg) |
| Structure des fichiers | raw/snapshots/{date}/{domain}_{date}.json |
| Granularite | Hebdomadaire (26 snapshots sur 6 mois) |
| Historique | Snapshots reconstitues (car l'API n'a pas d'historique) |
| Bronze | Copie brute des JSON avec snapshot_date |
| Silver | Donnees nettoyees, typees, flatten |
| Gold | Agregations et jointures |
| Declenchement Airflow | Option A : Planifie (verification MinIO) |

### 4. Creation de la structure dans VSCode
- Dossiers organises par fonction (spark-jobs, dags, config, etc.)
- READMEs pour chaque etape (pour suivre la progression)

### 5. Creation du depot GitHub
- Depot prive cree sur GitHub
- Premier commit avec la structure de base
- Push vers le remote

## Pourquoi cette structure

| Dossier | Pourquoi |
|---|---|
| spark-jobs/ | Les 3 jobs Spark (Bronze, Silver, Gold) sont separes pour plus de clarte |
| dags/ | Les DAGs Airflow sont dans un dossier dedie (Airflow les detecte) |
| config/ | Toutes les configurations sont centralisees |
| etapes/ | Chaque etape a son propre README pour suivre la progression |

## Decisions importantes

### Granularite hebdomadaire
- Pourquoi ? L'API ne donne que l'etat courant. On veut 6 mois d'historique.
- Calcul : 6 mois x 4 semaines = 26 snapshots
- Volume : 26 snapshots x 3 domaines = 78 appels API

### Snapshots plutot que deltas
- Chaque snapshot = l'etat complet de l'API a une date donnee
- En Gold, on pourra faire des analyses temporelles
- Plus simple a implementer qu'un systeme de delta

## Analyse de l'API

### Products
{
  "id": 1,
  "title": "T-shirt",
  "price": 15.99,
  "description": "...",
  "category": "men's clothing",
  "image": "...",
  "rating": { "rate": 3.9, "count": 120 }
}

### Users
{
  "id": 1,
  "email": "john@mail.com",
  "username": "johnd",
  "name": { "firstname": "John", "lastname": "Doe" },
  "address": { "city": "Paris", "street": "...", "number": 3 },
  "phone": "123-456-7890"
}

### Carts
{
  "id": 1,
  "userId": 1,
  "date": "2025-01-15",
  "products": [
    { "productId": 1, "quantity": 3 }
  ]
}

### Relations
- Un cart contient des productId qui font reference a /products
- Un cart contient un userId qui fait reference a /users


## Validation de l'etape 0

-  Lecture complete du sujet
-  Test des 3 endpoints de l'API
-  Analyse de la structure des donnees
-  Decisions d'architecture prises
-  Structure de dossiers creee dans VSCode
-  README principal ecrit
-  README de l'etape 0 ecrit
-  Depot GitHub cree
-  Premier commit et push effectues

## Prochaine etape

Etape 1 : Infrastructure Docker
- Ecrire le fichier docker-compose.yml
- Deployer tous les services
- Verifier que tout communique
EOF