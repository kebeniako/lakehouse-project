# Partie 4 — Pipeline Spark du médaillon complet 

## Exigences du sujet (rappel exact)

Développer soi-même les jobs Spark nécessaires pour construire, à partir de
la zone brute alimentée par NiFi, les trois couches du médaillon en tables
Iceberg (catalogue Nessie) : Bronze, Silver, Gold. Aucune règle de
nettoyage, de jointure ou de calcul n'est imposée — la conception des
transformations et des indicateurs Gold est entièrement à la charge de
l'étudiant, à expliquer et justifier en vidéo, **en cohérence avec
l'historique de 6 mois constitué**.

## Statut

**Fonctionnellement terminé** — les 3 couches sont construites et
validées pour les 3 domaines, **sur l'historique complet de 28 dates**
(2 dates réelles + 26 semaines de backfill, contrainte 4.1 du sujet)

## Architecture générale

```
MinIO (raw/) → Spark lit le JSON brut (toutes dates confondues, via un
                glob raw/snapshots/*/​{domain}_*.json)
             → écrit BRONZE (copie brute + métadonnées techniques)
             → écrit SILVER (nettoyage, typage, aplatissement — par domaine)
             → écrit GOLD (jointures entre domaines + agrégations)
```

Chaque écriture passe par le catalogue **Nessie**. Un job = un fichier
Python, paramétré en ligne de commande (`--domain` pour Bronze/Silver,
`--table` pour Gold).

## Couche Bronze — `bronze_job.py`

Copie quasi brute du JSON déposé par NiFi. Le motif de lecture
(`raw/snapshots/*/{domain}_*.json`) capture automatiquement **toutes les
dates présentes** dans le bucket — aucune modification n'a été nécessaire
pour intégrer les 26 dates générées par le backfill (Partie 3).

| Table | Lignes (avant backfill) | Lignes (après backfill, 28 dates) |
|---|---|---|
| `nessie.bronze.products` | 20 | 560 |
| `nessie.bronze.users` | 10 | 280 |
| `nessie.bronze.carts` | 7 | 196 |

## Couche Silver — `silver_job.py`

Nettoyage, typage et aplatissement des structures imbriquées, appliqués
identiquement quel que soit le nombre de dates en entrée.

| Domaine | Transformation appliquée |
|---|---|
| `products` | `rating.rate`/`rating.count` → colonnes simples |
| `users` | `name.*` et `address.*` aplatis |
| `carts` | Tableau imbriqué `products[]` explosé (une ligne par produit du panier) |

## Couche Gold — `gold_job.py`

| Table | Question métier | Domaines joints |
|---|---|---|
| `gold.product_performance` | Quels produits sont les plus commandés, et sont-ils bien notés ? | carts × products |
| `gold.cart_summary` | Valeur d'un panier, provenance des clients qui dépensent le plus | carts × products × users |
| `gold.category_trends` | Évolution de la popularité des catégories dans le temps | products (agrégation par catégorie et par date) |

**`category_trends` dispose désormais d'une vraie profondeur temporelle** :
28 valeurs de `snapshot_date` distinctes, vérifiées via Dremio :
```sql
SELECT * FROM nessie.gold.category_trends ORDER BY snapshot_date;
-- → 112 lignes (28 dates × 4 catégories)
```

**Limite ** : les indicateurs (`num_products`,
`avg_price`, `avg_rating`) sont identiques sur toutes les dates, puisque
FakeStoreAPI renvoie systématiquement les mêmes données statiques — seule
la structure temporelle (les 28 `snapshot_date` distincts) est réelle. Avec
une source de données réellement évolutive, la même architecture
produirait des tendances effectives sans aucune modification du code.

## Fichiers produits à cette partie

| Fichier | Rôle |
|---|---|
| `spark-jobs/bronze/bronze_job.py` | Construction des 3 tables Bronze |
| `spark-jobs/silver/silver_job.py` | Construction des 3 tables Silver |
| `spark-jobs/gold/gold_job.py` | Construction des 3 tables Gold |

