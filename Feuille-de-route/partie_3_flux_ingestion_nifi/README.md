# Partie 3 — Flux d'ingestion NiFi depuis FakeStoreAPI vers MinIO 

## Exigences du sujet (rappel exact)

Concevoir dans NiFi un dataflow qui interroge FakeStoreAPI et dépose les
données JSON obtenues dans la zone brute de MinIO. Aucun processeur n'est
imposé : le choix, l'agencement et la configuration relèvent entièrement de
la conception de l'étudiant, à expliquer et justifier en vidéo (choix des
processeurs, planification des appels, gestion des erreurs).

Seule contrainte technique imposée : l'endpoint S3 doit pointer vers le nom
du service Docker (`http://minio:9000`), pas `localhost`.

## Statut

**Fonctionnellement terminé** pour les 3 domaines (products, users, carts),
**y compris le mécanisme de backfill (contrainte 4.1)**

## Architecture du flow (identique pour chaque domaine)

```
InvokeHTTP (GET FakeStoreAPI)
      │ (relation "Response")
      ▼
UpdateAttribute (ajoute snapshot_date + construit la clé S3)
      │ (relation "success")
      ▼
PutS3Object (dépose le JSON brut dans le bucket raw)
```

Ce même schéma à 3 processeurs est dupliqué (copier-coller) une fois par
domaine, organisé en 3 rangées sur le canevas avec des étiquettes visuelles
(PRODUCT / USERS / CARTS) pour la lisibilité en vidéo.

## Choix de conception et justification

### Choix des processeurs
- **InvokeHTTP** : processeur standard pour interroger une API REST.
- **UpdateAttribute** : utilisé uniquement pour **étiqueter** le flowfile
  (ajout d'attributs `snapshot_date` et `s3.key`), sans toucher au contenu
  du fichier — respecte la contrainte "aucune transformation métier".
- **PutS3Object** : processeur standard pour écrire vers un stockage
  S3-compatible, avec support natif de l'endpoint personnalisé et du
  path-style access nécessaires pour MinIO.

### Contrôle minimal
Le contrôle des données reçues se limite à la vérification implicite du
code de statut HTTP via le routage des relations NiFi : `No Retry` (codes
400-499), `Retry` (codes 500-599) et `Original`/`Failure` sont routées et
terminées séparément de la relation `Response`.

### Structure de la clé S3
```
raw/snapshots/{yyyy-MM-dd}/{domain}_{yyyy-MM-dd}.json
```

### Configuration MinIO (PutS3Object)
- Bucket : `raw`, Object Key : `${s3.key}`
- Endpoint Override URL : `http://minio:9000`, Use Path Style Access : `true`
- Identifiants via un `AWSCredentialsProviderControllerService` dédié

## Backfill des 6 mois d'historique (contrainte 4.1)

**Le flow de production (FLOW QUOTIDIEN) reste inchangé** — il continue de
générer `snapshot_date` avec `${now():format('yyyy-MM-dd')}` à chaque
exécution horaire du DAG principal.

Pour reconstituer l'historique de 6 mois exigé par le sujet, un **second
flow dédié** a été construit :

1. **Duplication du flow** dans un **Process Group séparé** (`Backfill_Group`),
   pour ne jamais risquer d'impacter le flow de production
2. **Parameter Context** (`backfill_context`) créé et associé à ce groupe,
   avec un paramètre `backfill_date`
3. Dans les 3 processeurs `UpdateAttribute` de ce groupe, remplacement de
   `${now():format('yyyy-MM-dd')}` par `#{backfill_date}` dans les
   propriétés `s3.key` et `snapshot_date`

Résultat : NiFi peut désormais écrire des données sous **n'importe quelle
date fournie de l'extérieur**, plutôt que systématiquement la date du jour
— condition nécessaire pour que le DAG de backfill (Partie 4/6, voir
`dag_backfill.py`) puisse piloter 26 exécutions simulant 26 semaines
différentes, via l'API REST de NiFi (mise à jour du Parameter Context +
démarrage/arrêt du Process Group).

**Validé** : 26 dossiers de dates hebdomadaires (`2026-03-10` à
`2026-09-01`) sont apparus dans `raw/snapshots/`, chacun contenant les 3
domaines.

## Difficultés rencontrées et solutions

| Difficulté | Cause | Solution |
|---|---|---|
| `InvokeHTTP` refusait de démarrer | Relation `Failure` non connectée/terminée | Ajout de `terminate` sur `Failure` |
| `InvokeHTTP` exécutait des centaines d'appels en boucle | `Run Schedule` à `0 sec` par défaut | Réglage explicite à `3600 sec` |
| Connexion au vrai S3 (MinIO) plutôt qu'AWS | `PutS3Object` pointe par défaut vers AWS | `Endpoint Override URL` + `Use Path Style Access` + service de credentials dédié |
| Les identifiants AWS du service de credentials se sont vidés après manipulation du flow (copie/déplacement dans un Process Group) | Comportement observé lors de l'export/réimport ou de la copie de processeurs | Re-saisie manuelle des identifiants (`minioadmin`/`minioadmin123`) après chaque manipulation majeure du flow |
| Le "Reporting Task Prometheus" mentionné ailleurs dans le sujet n'existe pas dans NiFi 2.8.0 | Composant supprimé depuis NiFi 1.x | Voir Partie 7 (bonus non abouti) |

## Fichiers produits à cette partie

| Fichier | Rôle |
|---|---|
| `nifi-flow/nifi_flow.json` | Export du flow complet (flow quotidien + flow backfill) |
