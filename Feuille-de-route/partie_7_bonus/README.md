# Partie 7 — Bonus 

## Options proposées par le sujet

1. Supervision NiFi via Prometheus/Grafana (reporting task Prometheus)
2. Gestion de l'idempotence (DistributedMapCache ou vérification d'attribut)
3. Troisième domaine "commandes" (réel ou synthétique)
4. Process Group NiFi paramétrable (Parameter Context)
5. Fonctionnalité Iceberg avancée (time travel, schema evolution) interrogée depuis Dremio

## Statut global

3 des 5 options réalisées et démontrées avec succès.

---

## Bonus 1 — Troisième domaine "commandes" 

**Décision prise dès l'étape 0** : couvrir les 3 domaines FakeStoreAPI
(`products`, `users`, `carts`) plutôt que le minimum de 2 exigé. Le
domaine `carts` correspond exactement au "troisième domaine commandes"
suggéré par le sujet — il est intégré de bout en bout dans le pipeline
(ingestion NiFi, Bronze, Silver avec explosion du tableau imbriqué, Gold
via jointure avec `products` et `users`).

**Statut : acquis dès la conception initiale, sans effort supplémentaire dédié.**

---

## Bonus 2 — Gestion de l'idempotence 

**Objectif** : éviter que NiFi ré-écrive inutilement une donnée déjà
déposée dans MinIO (utile notamment pour éviter les doublons lors de
retraitements ou d'exécutions répétées).

**Implémentation** : ajout d'un processeur `DetectDuplicate` entre
`UpdateAttribute` et `PutS3Object`, associé à deux services de cache :
- `MapCacheServer` (le "serveur" qui mémorise les clés déjà vues)
- `MapCacheClientService` (le "client" utilisé par `DetectDuplicate`)

La clé vérifiée est `${s3.key}` (domaine + date), déjà calculée par
`UpdateAttribute`. Si cette clé a déjà été traitée, le flowfile est routé
vers la relation `duplicate` (terminée, rien n'est écrit) ; sinon il est
routé vers `non-duplicate`, qui alimente `PutS3Object`.

**Démonstration réalisée** : un appel API identique déclenché deux fois de
suite montre que `DetectDuplicate` reçoit bien 2 flowfiles mais n'en laisse
sortir qu'1 seul vers l'écriture — le second est intercepté avant
`PutS3Object`, qui n'a rien reçu (`In: 0`).

**Choix de conception** : `MapCacheServer`/`MapCacheClientService` plutôt
que la variante Redis (`SimpleRedisDistributedMapCacheClientService`), pour
ne pas dépendre d'un service externe supplémentaire — le cache tourne
directement à l'intérieur du conteneur NiFi.

**Statut : implémenté et démontré avec succès.**

---

## Bonus 3 — Fonctionnalité Iceberg avancée : Time Travel via Dremio 

**Objectif** : montrer qu'on peut interroger une version passée d'une
table Iceberg, pas seulement son état courant.

