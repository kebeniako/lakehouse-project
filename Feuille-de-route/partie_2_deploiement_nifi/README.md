# Partie 2 — Déploiement et configuration d'Apache NiFi 

## Exigences du sujet 

1. Intégrer NiFi au réseau Docker de la plateforme afin qu'il puisse résoudre le nom du service `minio`.
2. Exposer l'interface web de NiFi et documenter les identifiants de connexion (génération automatique ou configuration simplifiée, à justifier).
3. Prévoir un volume persistant pour l'état interne de NiFi (repository de flowfiles, provenance, etc.).

**Point d'attention technique du sujet** : NiFi récent impose HTTPS et une
authentification par défaut (utilisateur généré automatiquement, mot de
passe dans les logs). La procédure de récupération doit être montrée en vidéo.

## Ce qui a été fait

### 1. Intégration réseau

Le service `nifi` est déclaré sur le même réseau Docker `data-platform-network`
que les autres services, avec une dépendance explicite sur `minio` (attente
que MinIO soit `healthy` avant de démarrer NiFi). Ça permet à NiFi de
résoudre le nom `minio` (ex. `http://minio:9000`) sans passer par
`localhost`, condition indispensable pour la Partie 3 (écriture S3 depuis un
processeur NiFi).

### 2. Interface web et identifiants

- Version utilisée : `apache/nifi:2.8.0`
- Interface exposée en HTTPS sur `https://localhost:8443` (port HTTP interne
  décalé à 8880 pour éviter le conflit avec Spark Master UI)
- **Choix retenu : génération automatique des identifiants**, comme prévu par
  défaut par l'image officielle NiFi 2.x (authentification "Single User"),
  plutôt qu'une configuration HTTP simplifiée — ce choix reste proche du
  comportement par défaut de NiFi et évite d'affaiblir la sécurité pour un
  gain de commodité minime
- Identifiants récupérés via les logs du conteneur au premier démarrage :
  ```bash
  docker-compose logs nifi | grep -i username
  docker-compose logs nifi | grep -i password
  ```
- Connexion validée dans le navigateur (avertissement de certificat
  auto-signé accepté, normal en environnement local)

### 3. Volume persistant

Volumes Docker nommés dédiés à l'état interne de NiFi, montés sur les
répertoires internes correspondants :
- `nifi_state` — état du cluster/composants
- `nifi_db` — repository de base de données interne
- `nifi_flowfile` — repository des flowfiles
- `nifi_content` — repository de contenu
- `nifi_provenance` — repository de provenance (traçabilité des données)

Ces volumes garantissent que l'état de NiFi construit en survit à un redémarrage du conteneur.

## Difficultés rencontrées et solutions

| Difficulté | Cause | Solution |
|---|---|---|
| Aucune difficulté bloquante spécifique à cette partie | — | Le déploiement de NiFi s'est fait sans accroc une fois l'image correctement figée (`2.8.0`) et le port HTTP décalé (voir Partie 1) |

## Fichiers produits ou modifiés à cette partie

| Fichier | Rôle |
|---|---|
| `docker-compose.yml` (service `nifi`) | Intégration réseau, ports, volumes persistants |
| `.env` (variables `NIFI_*`) | Ports HTTP/HTTPS, clé de chiffrement des propriétés sensibles |