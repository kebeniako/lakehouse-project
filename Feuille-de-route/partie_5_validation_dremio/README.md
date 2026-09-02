
# Partie 5 — Test et validation du moteur de requête Dremio 

## Exigences du sujet (rappel exact)

Dremio doit être configuré comme moteur de requête SQL du lakehouse,
connecté au catalogue Nessie (source Iceberg) et à MinIO :
1. Connecter Dremio au catalogue Nessie, vérifier que Bronze/Silver/Gold
   sont visibles pour chaque domaine
2. Exécuter une requête sur une table Bronze, une sur Silver, une sur Gold
3. Exécuter une requête de jointure entre deux domaines, directement
   depuis Dremio
4. Exécuter une requête d'agrégation sur une table Gold
5. (Valorisé, non obligatoire) Créer des vues (virtual datasets) au-dessus
   des tables Gold

## Statut

**Entièrement validé**, y compris le point valorisé (vue)

## 1. Connexion et visibilité des tables

La source **nessie** (configurée en Partie 1/2) expose bien les trois
espaces de noms `bronze`, `silver`, `gold` sous le catalogue Nessie dans
Dremio, avec les tables réelles créées par les jobs Spark de la Partie 4.

## 5. Vue (virtual dataset) — bonus valorisé

La requête de jointure (section 3) a été sauvegardée comme vue Dremio via
**"Save as View"**, sous le nom **`cart_details`**, dans l'espace personnel
`@kebeniako`. Réexécutée telle quelle depuis la vu
