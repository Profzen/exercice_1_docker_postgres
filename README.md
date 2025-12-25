# Exercice 1 — Pipeline CSV → PostgreSQL (Docker)

## Objectif
Ce guide décrit l'exécution et la vérification du pipeline d'ingestion CSV vers PostgreSQL, orchestré avec **Docker Compose**. Le pipeline expose une vue analytique `accidents_monthly_stats` pour analyser les tendances mensuelles.


---

## Lancement rapide (une seule commande)

```powershell
# Depuis le dossier exercice_1_docker_postgres
docker compose up
```

L'orchestration démarre en une commande. Vous verrez ensuite, dans le bon ordre :
1. PostgreSQL démarrer et être prêt (healthcheck).
2. Le service d’ingestion lire le CSV.
3. Les données nettoyées être insérées (sans doublons).
4. La vue analytique être créée/mise à jour.

---

## Prérequis

- **Docker Desktop** (v4.20+)
- **Docker Compose** (inclus dans Docker Desktop)
- Fichier `accidents_raw.csv` dans ce répertoire

Vérifiez l'installation :
```powershell
docker --version
docker compose version
```

---

## Guide complet d’exécution

### Étape 1 — Nettoyage préalable (état propre)

Commencez par un état propre :

```powershell
cd exercice_1_docker_postgres

# Arrêter les services et supprimer les volumes
docker compose down -v

# Vérifier que les volumes sont supprimés
docker volume ls | Select-String "pgdata"
# (Ne doit rien afficher)
```

### Étape 2 — Lancer le pipeline

Après le nettoyage, vous avez deux cas de figure :

**Cas standard (première exécution):** Un simple `docker compose up` suffit. Compose détectera qu'il faut construire l'image du service `ingestion` et le fera automatiquement.

**Cas où vous avez modifié le code:** Si vous avez changé `ingest.py`, `Dockerfile`, ou `requirements.txt`, forcez une reconstruction :

```powershell
# Lancement standard (première fois, ou après modification sans rebuild)
docker compose up

# Forcer la reconstruction si code/Dockerfile/requirements modifiés
docker compose up --build

# Reconstruction "propre" si vous soupçonnez un cache gênant
docker compose build --no-cache --progress plain
docker compose up
```

Logs attendus :
```
ingestion-1  | 2025-12-25 08:43:13,214 INFO Lines read: 10000
ingestion-1  | 2025-12-25 08:43:13,214 INFO Lines removed (invalid/missing date): 527
ingestion-1  | 2025-12-25 08:43:13,214 INFO Lines removed (duplicates or invalid submission_id): 261
ingestion-1  | 2025-12-25 08:43:13,444 INFO Partition created: accidents_2024 (year=2024)
ingestion-1  | 2025-12-25 08:43:13,619 INFO Partition created: accidents_2025 (year=2025)
ingestion-1  | 2025-12-25 08:43:15,045 INFO Rows inserted: 9212
ingestion-1  | 2025-12-25 08:43:15,117 INFO View accidents_monthly_stats created/updated
ingestion-1 exited with code 0
```

Le `code 0` signifie succès.

### Étape 3 — Vérifier les données (nouveau terminal)

```powershell
# 1. Voir les partitions créées
docker compose exec postgres psql -U app -d data_lab -c "SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename LIKE 'accidents_%' ORDER BY tablename;"

# 2. Vérifier la distribution des données par partition
docker compose exec postgres psql -U app -d data_lab -c "SELECT tableoid::regclass AS partition, COUNT(*) AS rows FROM accidents_clean GROUP BY tableoid ORDER BY partition;"

# 3. Consulter les statistiques mensuelles
docker compose exec postgres psql -U app -d data_lab -c "SELECT * FROM accidents_monthly_stats ORDER BY year, month LIMIT 12;"

# 4. Vérifier l'index unique
docker compose exec postgres psql -U app -d data_lab -c "SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'accidents_clean';"

# 5. Afficher la vue complète (optionnel)
docker compose exec postgres psql -U app -d data_lab -c "SELECT * FROM accidents_monthly_stats ORDER BY year, month;"
```

### Étape 4 — Arrêter les services

```powershell
# Dans le terminal où tourne docker compose up, vous pouvez appuyer sur Ctrl+C

# Ou dans un autre terminal :
docker compose down

# Pour arrêter ET supprimer les données :
docker compose down -v
```

---

## Consultation des logs

Plusieurs commandes permettent de suivre l'exécution du pipeline et diagnostiquer d'éventuelles erreurs.

### Logs en temps réel (mode interactif)

```powershell
cd exercice_1_docker_postgres
docker compose up
```

Vous verrez les logs des deux services (`postgres` et `ingestion`) directement dans votre terminal. Cette méthode est idéale pour une première exécution ou pour observer le comportement du pipeline en direct.

### Logs après exécution (mode détaché)

```powershell
# Lancer en arrière-plan
docker compose up -d

# Consulter les logs du service d'ingestion
docker compose logs ingestion

# Consulter les logs de PostgreSQL
docker compose logs postgres

# Consulter tous les logs
docker compose logs
```

### Suivre les logs en temps réel

```powershell
# Suivre les logs de l'ingestion (Ctrl+C pour quitter)
docker compose logs -f ingestion

# Suivre tous les logs
docker compose logs -f
```

### Logs récents (dernières N lignes)

```powershell
# Afficher les 50 dernières lignes
docker compose logs ingestion --tail 50

# Afficher les 100 dernières lignes et continuer à suivre
docker compose logs -f ingestion --tail 100
```

Ces commandes vous permettent de vérifier que le pipeline s'est bien exécuté, d'identifier les erreurs éventuelles et de suivre l'évolution du traitement en temps réel.

---

## Architecture technique

### Services Docker

| Service | Rôle | Image |
|---------|------|-------|
| `postgres` | Base de données analytique | `postgres:15` |
| `ingestion` | Pipeline Python | `python:3.11-slim` |

### Volumes

- `pgdata` : persistance PostgreSQL.
- `./accidents_raw.csv` : données brutes montées en lecture seule.

### Ports exposés

- `5432` (PostgreSQL) : accessible via localhost:5432.

### Variables d’environnement

```yaml
POSTGRES_USER: app
POSTGRES_PASSWORD: secure_password
POSTGRES_DB: data_lab
CSV_PATH: /data/accidents_raw.csv
DATABASE_URL: postgresql://app:secure_password@postgres:5432/data_lab
```

---

## Choix techniques et justification

- Orchestration Docker Compose : répond à l'exigence de démarrage unique (`docker compose up`) en isolant Postgres et le service d'ingestion ; Compose garantit la reproductibilité et la dépendance réseau entre services.
- Python + pandas pour l’ingestion CSV : volume modéré (~10 000 lignes) adapté à un traitement en mémoire simple ; parsing, nettoyage (dates/heures), enrichissement (colonnes dérivées) rapides sans surdimensionner l’outillage.
- PostgreSQL pour le stockage et l’analytique : correspond à l’énoncé (base analytique relationnelle), permet index et vue SQL ; partitionnement par année pour le bonus de persistance/optimisation et la gestion du volume par plage temporelle.
- Index unique `(submission_id, year)` : garantit la déduplication demandée ; combiné au nettoyage en amont, il sécurise l’intégrité des chargements successifs.
- Vue `accidents_monthly_stats` : couvre l’exposition analytique requise (year, month, total_accidents, taux_alcool_positif) en SQL natif, vérifiable en une commande `psql`.
- Alternatives non retenues : Spark/Flink jugés disproportionnés pour ~10 000 lignes ; une simple tâche cron locale aurait suffi mais ne répond pas à l’exigence Docker/Compose et à l’isolation des services.

---

## Détails du traitement des données

### 1. Lecture CSV
- Fichier : `accidents_raw.csv`.
- Colonnes principales : `submission_id`, `date_accident`, `heure_accident`, `region`, `type_accident`, `alcool`.

### 2. Nettoyage
- Suppression des lignes sans date d’accident (NaT).
- Déduplication par `submission_id` (après coercition en entier).
- Normalisation de la colonne alcool vers `Positif` / `Negatif` (sans accent).

### 3. Transformation
- Parsing des dates : formats principaux `YYYY-MM-DD`, `DD/MM/YYYY` (fallback permissif).
- Parsing des heures : format `HH:MM`.
- Colonnes dérivées : `year`, `month`, `hour`.

### 4. Stockage en PostgreSQL
- Table partitionnée `accidents_clean` (RANGE sur `year`).
- Partitions créées dynamiquement (ex. `accidents_2024`, `accidents_2025`).
- Index unique `(submission_id, year)` pour éviter les doublons.

### 5. Vue analytique
```sql
CREATE OR REPLACE VIEW accidents_monthly_stats AS
SELECT 
  year,
  month,
  COUNT(*) AS total_accidents,
  ROUND(AVG(CASE WHEN alcool = 'Positif' THEN 1 ELSE 0 END)::numeric, 4) AS taux_alcool_positif
FROM accidents_clean
GROUP BY year, month
ORDER BY year, month;
```

La vue est validée après insertion ; l'ordre des colonnes est conforme : `year`, `month`, `total_accidents`, `taux_alcool_positif`.

---

## Codes de sortie et résolution d’erreurs

### Exit Code 0 = Succès
Le pipeline s’est exécuté sans erreur.

### Exit Code 1 = Erreur

#### **Erreur : "duplicate key value violates unique constraint"**
```
ERROR: duplicate key value violates unique constraint "accidents_2025_submission_id_year_idx"
DETAIL: Key (submission_id, year)=(1, 2025) already exists.
```

Cause : données déjà présentes dans PostgreSQL (volume non nettoyé).

Solution :
```powershell
docker compose down -v
docker compose up
```

#### **Erreur : "cannot connect to postgres"**
```
psycopg2.OperationalError: could not translate host name "postgres" to address
```

Cause : PostgreSQL n’est pas prêt (timing).

Solution : le healthcheck attend automatiquement. Si cela persiste :
```powershell
docker compose down
docker compose up
```

#### **Erreur : "parent snapshot does not exist"**
```
failed to prepare extraction snapshot: parent snapshot does not exist
```

Cause : corruption du cache Docker.

**Solution :**
```powershell
docker compose down -v
docker builder prune -f
docker compose up --build
```

#### **Erreur : "CSV file not found"**
```
FileNotFoundError: /data/accidents_raw.csv
```

Cause : fichier `accidents_raw.csv` manquant dans le répertoire courant.

Solution :
```powershell
# Vérifier que le fichier existe
ls accidents_raw.csv

# Sinon, le copier depuis le parent
cp ../accidents_raw.csv .

docker compose up
```

#### **Erreur : "port 5432 already in use"**
```
Error response from daemon: Ports are not available: exposing port TCP 0.0.0.0:5432 -> 0.0.0.0:5432: listen tcp 0.0.0.0:5432: bind: An attempt was made to use a port that cannot be used.
```

Solution :
```powershell
# Arrêter tous les conteneurs
docker compose down

# Ou utiliser un port différent dans docker-compose.yml
# postgres:
#   ports:
#     - "5433:5432"
```

---

## Gestion des erreurs et robustesse

Le pipeline implémente une gestion explicite des erreurs dans le script d'ingestion (`ingest.py`) pour garantir qu'aucune défaillance ne passe inaperçue. Le pipeline **ne peut pas échouer silencieusement** : toute erreur est capturée, loggée avec sa trace complète et provoque un arrêt avec code de sortie non-zéro.

### Mécanismes de capture

**Erreurs spécifiques traitées :**
- `FileNotFoundError` : fichier CSV manquant → message explicite avec chemin attendu.
- `psycopg2.OperationalError` : PostgreSQL inaccessible (réseau, credentials, service non démarré) → message de diagnostic avec suggestions.
- `Exception` générique : toute autre erreur (parsing, insertion, etc.) → capture avec trace complète.

**Logging structuré :**
Chaque erreur est loggée avec :
- Un message d'erreur clair (ex. `FATAL ERROR: Cannot find CSV file at /data/accidents_raw.csv`).
- La trace complète de la pile d'appels (`exc_info=True`) pour faciliter le diagnostic.
- Le contexte d'exécution (timestamp, niveau de log).

**Codes de sortie garantis :**
- `Exit Code 0` : pipeline exécuté sans erreur.
- `Exit Code 1` : échec avec erreur capturée et loggée.
- Jamais de sortie silencieuse .

### Vérification post-exécution

Après le lancement, vous pouvez vérifier l'état du conteneur :

```powershell
# Vérifier le code de sortie
docker compose ps
# Si ingestion affiche "Exited (0)", succès
# Si ingestion affiche "Exited (1)", erreur

# Consulter les logs pour identifier l'erreur
docker compose logs ingestion | Select-String "FATAL ERROR"
```

Cette architecture garantit la conformité à l'exigence "qualité & journalisation : le pipeline ne doit pas échouer silencieusement".

---

## Diagnostics avancés

### Voir les logs complets
```powershell
# Logs du service d'ingestion
docker compose logs ingestion

# Logs de PostgreSQL
docker compose logs postgres

# Logs en direct (suivi live)
docker compose logs -f
```

### Accéder à PostgreSQL en ligne de commande
```powershell
docker compose exec postgres psql -U app -d data_lab
```

Puis en SQL :
```sql
-- Compter les lignes par partition
SELECT tableoid::regclass, COUNT(*) FROM accidents_clean GROUP BY tableoid;

-- Voir les 5 premières lignes
SELECT * FROM accidents_clean LIMIT 5;

-- Vérifier les dates extrêmes
SELECT MIN(date_accident), MAX(date_accident) FROM accidents_clean;

-- Quitter
\q
```

### Inspecter les conteneurs
```powershell
# Voir les conteneurs en cours
docker compose ps

# Voir tous les conteneurs (y compris arrêtés)
docker container ls -a

# Voir les volumes
docker volume ls
```

---

## Détails des livrables

### Fichiers fournis

| Fichier | Description |
|---------|-------------|
| `Dockerfile` | Image Python 3.11 avec requirements.txt |
| `docker-compose.yml` | Orchestration PostgreSQL + service d'ingestion |
| `ingest.py` | Script Python (ingestion + transformation) |
| `sql/create_view.sql` | Création de la vue analytique |
| `requirements.txt` | Dépendances Python (pandas, psycopg2, etc.) |
| `README.md` | Ce fichier |

### Conformité au travail demandé

Le pipeline implémente l'ensemble des points demandés par l'énoncé :
- Docker & orchestration : Dockerfile + docker-compose.yml ; démarrage via `docker compose up`.
- Ingestion & transformation : lecture CSV, suppression des lignes sans date, normalisation alcool (`Positif` / `Negatif`), conversion des dates/heures, ajout de `year`, `month`, `hour`.
- Stockage PostgreSQL : création de `accidents_clean` partitionnée par année ; insertion des données transformées ; déduplication via index unique `(submission_id, year)` et nettoyage en amont.
- Qualité & journalisation : logs du nombre de lignes lues, rejetées et insérées ; en cas d'erreur, le service ne reste pas silencieux (exceptions visibles dans les logs et exit code ≠ 0).
- Exposition analytique : vue `accidents_monthly_stats` conforme (year, month, total_accidents, taux_alcool_positif).

Obligatoires :
1. Docker & Orchestration - Dockerfile + docker-compose.yml
2. Ingestion & Transformation - CSV → Python → PostgreSQL
3. Stockage PostgreSQL - Table `accidents_clean`, déduplication
4. Qualité & Journalisation - Logs des compteurs
5. Exposition analytique - Vue `accidents_monthly_stats`

### Bonus implémentés
- Variables d’environnement.
- Volumes Docker (persistance `pgdata`).
- Healthcheck PostgreSQL.
- Partitionnement par année.

---

## Limites et améliorations

### Limites actuelles

**Volume de données :** Le pipeline actuel traite environ 10 000 lignes en mémoire avec Pandas. Cette approche est adaptée pour ce volume, mais ne passerait pas à l'échelle pour des fichiers CSV de plusieurs millions de lignes. Au-delà de 100 000 lignes, il faudrait envisager un traitement par batch (chunking Pandas) ou un moteur distribué (Apache Spark).

**Validation des données :** Le nettoyage actuel reste basique (suppression des dates manquantes, normalisation de la colonne alcool). Il n'y a pas de validation stricte sur les autres champs (région, type_accident, coordonnées GPS). Des valeurs aberrantes ou des incohérences métier (ex. heure > 23:59, région inexistante) peuvent passer inaperçues et polluer l'analyse.

**Partitionnement :** Le partitionnement par année est un bonus simple, mais pour des volumes importants sur plusieurs années, un partitionnement mensuel ou trimestriel serait plus efficace pour cibler les requêtes analytiques sur des plages de dates courtes.

**Gestion des doublons :** La déduplication repose sur un index unique `(submission_id, year)` après nettoyage Pandas. En cas de réingestion partielle (même fichier rechargé), les doublons sont rejetés par PostgreSQL. Il n'y a pas de stratégie d'UPSERT (mise à jour si déjà existant) qui permettrait d'actualiser des lignes corrigées.

**Observabilité :** Les logs sont basiques (stdout du conteneur). Il n'y a pas de métriques exportées (durée d'exécution, taux de rejet par type d'erreur) ni de dashboards de monitoring (Prometheus, Grafana) pour suivre l'évolution de la qualité des données dans le temps.

### Axes d'amélioration

**Validation stricte avec Pydantic :** Définir un schéma Pydantic pour chaque ligne CSV afin de valider les types, les plages de valeurs (heure entre 0 et 23, région dans une liste fermée) et lever des exceptions explicites en cas d'anomalie. Cela améliorerait la traçabilité des rejets.

**Partitionnement mensuel :** Remplacer le partitionnement annuel par un partitionnement mensuel (`RANGE(year, month)`) pour optimiser les requêtes analytiques ciblant un mois précis et réduire le scan de données inutiles.

**UPSERT intelligent :** Implémenter une logique `ON CONFLICT DO UPDATE` dans PostgreSQL pour actualiser les lignes existantes au lieu de les ignorer. Cela permettrait de corriger des erreurs métier sans recréer toute la base.

**Tests automatisés :** Ajouter des tests unitaires (pytest) pour valider le parsing des dates/heures, la normalisation de l'alcool, le calcul des colonnes dérivées. Cela sécuriserait les évolutions du code.

**Monitoring et alerting :** Exporter des métriques vers Prometheus (nombre de lignes traitées, durée d'exécution, taux de rejet) et créer des dashboards Grafana. Configurer des alertes si le taux de rejet dépasse un seuil (ex. > 10%).

**CI/CD et déploiement continu :** Intégrer GitHub Actions ou GitLab CI pour automatiser les tests, la construction de l'image Docker et le déploiement sur un environnement de staging avant la production. Cela garantirait la non-régression lors des évolutions du pipeline.

---

## Support

En cas de problème :
1. Consultez la section « Codes de sortie et résolution d'erreurs ».
2. Vérifiez les logs : `docker compose logs`.
3. Nettoyez et relancez : `docker compose down -v && docker compose up`.
