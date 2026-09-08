# Paris & Cotonou Weather Pipeline

Un mini-projet data engineering qui collecte chaque matin la météo de **Paris** et
**Cotonou**, valide les données puis les charge dans **PostgreSQL** avec **Apache
Airflow**.

## Architecture

```text
Open-Meteo API
      │
      ▼
Extract & transform (une tâche par ville)
      │
      ▼
Contrôles de validité → PostgreSQL → Contrôle qualité final
```

Le chargement est idempotent : rejouer le DAG met à jour une observation existante
au lieu de créer un doublon.

## Technologies

- Apache Airflow 2.10 (TaskFlow API, LocalExecutor)
- PostgreSQL 16
- Python 3.11, Requests et psycopg2
- Docker Compose
- Streamlit pour le tableau de bord
- Pytest et GitHub Actions

## Lancer le projet

Prérequis : Docker et Docker Compose.

```bash
cp .env.example .env
# Remplacez les valeurs `replace-with-...` dans .env.
# Une clé aléatoire peut être produite avec : openssl rand -hex 32
mkdir -p logs
docker compose up airflow-init
docker compose up -d
```

Ouvrir <http://localhost:8081>, se connecter avec les identifiants définis dans
`.env`, activer le DAG `paris_cotonou_weather`, puis cliquer sur **Trigger DAG**.

Le tableau de bord est disponible sur <http://localhost:8501>. Son bouton
**Nouvelle collecte** déclenche le DAG via l'API Airflow.

Pour arrêter les services :

```bash
docker compose down
```

Pour supprimer aussi les données PostgreSQL :

```bash
docker compose down --volumes
```

## Consulter les données

```bash
docker compose exec postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
  "SELECT city, observed_at, temperature_c, humidity_percent FROM weather.observations ORDER BY observed_at DESC;"'
```

## Tests

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
pytest -q
```

## Structure

```text
dags/weather_pipeline.py       # orchestration Airflow et chargement PostgreSQL
dags/weather_utils.py          # transformation et validation testables
dashboard/app.py               # interface météo Streamlit
tests/test_weather_utils.py    # tests unitaires
.github/workflows/tests.yml    # intégration continue
docker-compose.yml             # environnement reproductible
```

## Améliorations possibles

- ajouter une vue analytique comparant les deux villes ;
- visualiser les tendances dans Metabase ou Grafana ;
- ajouter des contrôles de qualité avec Great Expectations ;
- déployer Airflow sur un service cloud.

Les données proviennent de l'[API Open-Meteo](https://open-meteo.com/), accessible
sans clé pour un usage non commercial.

## Sécurité

- Aucun secret n'est versionné : les identifiants restent dans le fichier `.env`,
  ignoré par Git.
- Les images et dépendances sont épinglées à des versions précises.
- PostgreSQL n'expose aucun port vers l'extérieur.
- Cette configuration est destinée à une démonstration locale. Pour une mise en
  production, utilisez un gestionnaire de secrets, HTTPS et un proxy inverse.

Consultez [SECURITY.md](SECURITY.md) pour signaler une vulnérabilité.
