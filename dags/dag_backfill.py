"""
DAG de backfill — reconstitution de 6 mois d'historique (26 snapshots
hebdomadaires), conformément à la contrainte 4.1 du sujet.

FakeStoreAPI ne renvoie que l'état courant, sans dimension temporelle.
Ce DAG pilote donc NiFi via son API REST pour faire varier artificiellement
la date utilisée par le flow BACKFILL (Process Group dédié, paramétré via
un Parameter Context), plutôt que d'utiliser NiFi comme un simple flux
statique. NiFi reste l'unique point d'ingestion, conformément au sujet.

Pour chacune des 26 semaines sur les 6 derniers mois :
    1. Calcule la date simulée
    2. Met à jour le paramètre 'backfill_date' via l'API REST NiFi
    3. Démarre les processeurs du Process Group Backfill_Group
    4. Attend que le traitement soit terminé
    5. Arrête les processeurs

Une fois les 26 semaines injectées dans raw/, les jobs Spark habituels
(bronze/silver/gold) peuvent être relancés pour reconstruire les tables
sur l'historique complet.
"""

import time
from datetime import datetime, timedelta

import requests
import urllib3
from airflow import DAG
from airflow.operators.python import PythonOperator

# Désactive les avertissements liés au certificat auto-signé de NiFi
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

NIFI_BASE_URL = "https://45cd79c1855e:8443/nifi-api"
NIFI_USERNAME = "78af66cf-bc9b-4163-a99b-9a6a8915cca5"  
NIFI_PASSWORD = "Gi58XYQ5kjEB7B7yP1yp0UNtc0laclTP"       

PROCESS_GROUP_ID = "5dd863e8-01a0-1000-da7d-84e787859ce9"
PARAMETER_CONTEXT_ID = "5dd72baa-01a0-1000-7ea0-a554fe418f44"

NUMBER_OF_WEEKS = 26
SECONDS_TO_WAIT_PER_RUN = 30  # temps laissé au flow pour traiter les 3 domaines


def get_nifi_token() -> str:
    """Authentification NiFi (Single User) — renvoie un jeton JWT."""
    response = requests.post(
        f"{NIFI_BASE_URL}/access/token",
        data={"username": NIFI_USERNAME, "password": NIFI_PASSWORD},
        verify=False,
    )
    response.raise_for_status()
    return response.text


def update_backfill_date(token: str, simulated_date: str) -> None:
  
    headers = {"Authorization": f"Bearer {token}"}

    # 1. Lire l'état actuel du Parameter Context (pour la révision)
    get_resp = requests.get(
        f"{NIFI_BASE_URL}/parameter-contexts/{PARAMETER_CONTEXT_ID}",
        headers=headers,
        verify=False,
    )
    get_resp.raise_for_status()
    current = get_resp.json()
    revision = current["revision"]

    # 2. Envoyer la mise à jour du paramètre
    payload = {
        "revision": revision,
        "component": {
            "id": PARAMETER_CONTEXT_ID,
            "parameters": [
                {
                    "parameter": {
                        "name": "backfill_date",
                        "value": simulated_date,
                    }
                }
            ],
        },
    }
    update_resp = requests.post(
        f"{NIFI_BASE_URL}/parameter-contexts/{PARAMETER_CONTEXT_ID}/update-requests",
        headers=headers,
        json=payload,
        verify=False,
    )
    update_resp.raise_for_status()
    print(f"[Backfill] Paramètre backfill_date mis à jour : {simulated_date}")

    # Laisse à NiFi le temps d'appliquer la mise à jour au flow
    time.sleep(3)


def set_process_group_state(token: str, state: str) -> None:
    """state = 'RUNNING' ou 'STOPPED', appliqué à tous les composants du groupe."""
    headers = {"Authorization": f"Bearer {token}"}
    payload = {"id": PROCESS_GROUP_ID, "state": state}
    resp = requests.put(
        f"{NIFI_BASE_URL}/flow/process-groups/{PROCESS_GROUP_ID}",
        headers=headers,
        json=payload,
        verify=False,
    )
    resp.raise_for_status()
    print(f"[Backfill] Process Group mis à l'état : {state}")


def run_backfill(**context):
    token = get_nifi_token()

    today = datetime.now()

    for week_index in range(NUMBER_OF_WEEKS):
        simulated_date = (today - timedelta(weeks=week_index)).strftime("%Y-%m-%d")
        print(f"[Backfill] Semaine {week_index + 1}/{NUMBER_OF_WEEKS} — date simulée : {simulated_date}")

        update_backfill_date(token, simulated_date)
        set_process_group_state(token, "RUNNING")

        time.sleep(SECONDS_TO_WAIT_PER_RUN)

        set_process_group_state(token, "STOPPED")
        time.sleep(2)

    print(f"[Backfill] Terminé — {NUMBER_OF_WEEKS} semaines injectées dans raw/.")


default_args = {
    "owner": "lakehouse-project",
    "retries": 0,
}

with DAG(
    dag_id="lakehouse_backfill_6_mois",
    description="Reconstitution de 6 mois d'historique via pilotage du flow NiFi paramétré",
    default_args=default_args,
    schedule_interval=None,  # déclenchement manuel uniquement, pas planifié
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["lakehouse", "backfill"],
) as dag:

    backfill_task = PythonOperator(
        task_id="run_backfill_26_semaines",
        python_callable=run_backfill,
    )