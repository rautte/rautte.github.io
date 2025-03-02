import os
import requests
from urllib.parse import urlencode
from datetime import datetime, timedelta
import time
import logging

TOKEN = {'access_token': None}
EXPIRATION_DATE = datetime.min

def get_tabular_access_token(tabular_credential):
    TOKEN_URL = 'https://api.tabular.io/ws/v1/oauth/tokens'
    client_id, client_secret = tabular_credential.split(':')
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    body = urlencode({
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
    })

    try:
        response = requests.post(TOKEN_URL, headers=headers, data=body)
        response.raise_for_status()  # Ensure an exception is raised for HTTP errors
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Error obtaining access token: {e}")
        return None


def refresh_token_if_needed(tabular_credential):
    global TOKEN, EXPIRATION_DATE
    if not TOKEN['access_token'] or EXPIRATION_DATE < datetime.now():
        logging.info("Refreshing access token...")
        TOKEN = get_tabular_access_token(tabular_credential)
        if TOKEN:
            EXPIRATION_DATE = datetime.now() + timedelta(days=1)
            logging.info("Access token refreshed.")
        else:
            logging.error("Failed to refresh access token.")


def query_table_partitions(table_name: str, partition_path: str, tabular_credential: str,
                           warehouse: str = 'ce557692-2f28-41e8-8250-8608042d2acb') -> bool:
    refresh_token_if_needed(tabular_credential)
    
    database, table = table_name.split('.')
    access_token = TOKEN['access_token']
    url = f'https://api.tabular.io/ws/v1/ice/warehouses/{warehouse}/namespaces/{database}/tables/{table}'
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {access_token}',
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        partitions = response.json()
        
        full_partition_path = f'partitions.{partition_path}'
        for snapshot in partitions.get('metadata', {}).get('snapshots', []):
            if full_partition_path in snapshot.get('summary', {}):
                logging.info(f"Partition {partition_path} found in table {table_name}.")
                return True
        return False
    except requests.RequestException as e:
        logging.error(f"Error querying partitions for {table_name}: {e}")
        return False


def poke_tabular_partition(table, partition, tabular_credential, max_retries=10, wait_time=60):
    attempt = 0
    while attempt < max_retries:
        if query_table_partitions(table, partition, tabular_credential):
            logging.info(f"Partition '{partition}' found for table '{table}' after {attempt + 1} attempt(s).")
            return True
        
        attempt += 1
        logging.warning(f"Attempt {attempt} of {max_retries}: Partition '{partition}' not found for table '{table}'. Retrying in {wait_time} seconds...")
        time.sleep(wait_time)
    
    logging.error(f"Partition '{partition}' not found for table '{table}' after {max_retries} attempts.")
    return False


poke_tabular_partition('bootcamp.actor_films', "season = YEAR(DATE('{current_year}'))", tabular_credential = Variable.get("TABULAR_CREDENTIAL"))


