import os
import logging
import requests
logger = logging.getLogger(__name__)

def notify_success(**context):
    dag_id = context['dag'].dag_id
    execution_date = context['ds']
    message = f"✅ DAG `{dag_id}` completed successfully for {execution_date}."

    logger.info(message)

