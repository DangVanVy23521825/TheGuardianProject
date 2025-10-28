import subprocess
import os
import logging

logger = logging.getLogger(__name__)

def trigger_dbt_run(**context):
    project_dir = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt")
    profiles_dir = os.getenv("DBT_PROFILES_DIR", "/opt/airflow/dbt/.dbt")

    logger.info("🚀 Triggering dbt transformations (inside Airflow container)...")
    logger.info(f"📁 Project dir: {project_dir}")
    logger.info(f"⚙️ Profiles dir: {profiles_dir}")

    result = subprocess.run(
        [
            "dbt", "run",
            "--project-dir", project_dir,
            "--profiles-dir", profiles_dir,
            "--full-refresh"
        ],
        capture_output=True,
        text=True
    )

    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise Exception("❌ dbt run failed")

    logger.info("✅ dbt transformations completed successfully.")