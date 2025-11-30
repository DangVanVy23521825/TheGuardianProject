import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()

user = os.getenv("POSTGRES_USER", "airflow")
password = os.getenv("POSTGRES_PASSWORD", "airflow")
db = os.getenv("POSTGRES_DB", "guardian_dw")
host = "localhost"
port = "5433"  # port Docker map ra host

engine = create_engine(
    f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
)

schema = 'analytics_analytics'

query_tables = f"""
SELECT table_name
FROM information_schema.tables
WHERE table_schema = '{schema}'
  AND table_type = 'BASE TABLE';
"""

tables = pd.read_sql(query_tables, engine)['table_name'].tolist()
print("Tables in schema:", tables)

output_dir = "csv_exports"
import os
os.makedirs(output_dir, exist_ok=True)

for table in tables:
    print(f"Exporting {table} ...")
    df = pd.read_sql(f'SELECT * FROM {schema}.{table}', engine)
    df.to_csv(f"{output_dir}/{table}.csv", index=False)

print("All tables exported to", output_dir)