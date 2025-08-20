# Run this script only once. Running it again will create duplicate entries.
# python -m app.scripts.migrate_logs

import os
import uuid
import json
import psycopg2
from datetime import datetime, timezone
from dotenv import load_dotenv
import ollama
import numpy as np
import warnings

from logs.services.weaviate_client import (
    create_schema,
    weaviate_store,
    weaviate_search,
    delete_all_weaviate_data
)

warnings.simplefilter("ignore", ResourceWarning)
load_dotenv()

BATCH_SIZE = 500
SIMILARITY_THRESHOLD = 0.85

# PostgreSQL connection
PG_CONN = psycopg2.connect(
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
)


def get_embedding(text: str):
    """Generate vector embedding using Ollama."""
    try:
        response = ollama.embed(model="nomic-embed-text", input=text)
        vector = response.get("embeddings", [])
        if not vector:
            return []
        return np.array(vector).flatten().astype(np.float32).tolist()
    except Exception as e:
        print(f"Error generating embedding: {e}")
        return []


def insert_duplicate(cur, top_match, row):
    """Insert a duplicate log into duplicate_logs table."""
    (
        log_id, date, time, appName, serviceName, job, label,
        level, message, kubernetesDetails
    ) = row

    k8s_details_json = json.dumps(kubernetesDetails) if kubernetesDetails else None
    incident_id = top_match["incident_id"]

    cur.execute("""
        INSERT INTO duplicate_logs (incident_id, date, time, appName, serviceName, job, label, level, message, kubernetesDetails)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        incident_id, date, time, appName, serviceName, job,
        label, level, message, k8s_details_json
    ))


def migrate_logs():
    create_schema()
    delete_all_weaviate_data()

    with PG_CONN.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM all_logs")
        total_rows = cur.fetchone()[0]
        print(f"Total rows in all_logs: {total_rows}")

        offset = 0
        processed = 0
        inserted_cleaned = 0
        inserted_duplicates = 0

        while offset < total_rows:
            cur.execute("""
                SELECT id, date, time, appName, serviceName, job, label, level, message, kubernetesDetails
                FROM all_logs
                ORDER BY date, time
                LIMIT %s OFFSET %s
            """, (BATCH_SIZE, offset))
            rows = cur.fetchall()

            for row in rows:
                # Fix: include 'id' in unpacking
                (
                    log_id, date, time, appName, serviceName, job, label,
                    level, message, kubernetesDetails
                ) = row

                alert_text = " | ".join([
                    appName or "",
                    serviceName or "",
                    job or "",
                    label or "",
                    level or "",
                    message or "",
                    str(kubernetesDetails or "")
                ])

                vector = get_embedding(alert_text)
                if not vector:
                    continue

                matches = weaviate_search(vector, limit=1)
                timestamp = datetime.combine(date, time).replace(tzinfo=timezone.utc)
                k8s_details_json = json.dumps(kubernetesDetails) if kubernetesDetails else None

                if matches and matches[0].get("similarity", 0) >= SIMILARITY_THRESHOLD:
                    insert_duplicate(cur, matches[0], row)
                    inserted_duplicates += 1
                    continue

                # Insert unique logs
                incident_id = str(uuid.uuid4())[:8]
                cur.execute("""
                    INSERT INTO cleaned_logs (id, date, time, appName, serviceName, job, label, level, message, kubernetesDetails)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    incident_id, date, time, appName, serviceName, job,
                    label, level, message, k8s_details_json
                ))
                weaviate_store(vector, incident_id, alert_text, timestamp)
                inserted_cleaned += 1

            PG_CONN.commit()
            offset += BATCH_SIZE
            processed += len(rows)
            print(f"Processed {processed}/{total_rows} | Inserted cleaned: {inserted_cleaned} | Inserted duplicates: {inserted_duplicates}")

    PG_CONN.close()
    print("Migration completed and connections closed.")


if __name__ == "__main__":
    migrate_logs()