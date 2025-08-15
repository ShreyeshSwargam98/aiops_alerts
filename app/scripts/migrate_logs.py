# Run this script only once. Running it again will create duplicate entries.
import os
import uuid
import json
import psycopg2
from datetime import datetime, timezone
from dotenv import load_dotenv
import ollama
import numpy as np

from app.services.weaviate_client import weaviate_store, weaviate_search
import warnings

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
    """Get vector embedding from Ollama using 'nomic-embed-text'."""
    try:
        response = ollama.embed(
            model="nomic-embed-text",
            input=text
        )
        vector = response.get("embeddings", [])
        if not vector:
            return []
        # Flatten and cast to float32 for Weaviate
        flat_vector = np.array(vector).flatten().astype(np.float32).tolist()
        return flat_vector
    except Exception as e:
        print(f"Error generating embedding: {e}")
        return []

def migrate_logs():
    with PG_CONN.cursor() as cur:
        # Count total rows
        cur.execute("SELECT COUNT(*) FROM all_logs")
        total_rows = cur.fetchone()[0]
        print(f"Total rows in all_logs: {total_rows}")

        offset = 0
        processed = 0
        skipped = 0
        inserted = 0

        while offset < total_rows:
            cur.execute("""
                SELECT id, date, time, appName, serviceName, job, label, level, message, kubernetesDetails
                FROM all_logs
                ORDER BY date, time
                LIMIT %s OFFSET %s
            """, (BATCH_SIZE, offset))
            rows = cur.fetchall()

            for row in rows:
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
                    print(f"Skipping log {log_id} due to embedding failure")
                    skipped += 1
                    continue

                matches = weaviate_search(vector, limit=1)
                if matches and isinstance(matches, list) and len(matches) > 0:
                    top_match = matches[0]
                    if top_match.get("similarity", 0) >= SIMILARITY_THRESHOLD:
                        skipped += 1
                        continue

                incident_id = str(uuid.uuid4())[:8]
                k8s_details_json = json.dumps(kubernetesDetails) if kubernetesDetails else None

                cur.execute("""
                    INSERT INTO cleaned_logs (id, date, time, appName, serviceName, job, label, level, message, kubernetesDetails)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    incident_id, date, time, appName, serviceName, job,
                    label, level, message, k8s_details_json
                ))

                timestamp = datetime.combine(date, time).replace(tzinfo=timezone.utc)
                weaviate_store(vector, incident_id, alert_text, timestamp)

                inserted += 1

            PG_CONN.commit()
            offset += BATCH_SIZE
            processed += len(rows)
            print(f"Processed {processed}/{total_rows} | Inserted: {inserted} | Skipped: {skipped}")

    PG_CONN.close()
    print("Migration completed and connections closed.")

if __name__ == "__main__":
    migrate_logs()