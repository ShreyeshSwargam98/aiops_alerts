# Run this script only once. Running it again will create duplicate entries.
# python -m alerts.scripts.migrate_logs

import os, json, psycopg2, warnings
from dotenv import load_dotenv
import ollama, numpy as np
from alerts.services.weaviate_client import (
    create_schema, weaviate_store, weaviate_search, delete_all_weaviate_data
)

warnings.simplefilter("ignore", ResourceWarning)
load_dotenv()

BATCH_SIZE = 500
SIMILARITY_THRESHOLD = 0.85

PG_CONN = psycopg2.connect(
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
)

def get_embedding(text: str):
    try:
        response = ollama.embed(model="nomic-embed-text", input=text)
        vector = response.get("embeddings", [])
        if not vector:
            return []
        return np.array(vector).flatten().tolist()
    except Exception as e:
        print(f"Error generating embedding: {e}")
        return []

def insert_into_all_alerts(cur, row):
    """Insert raw alert into all_alerts (audit log)"""
    (
        incident_id, observed_value, policy_name, condition_name,
        subject, display_name, severity, summary, log_data, created_at
    ) = row

    cur.execute("""
        INSERT INTO all_alerts (
            incident_id, observed_value, policy_name, condition_name,
            subject, display_name, severity, summary, log_data, created_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (incident_id) DO NOTHING
    """, (
        incident_id, observed_value, policy_name, condition_name,
        subject, display_name, severity, summary,
        json.dumps(log_data) if log_data else None,
        created_at
    ))

def insert_duplicate(cur, top_match, row):
    """Insert duplicate into duplicate_alerts (reference original incident_id)"""
    (
        incident_id, observed_value, policy_name, condition_name,
        subject, display_name, severity, summary, log_data, created_at
    ) = row

    original_incident_id = top_match["incident_id"]

    cur.execute("""
        INSERT INTO duplicate_alerts (
            incident_id, observed_value, policy_name, condition_name,
            subject, display_name, severity, summary, log_data, created_at
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        original_incident_id, observed_value, policy_name, condition_name,
        subject, display_name, severity, summary,
        json.dumps(log_data) if log_data else None,
        created_at
    ))

def migrate_alerts():
    create_schema()
    delete_all_weaviate_data()

    with PG_CONN.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM all_alerts")
        total_rows = cur.fetchone()[0]
        print(f"Total rows in all_alerts: {total_rows}")

        offset = 0
        processed = 0
        inserted_cleaned = 0
        inserted_duplicates = 0

        while offset < total_rows:
            cur.execute("""
                SELECT incident_id, observed_value, policy_name, condition_name,
                       subject, display_name, severity, summary, log_data, created_at
                FROM all_alerts
                ORDER BY created_at
                LIMIT %s OFFSET %s
            """, (BATCH_SIZE, offset))
            rows = cur.fetchall()

            for row in rows:
                (
                    incident_id, observed_value, policy_name, condition_name,
                    subject, display_name, severity, summary, log_data, created_at
                ) = row

                # Step 1 - Always insert into all_alerts (audit trail)
                insert_into_all_alerts(cur, row)

                # Step 2 - Build embedding text
                alert_text = " | ".join([
                    str(incident_id or ""),
                    str(observed_value or ""),
                    str(policy_name or ""),
                    str(condition_name or ""),
                    str(subject or ""),
                    str(display_name or ""),
                    str(severity or ""),
                    str(summary or ""),
                    str(log_data or "")
                ])

                vector = get_embedding(alert_text)
                if not vector:
                    continue

                # Step 3 - Semantic duplicate check
                matches = weaviate_search(vector, limit=1)
                if matches and matches[0].get("similarity", 0) >= SIMILARITY_THRESHOLD:
                    insert_duplicate(cur, matches[0], row)
                    inserted_duplicates += 1
                    continue

                # Step 4 - Insert cleaned alert
                cur.execute("""
                    INSERT INTO cleaned_alerts (
                        incident_id, observed_value, policy_name, condition_name,
                        subject, display_name, severity, summary, log_data, created_at
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (incident_id) DO NOTHING
                """, (
                    incident_id, observed_value, policy_name, condition_name,
                    subject, display_name, severity, summary,
                    json.dumps(log_data) if log_data else None,
                    created_at
                ))

                # Step 5 - Store vector
                weaviate_store(
                    vector,
                    incident_id,
                    observed_value=observed_value,
                    policy_name=policy_name,
                    condition_name=condition_name,
                    subject=subject,
                    display_name=display_name,
                    severity=severity,
                    summary=summary,
                    log_data=log_data,
                )
                inserted_cleaned += 1

            PG_CONN.commit()
            offset += BATCH_SIZE
            processed += len(rows)
            print(f"Processed {processed}/{total_rows} | Cleaned: {inserted_cleaned} | Duplicates: {inserted_duplicates}")

    PG_CONN.close()
    print("Migration completed")

if __name__ == "__main__":
    migrate_alerts()
