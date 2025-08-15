import psycopg2
from psycopg2.extras import Json, RealDictCursor
from dotenv import load_dotenv
import os
import json

load_dotenv()

def get_pg_connection():
    """Create and return a new Postgres connection."""
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT")
    )

def insert_cleaned_log(incident_id, timestamp, appName, serviceName, job, label, level, message, kubernetesDetails=None):
    """Insert a new cleaned log into cleaned_logs table."""
    conn = get_pg_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO cleaned_logs (
            id, date, time, appName, serviceName, job, label, level, message, kubernetesDetails
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        incident_id,
        timestamp.date(),
        timestamp.time(),
        appName,
        serviceName,
        job,
        label,
        level,
        message,
        Json(kubernetesDetails) if kubernetesDetails else None
    ))
    conn.commit()
    cur.close()
    conn.close()
    
def fetch_alerts(limit: int = 10):
    """Fetch list of alerts with pagination."""
    conn = get_pg_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT id as incident_id, appName, serviceName, job, label, level, message,
               kubernetesDetails, date, time
        FROM cleaned_logs
        ORDER BY date DESC, time DESC
        LIMIT %s
    """, (limit,))
    alerts = cur.fetchall()
    cur.close()
    conn.close()
    return alerts