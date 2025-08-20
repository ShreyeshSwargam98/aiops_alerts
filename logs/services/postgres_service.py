import psycopg2
from psycopg2.extras import Json, RealDictCursor
from dotenv import load_dotenv
import os

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

def insert_duplicate_log(original_incident_id, timestamp, appName, serviceName, job, label, level, message, kubernetesDetails=None):
    """Insert a duplicate alert into duplicate_logs table referencing the original incident_id."""
    conn = get_pg_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO duplicate_logs (
            incident_id, date, time, appName, serviceName, job, label, level, message, kubernetesDetails
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        original_incident_id,
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
    
def fetch_alerts():
    """Fetch list of alerts with pagination."""
    conn = get_pg_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT id as incident_id, appName, serviceName, job, label, level, message,
               kubernetesDetails, date, time
        FROM cleaned_logs
        ORDER BY date DESC, time DESC
    """)
    alerts = cur.fetchall()
    cur.close()
    conn.close()
    return alerts

def fetch_grouped_alerts():
    """
    Fetch alerts from cleaned_logs and duplicate_logs and group them by incident_id.
    """
    conn = get_pg_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # Fetch all cleaned logs
    cur.execute("""
        SELECT id as incident_id, appName, level, message, kubernetesDetails, date, time
        FROM cleaned_logs
        ORDER BY date DESC, time DESC
    """)
    cleaned_logs = cur.fetchall()

    # Fetch all duplicate logs
    cur.execute("""
        SELECT incident_id as original_incident_id, appName, level, message, kubernetesDetails, date, time
        FROM duplicate_logs
        ORDER BY date DESC, time DESC
    """)
    duplicate_logs = cur.fetchall()

    cur.close()
    conn.close()

    grouped = {}

    # Group cleaned logs by their incident_id
    for log in cleaned_logs:
        incident_id = log["incident_id"]
        if incident_id not in grouped:
            grouped[incident_id] = []

        grouped[incident_id].append({
            "source": "cleaned",
            "message": log["message"],
            "level": log["level"].lower(),
            "appName": log.get("appName"),
            "timestamp": f"{log['date']} {log['time']}"
        })

    # Include duplicates under their original incident_id
    for dup in duplicate_logs:
        incident_id = dup["original_incident_id"]
        if incident_id not in grouped:
            grouped[incident_id] = []

        grouped[incident_id].append({
            "source": "duplicate",
            "message": dup["message"],
            "level": dup["level"].lower(),
            "appName": dup.get("appName"),
            "timestamp": f"{dup['date']} {dup['time']}"
        })

    return grouped

def get_alert_counts():
    """Fetch summary counts from cleaned_logs."""
    conn = get_pg_connection()
    cur = conn.cursor()

    # Total alerts
    cur.execute("SELECT COUNT(*) FROM cleaned_logs")
    total_alerts = cur.fetchone()[0]

    # Total deduplicated alerts (difference between all_logs and cleaned_logs)
    cur.execute("SELECT (SELECT COUNT(*) FROM all_logs) - (SELECT COUNT(*) FROM cleaned_logs)")
    total_deduplicated = cur.fetchone()[0]

    # Count by severity/level
    cur.execute("""
        SELECT level, COUNT(*) 
        FROM cleaned_logs
        GROUP BY level
    """)
    severity_counts_rows = cur.fetchall()
    severity_counts = {row[0]: row[1] for row in severity_counts_rows}

    cur.close()
    conn.close()

    return {
        "totalAlertsCount": total_alerts,
        "totalDeduplicatedCount": total_deduplicated,
        "severityCounts": severity_counts
    }
    
def get_alert_summary():
    """Fetch summary for dashboard/health endpoint."""
    counts = get_alert_counts()  # reuse existing function

    total_alerts = counts["totalAlertsCount"]
    deduplicated = counts["totalDeduplicatedCount"]
    reduction = round((deduplicated / total_alerts) * 100, 2) if total_alerts > 0 else 0

    return {
        "totalAlerts": total_alerts,
        "deduplicated": deduplicated,
        "reduction": reduction,
        "severityCounts": counts["severityCounts"]
    }
    
def fetch_alert_by_id(incident_id: str):
    """Fetch single alert by incident_id."""
    conn = get_pg_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT id as incident_id, appName, serviceName, job, label, level, message,
               kubernetesDetails, date, time
        FROM cleaned_logs
        WHERE id = %s
    """, (incident_id,))
    alert = cur.fetchone()
    cur.close()
    conn.close()
    return alert