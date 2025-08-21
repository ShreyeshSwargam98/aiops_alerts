import psycopg2
from psycopg2.extras import RealDictCursor, Json
import os
from dotenv import load_dotenv

load_dotenv()


def get_pg_connection():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
    )


def insert_cleaned_alert(alert: dict):
    """Insert into cleaned_alerts (new schema)."""
    conn = get_pg_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO cleaned_alerts (
            incident_id, observed_value, policy_name, condition_name, subject,
            display_name, severity, summary, log_data
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            alert["incident_id"],
            alert.get("observed_value"),
            alert.get("policy_name"),
            alert.get("condition_name"),
            alert.get("subject"),
            alert.get("display_name"),
            alert.get("severity"),
            alert.get("summary"),
            Json(alert.get("log_data")),  # always full payload
        ),
    )
    conn.commit()
    cur.close()
    conn.close()


def insert_duplicate_alert(original_incident_id: str, alert: dict):
    """Insert into duplicate_alerts, referencing original cleaned incident."""
    conn = get_pg_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO duplicate_alerts (
            incident_id, observed_value, policy_name, condition_name, subject,
            display_name, severity, summary, log_data
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            original_incident_id,
            alert.get("observed_value"),
            alert.get("policy_name"),
            alert.get("condition_name"),
            alert.get("subject"),
            alert.get("display_name"),
            alert.get("severity"),
            alert.get("summary"),
            Json(alert.get("log_data")),  # always full payload
        ),
    )
    conn.commit()
    cur.close()
    conn.close()


def fetch_alerts():
    """Fetch cleaned alerts (latest first)."""
    conn = get_pg_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """
        SELECT incident_id, observed_value, policy_name, condition_name,
               subject, display_name, severity, summary, log_data, created_at
        FROM cleaned_alerts
        ORDER BY created_at DESC
        """
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def fetch_grouped_alerts():
    """Group cleaned + duplicates by incident_id for UI display."""
    conn = get_pg_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("""SELECT * FROM cleaned_alerts ORDER BY created_at DESC""")
    cleaned = cur.fetchall()

    cur.execute("""SELECT * FROM duplicate_alerts ORDER BY created_at DESC""")
    duplicates = cur.fetchall()

    cur.close()
    conn.close()

    grouped = {}
    for c in cleaned:
        grouped[c["incident_id"]] = [
            {
                "source": "cleaned",
                "severity": c.get("severity"),
                "summary": c.get("summary"),
                "timestamp": c["created_at"].isoformat(),
            }
        ]
    for d in duplicates:
        inc_id = d["incident_id"]
        grouped.setdefault(inc_id, []).append(
            {
                "source": "duplicate",
                "severity": d.get("severity"),
                "summary": d.get("summary"),
                "timestamp": d["created_at"].isoformat(),
            }
        )
    return grouped


def get_alert_counts():
    """Counts for dashboard/metrics."""
    conn = get_pg_connection()
    cur = conn.cursor()

    # Count cleaned
    cur.execute("SELECT COUNT(*) FROM cleaned_alerts")
    cleaned_count = cur.fetchone()[0]

    # Count duplicates
    cur.execute("SELECT COUNT(*) FROM duplicate_alerts")
    duplicate_count = cur.fetchone()[0]

    # Severity breakdown from cleaned only
    cur.execute("SELECT severity, COUNT(*) FROM cleaned_alerts GROUP BY severity")
    severity = {row[0]: row[1] for row in cur.fetchall()}

    cur.close()
    conn.close()

    return {
        "totalAlertsCount": cleaned_count,
        "totalDuplicateCount": duplicate_count,
        "severityCounts": severity,
    }


def fetch_alert_by_id(incident_id: str):
    """Fetch a single cleaned alert by ID."""
    conn = get_pg_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM cleaned_alerts WHERE incident_id=%s", (incident_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row


def get_alert_summary():
    """Fetch summary for dashboard/health endpoint."""
    counts = get_alert_counts()
    total = counts["totalAlertsCount"]
    dup = counts["totalDuplicateCount"]
    reduction = round((dup / (total + dup)) * 100, 2) if (total + dup) > 0 else 0

    return {
        "totalAlerts": total,
        "duplicates": dup,
        "reduction": reduction,
        "severityCounts": counts["severityCounts"],
    }
