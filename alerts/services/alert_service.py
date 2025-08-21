from alerts.services.postgres_service import (
    insert_cleaned_alert, insert_duplicate_alert, fetch_alert_by_id
)
from alerts.services.vector_service import (
    get_embedding, search_vector_store, store_vector
)

SIMILARITY_THRESHOLD = 0.85

def normalize_alert(raw_alert: dict) -> dict:
    return {
        "incident_id": raw_alert.get("incident_id"),
        "observed_value": raw_alert.get("observed_value"),
        "policy_name": raw_alert.get("policy_name"),
        "condition_name": raw_alert.get("condition_name"),
        "subject": raw_alert.get("documentation", {}).get("subject"), 
        "display_name": raw_alert.get("metric", {}).get("displayName"), 
        "severity": raw_alert.get("severity"),
        "summary": raw_alert.get("summary"),
        "log_data": raw_alert,
    }

def process_alert(raw_alert: dict):
    alert = normalize_alert(raw_alert)
    incident_id = alert["incident_id"]

    # Step 2 - Check exact duplicate (incident_id)
    existing = fetch_alert_by_id(incident_id)
    if existing:
        insert_duplicate_alert(incident_id, alert)
        return {
            "status": "Duplicate",
            "message": f"Incident {incident_id} already exists",
            "incident_id": incident_id,
        }

    # Step 3 - Build embedding text
    alert_text = " | ".join([
        str(alert.get("incident_id", "")),
        str(alert.get("observed_value", "")),
        str(alert.get("policy_name", "")),
        str(alert.get("condition_name", "")),
        str(alert.get("subject", "")),
        str(alert.get("display_name", "")),
        str(alert.get("severity", "")),
        str(alert.get("summary", "")),
    ])

    vector = get_embedding(alert_text)
    if not vector:
        insert_cleaned_alert(alert)
        return {
            "status": "Unique",
            "message": "Stored (embedding failed)",
            "incident_id": incident_id,
        }

    # Step 4 - Semantic duplicate check
    match = search_vector_store(vector, limit=1)
    if match and match.get("similarity", 0) >= SIMILARITY_THRESHOLD:
        insert_duplicate_alert(match["incident_id"], alert)
        return {
            "status": "Duplicate alert detected (semantic match)",
            "message": "An alert with similar content already exists.",
            "incident_id": f"This alert matches existing incident ID: {match['incident_id']}",
        }

    # Step 5 - Store unique
    insert_cleaned_alert(alert)
    store_vector(vector, **alert)
    return {
        "status": "New alert created",
        "message": "This is a new alert and has been stored successfully.",
        "incident_id": f"New incident created with ID: {incident_id}",
    }
