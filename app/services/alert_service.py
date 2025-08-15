from datetime import datetime, timezone
import uuid
from app.services.postgres_service import insert_cleaned_log, insert_duplicate_log
from app.services.vector_service import get_embedding, weaviate_search, weaviate_store

SIMILARITY_THRESHOLD = 0.85

def process_alert(alert: dict):
    alert_text = " | ".join([
        alert.get("appName", ""),
        alert.get("serviceName", ""),
        alert.get("job", ""),
        alert.get("label", ""),
        alert.get("level", ""),
        alert.get("message", ""),
        str(alert.get("kubernetesDetails", ""))
    ])

    vector = get_embedding(alert_text)
    timestamp = datetime.now(timezone.utc)

    if not vector:
        # Embedding failed → treat as unique
        new_incident_id = str(uuid.uuid4())[:8]
        insert_cleaned_log(incident_id=new_incident_id, timestamp=timestamp, **alert)
        return {
            "status": "unique",
            "message": "Alert stored in cleaned_logs (embedding failed).",
            "incident_id": new_incident_id
        }

    # Search for duplicates
    matches = weaviate_search(vector, limit=1)
    if matches:
        top_match = matches[0]
        similarity = top_match.get("similarity", 0)
        original_incident_id = top_match.get("incident_id")

        if similarity >= SIMILARITY_THRESHOLD and original_incident_id:
            # Duplicate found → store in duplicate_logs
            insert_duplicate_log(
                original_incident_id=original_incident_id,
                timestamp=timestamp,
                **alert
            )
            return {
                "status": "Duplicate alert detected",
                "message": "An alert with similar content already exists.",
                "incident_id": f"This alert matches an existing incident with ID: {original_incident_id}",
            }

    # Unique alert → store in cleaned_logs & Weaviate
    new_incident_id = str(uuid.uuid4())[:8]
    insert_cleaned_log(incident_id=new_incident_id, timestamp=timestamp, **alert)
    weaviate_store(vector, new_incident_id, alert_text, timestamp)

    return {
            "status": "New alert created",
            "message": "This is a new alert and has been stored successfully.",
            "incident_id": f"New incident created with ID: {new_incident_id}"  
        }
