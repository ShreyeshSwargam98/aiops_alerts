import uuid
from datetime import datetime, timezone
from app.services.postgres_service import insert_cleaned_log
from app.services.vector_service import *

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
    if not vector:
        print("Skipping Weaviate search, empty vector")
        new_incident_id = str(uuid.uuid4())[:8]
        timestamp = datetime.now(timezone.utc)
        insert_cleaned_log(
            incident_id=new_incident_id,
            timestamp=timestamp,
            **alert
        )
        return {
            "status": "new",
            "incident_id": new_incident_id,
            "similarity": None
        }

    matches = weaviate_search(vector)
    if matches:
        best_match = matches[0]
        if best_match.get("similarity", 0) >= SIMILARITY_THRESHOLD:
            # Duplicate found
            return {
                "status": "Duplicate alert detected",
                "message": "An alert with similar content already exists.",
                "incident_id": f"This alert matches an existing incident with ID: {best_match['incident_id']}",
            }


    # No match → new alert
    new_incident_id = str(uuid.uuid4())[:8]
    timestamp = datetime.now(timezone.utc)

    # Store in Weaviate
    weaviate_store(vector, new_incident_id, alert_text)

    # Store in Postgres cleaned_logs
    insert_cleaned_log(
        incident_id=new_incident_id,
        timestamp=timestamp,
        **alert
    )

    return {
            "status": "New alert created",
            "message": "This is a new alert and has been stored successfully.",
            "incident_id": f"New incident created with ID: {new_incident_id}"  
        }