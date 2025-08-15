import weaviate
from datetime import datetime, timezone

client = weaviate.Client(url="http://localhost:8080")

def create_schema():
    """Ensure the new Incident schema exists with updated fields."""
    schema = {
        "class": "Incident",
        "vectorizer": "none",
        "properties": [
            {"name": "incident_id", "dataType": ["string"]},
            {"name": "appName", "dataType": ["string"]},
            {"name": "serviceName", "dataType": ["string"]},
            {"name": "job", "dataType": ["string"]},
            {"name": "label", "dataType": ["string"]},
            {"name": "level", "dataType": ["string"]},
            {"name": "message", "dataType": ["string"]},
            {"name": "kubernetesDetails", "dataType": ["text"]},  # JSON stored as text
            {"name": "timestamp", "dataType": ["date"]},
        ]
    }

    existing_classes = client.schema.get().get("classes", [])
    if not any(c.get("class") == "Incident" for c in existing_classes):
        client.schema.create_class(schema)

def weaviate_store(vector, incident_id, alert_text):
    try:
        timestamp = datetime.now(timezone.utc)
        properties = {
            "incident_id": incident_id,
            "message": alert_text,
            "timestamp": timestamp.isoformat()
        }
        client.data_object.create(
            data_object=properties,
            class_name="Incident",
            vector=vector
        )
    except Exception as e:
        print(f"Error storing vector in Weaviate: {e}")

def weaviate_search(vector, limit=1):
    try:
        if not vector:
            print("Empty vector provided, skipping search")
            return []

        result = (
            client.query
            .get("Incident", ["incident_id", "message", "timestamp"])
            .with_near_vector({"vector": vector})
            .with_additional(["distance"])
            .with_limit(limit)
            .do()
        )

        incidents = result.get("data", {}).get("Get", {}).get("Incident", [])
        safe_matches = []

        for match in incidents:
            distance = match.get("_additional", {}).get("distance", 1.0)
            similarity = max(0.0, 1 - distance)
            match["similarity"] = similarity
            safe_matches.append(match)

        return safe_matches

    except Exception as e:
        print(f"Error searching vector store: {e}")
        return []
