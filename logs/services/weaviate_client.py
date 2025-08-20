import weaviate

client = weaviate.Client(url="http://localhost:8080")


def create_schema():
    """Ensure the Incident schema exists in Weaviate."""
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
            {"name": "kubernetesDetails", "dataType": ["text"]},
            {"name": "timestamp", "dataType": ["date"]},
        ]
    }
    existing_classes = client.schema.get().get("classes", [])
    if not any(c.get("class") == "Incident" for c in existing_classes):
        client.schema.create_class(schema)


def weaviate_store(vector, incident_id, alert_text, timestamp):
    """Store a unique log into Weaviate."""
    try:
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
    """Search for similar vectors in Weaviate."""
    try:
        if not vector:
            return []

        result = (
            client.query
            .get("Incident", ["incident_id", "message"])
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
            if match.get("incident_id"):
                safe_matches.append(match)

        return safe_matches

    except Exception as e:
        print(f"Error searching vector store: {e}")
        return []

def delete_all_weaviate_data():
    """Delete the entire Incident class in Weaviate to start fresh."""
    print("Deleting the entire 'Incident' class in Weaviate...")
    try:
        client.schema.delete_class("Incident")
        print("Incident class deleted successfully.")
    except weaviate.exceptions.UnexpectedStatusCodeException as e:
        print(f"Error deleting class: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")