import weaviate

client = weaviate.Client(url="http://localhost:8080")

def create_schema():
    schema = {
        "class": "Incident",
        "vectorizer": "none",
        "properties": [
            {"name": "incident_id", "dataType": ["string"]},
            {"name": "observed_value", "dataType": ["string"]},
            {"name": "policy_name", "dataType": ["string"]},
            {"name": "condition_name", "dataType": ["string"]},
            {"name": "subject", "dataType": ["string"]},
            {"name": "display_name", "dataType": ["string"]},
            {"name": "severity", "dataType": ["string"]},
            {"name": "summary", "dataType": ["string"]},
            {"name": "log_data", "dataType": ["string"]},
        ]
    }
    existing = client.schema.get().get("classes", [])
    if not any(c.get("class") == "Incident" for c in existing):
        client.schema.create_class(schema)

def weaviate_store(vector, incident_id, **fields):
    props = {**fields, "incident_id": incident_id}
    props["log_data"] = str(props.get("log_data")) if props.get("log_data") else None
    client.data_object.create(data_object=props, class_name="Incident", vector=vector)

def weaviate_search(vector, limit=1):
    if not vector:
        return []
    result = client.query.get("Incident", [
        "incident_id", "observed_value", "policy_name", "condition_name",
        "subject", "display_name", "severity", "summary", "log_data"
    ]).with_near_vector({"vector": vector}).with_limit(limit).with_additional(["distance"]).do()
    matches = result.get("data", {}).get("Get", {}).get("Incident", [])
    for m in matches:
        distance = m.get("_additional", {}).get("distance", 1.0)
        m["similarity"] = max(0.0, 1 - distance)
    return matches

def delete_all_weaviate_data():
    try:
        client.schema.delete_class("Incident")
    except Exception as e:
        print(f"Error deleting schema: {e}")
