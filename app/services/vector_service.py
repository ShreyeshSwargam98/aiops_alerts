from datetime import datetime, timezone
from app.services.weaviate_client import weaviate_store, weaviate_search
import ollama

import ollama

def get_embedding(text: str) -> list[float]:
    try:
        response = ollama.embed(
            model='nomic-embed-text',
            input=text
        )
        vector = response.get('embeddings', [])
        if not vector:
            print("Warning: embedding returned empty vector")
            return []
        flat_vector = []
        for item in vector:
            if isinstance(item, list):
                for sub_item in item:
                    flat_vector.append(float(sub_item))
            else:
                flat_vector.append(float(item))

        return flat_vector

    except Exception as e:
        print(f"Error getting embedding: {e}")
        return []

def store_vector(vector: list[float], incident_id: str, alert_text: str) -> None:
    try:
        current_time = datetime.now(timezone.utc)
        weaviate_store(vector, incident_id, alert_text, current_time)
    except Exception as e:
        print(f"Error storing vector: {e}")


def search_vector_store(vector: list[float], limit: int = 1) -> dict | None:
    try:
        matches = weaviate_search(vector, limit=limit)
        if matches:
            match = matches[0]
            if "similarity" not in match:
                match["similarity"] = 0.9
            return match
    except Exception as e:
        print(f"Error searching vector store: {e}")
    return None