import ollama
from alerts.services.weaviate_client import weaviate_store, weaviate_search

def get_embedding(text: str) -> list[float]:
    try:
        response = ollama.embed(model="nomic-embed-text", input=text)
        vec = response.get("embeddings", [])
        if not vec:
            return []
        flat = []
        for item in vec:
            if isinstance(item, list):
                flat.extend(float(x) for x in item)
            else:
                flat.append(float(item))
        return flat
    except Exception as e:
        print(f"Error getting embedding: {e}")
        return []

def store_vector(vector: list[float], **fields) -> None:
    """Thin wrapper so callers can pass the same keyword args as weaviate_store."""
    try:
        weaviate_store(vector, **fields)
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