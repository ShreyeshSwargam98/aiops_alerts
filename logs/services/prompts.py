from llama_index.core import PromptTemplate

QUERY_PROMPT = PromptTemplate(
    "You are a helpful assistant. "
    "Answer the user query strictly using the provided context. "
    "Do not explain, do not ask follow-up questions, do not add uncertainty. "
    "Return only a single concise answer. "
    "If the answer is found in the context, give it directly. "
    "If not found, say 'No relevant information found.'\n\n"
    "Context:\n{context_str}\n\n"
    "Query: {query_str}\n\n"
    "Answer:"
)
