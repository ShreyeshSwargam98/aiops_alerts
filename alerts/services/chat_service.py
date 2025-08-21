from psycopg2.extras import RealDictCursor
import ollama
from alerts.pydantic_files.chat_service import ChatRequest, ChatResponse
from alerts.services.postgres_service import get_pg_connection
from alerts.services.prompts import QUERY_PROMPT

def add_chat_message(chat_req: ChatRequest, model: str = "llama3:latest") -> ChatResponse:
    # Fetch the row for the given incident_id (cleaned_alerts PK is incident_id)
    conn = get_pg_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """
        SELECT *
        FROM cleaned_alerts
        WHERE incident_id = %s
        """,
        (chat_req.incident_id,),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()

    # Prepare context for LLM
    if row:
        context_text = "\n".join(f"{key}: {value}" for key, value in row.items())
    else:
        context_text = "No related incident found."

    # Get LLM response
    llama_response = ollama.chat(
        model=model,
        messages=[
            {
                "role": "user",
                "content": QUERY_PROMPT.format(
                    context_str=context_text,
                    query_str=chat_req.query
                )
            }
        ]
    )
    response_text = llama_response["message"]["content"]

    # Save chat response to DB (timestamp is defaulted by DB)
    conn = get_pg_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """
        INSERT INTO chat_messages (incident_id, query, response)
        VALUES (%s, %s, %s)
        RETURNING id, incident_id, query, response, timestamp
        """,
        (chat_req.incident_id, chat_req.query, response_text),
    )
    saved_row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()

    return ChatResponse(**saved_row)

def get_chat_messages(incident_id: str) -> list[ChatResponse]:
    """Fetch all chat history for an incident from Postgres chat_messages table"""
    conn = get_pg_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """
        SELECT id, incident_id, query, response, timestamp
        FROM chat_messages
        WHERE incident_id = %s
        ORDER BY timestamp ASC
        """,
        (incident_id,),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return [ChatResponse(**row) for row in rows]