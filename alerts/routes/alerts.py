from fastapi import APIRouter, HTTPException
from alerts.pydantic_files.alerts import AlertRequest
from alerts.pydantic_files.chat_service import ChatRequest, ChatResponse
from alerts.services.alert_service import process_alert
from typing import List, Dict
from alerts.services.postgres_service import (
    fetch_alerts,
    fetch_grouped_alerts,
    get_alert_counts,
    get_alert_summary,
    fetch_alert_by_id,
)
from alerts.services.chat_service import add_chat_message, get_chat_messages

router = APIRouter(tags=["Alerts"])

@router.post("/deduplicate_alert")
def deduplicate_alert(alert: AlertRequest):
    """Endpoint for Flow Designer (or external services) to send alerts."""
    return process_alert(alert.model_dump())

@router.get("/alerts", response_model=List[Dict])
def list_alerts():
    """List all deduplicated alerts (from cleaned_alerts)."""
    return fetch_alerts()

@router.get("/alerts/grouped")
def get_grouped_alerts():
    """Return alerts grouped by incident_id including duplicates."""
    return fetch_grouped_alerts()

@router.get("/alerts/counts")
def alert_counts():
    """Return counts of total alerts, deduplicated alerts, and severity distribution."""
    return get_alert_counts()

@router.get("/alerts/summary")
def alerts_summary():
    """Return aggregated summary for dashboards."""
    return get_alert_summary()

@router.post("/alerts/grouped/chat", response_model=ChatResponse)
def create_chat_message(chat_req: ChatRequest):
    """Attach a chat message to a grouped alert thread."""
    return add_chat_message(chat_req)

@router.get("/alerts/grouped/chat/{incident_id}", response_model=List[ChatResponse])
def fetch_chat_messages(incident_id: str):
    """Fetch all chat messages for a given incident_id."""
    return get_chat_messages(incident_id)

@router.get("/alerts/{incident_id}", response_model=Dict)
def get_alert_detail(incident_id: str):
    """Get full alert details by incident_id."""
    alert = fetch_alert_by_id(incident_id)
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    return alert