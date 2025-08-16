from fastapi import APIRouter, Query, HTTPException
from app.pydantic_files.alerts import AlertRequest
from app.services.alert_service import process_alert
from typing import List, Dict
from app.services.postgres_service import *

router = APIRouter(tags=["Alerts"])

@router.post("/deduplicate_alert")
def deduplicate_alert(alert: AlertRequest):
    """
    Endpoint for Flow Designer to send alerts.
    """
    return process_alert(alert.model_dump())

@router.get("/alerts", response_model=List[Dict])
def list_alerts():
    """
    List all alerts from cleaned_logs with optional pagination.
    """
    alerts = fetch_alerts()
    return alerts

@router.get("/alerts/grouped")
def get_grouped_alerts():
    """
    Return alerts grouped by incident_id, including duplicates.
    """
    grouped = fetch_grouped_alerts()
    return grouped

@router.get("/alerts/counts")
def alert_counts():
    return get_alert_counts()