from fastapi import APIRouter, HTTPException
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

@router.get("/alerts/summary")
def alerts_summary():
    """
    Returns alert summary for dashboards or monitoring.
    """
    summary = get_alert_summary()
    return summary

@router.get("/alerts/{incident_id}", response_model=Dict)
def get_alert_detail(incident_id: str):
    """
    Get full alert details by incident_id from cleaned_logs.
    """
    alert = fetch_alert_by_id(incident_id)
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    return alert