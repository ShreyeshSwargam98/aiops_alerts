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