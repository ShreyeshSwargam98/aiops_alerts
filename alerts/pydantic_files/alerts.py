from pydantic import BaseModel, Extra
from typing import Optional, Dict, Any

class AlertRequest(BaseModel):
    incident_id: str
    observed_value: Optional[str] = None
    policy_name: Optional[str] = None
    condition_name: Optional[str] = None
    subject: Optional[str] = None
    display_name: Optional[str] = None
    severity: Optional[str] = None
    summary: Optional[str] = None
    log_data: Optional[Dict] = None

    class Config:
        extra = "allow"
