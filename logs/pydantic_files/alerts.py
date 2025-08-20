from pydantic import BaseModel
from typing import Dict, Optional

class AlertRequest(BaseModel):
    appName: str
    serviceName: str
    job: str
    label: str
    level: str
    message: str
    kubernetesDetails: Optional[Dict] = None  # JSON field