from pydantic import BaseModel
from datetime import datetime
from typing import Optional


class ChatRequest(BaseModel):
    incident_id: str
    query: str 

class ChatResponse(BaseModel):
    id: int
    incident_id: str
    query: str 
    response: str
    timestamp: datetime
