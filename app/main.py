from fastapi import FastAPI, HTTPException
import uvicorn
from fastapi.responses import JSONResponse
from app.services.weaviate_client import create_schema
from app.routes import alerts

app = FastAPI()

# Ensure Weaviate schema exists at startup
create_schema()

#alert to check de duplication
app.include_router(alerts.router)