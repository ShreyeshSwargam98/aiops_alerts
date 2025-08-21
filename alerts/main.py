from fastapi import FastAPI
from alerts.services.weaviate_client import create_schema
from alerts.routes import alerts

app = FastAPI()

# Ensure Weaviate schema exists at startup
create_schema()

#alert to check de duplication
app.include_router(alerts.router)