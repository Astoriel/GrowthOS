"""REST API server for GrowthOS."""
from __future__ import annotations

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from growth_os.connectors import get_connector
from growth_os.services import AnalysisService, CatalogService, ReportingService

app = FastAPI(title="GrowthOS REST API", version="1.0.0")


class QueryRequest(BaseModel):
    sql: str
    offset: int = 0
    limit: int = 50


@app.get("/health")
def health():
    connector = get_connector()
    tables = connector.get_tables()
    return {"status": "ok", "tables": tables}


@app.post("/query")
def run_query(req: QueryRequest):
    try:
        service = CatalogService(get_connector())
        envelope = service.run_query(req.sql, offset=req.offset, limit=req.limit)
        return {"title": envelope.title, "body": envelope.body}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/tables")
def list_tables():
    service = CatalogService(get_connector())
    envelope = service.list_tables()
    return {"title": envelope.title, "body": envelope.body}


@app.get("/tables/{table_name}")
def describe_table(table_name: str):
    service = CatalogService(get_connector())
    envelope = service.describe_table(table_name)
    return {"title": envelope.title, "body": envelope.body}


@app.get("/analysis/funnel")
def analyze_funnel(
    events_table: str = "user_events",
    steps: str = "signup,activation,purchase",
    date_from: str = "",
    date_to: str = "",
):
    service = AnalysisService(get_connector())
    try:
        envelope = service.analyze_funnel(events_table, steps, date_from, date_to)
        return {"title": envelope.title, "body": envelope.body}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/analysis/cac-ltv")
def compute_cac_ltv(
    spend_table: str = "marketing_spend",
    events_table: str = "user_events",
):
    service = AnalysisService(get_connector())
    try:
        envelope = service.compute_cac_ltv(spend_table, events_table)
        return {"title": envelope.title, "body": envelope.body}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/analysis/churn")
def analyze_churn(events_table: str = "user_events", inactive_days: int = 30):
    service = AnalysisService(get_connector())
    try:
        envelope = service.analyze_churn(events_table, inactive_days)
        return {"title": envelope.title, "body": envelope.body}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/analysis/anomalies")
def detect_anomalies(
    table: str = "marketing_spend",
    metric_column: str = "spend",
    date_column: str = "date",
    lookback_days: int = 30,
):
    service = AnalysisService(get_connector())
    try:
        envelope = service.detect_anomalies(table, metric_column, date_column, lookback_days)
        return {"title": envelope.title, "body": envelope.body}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/reports/growth-summary")
def growth_summary(spend_table: str = "marketing_spend", events_table: str = "user_events"):
    service = ReportingService(get_connector())
    try:
        envelope = service.growth_summary(spend_table, events_table)
        return {"title": envelope.title, "body": envelope.body}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


def main():
    import uvicorn
    uvicorn.run("growth_os.api.server:app", host="0.0.0.0", port=8000, reload=False)
