"""FastAPI app: HTML dashboard and JSON metrics for the fact-table consumer."""

from __future__ import annotations

import logging
import threading
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from starlette.templating import Jinja2Templates

from consumer_dashboard.config import Settings, get_settings
from consumer_dashboard.kafka_worker import start_consumer_thread
from consumer_dashboard.state import MetricsStore

logger = logging.getLogger(__name__)

_templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent / "templates"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    stop = threading.Event()
    store = MetricsStore()
    app.state.metrics_store = store
    app.state.settings = settings
    app.state.consumer_stop = stop
    app.state.consumer_thread = None

    if settings.consumer_enabled():
        app.state.consumer_thread = start_consumer_thread(settings, store, stop)
    else:
        logger.warning(
            "Consumer not started: set KAFKA_BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL, "
            "KAFKA_SASL_USERNAME, and KAFKA_SASL_PASSWORD (see .env.example)"
        )

    yield

    stop.set()
    t = getattr(app.state, "consumer_thread", None)
    if t is not None and t.is_alive():
        t.join(timeout=8.0)


def create_app() -> FastAPI:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    return FastAPI(title="fct_nb_act_per_pgm consumer dashboard", lifespan=lifespan)


app = create_app()


@app.get("/healthz")
def healthz() -> dict[str, bool]:
    return {"ok": True}


@app.get("/api/metrics")
def api_metrics(request: Request) -> JSONResponse:
    store: MetricsStore = request.app.state.metrics_store
    settings: Settings = request.app.state.settings
    data = store.snapshot_metrics()
    data["consumer_configured"] = settings.consumer_enabled()
    data["kafka_topic"] = settings.kafka_topic
    return JSONResponse(data)


@app.get("/", response_class=HTMLResponse)
def index(request: Request) -> HTMLResponse:
    settings: Settings = request.app.state.settings
    return _templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context={"topic": settings.kafka_topic},
    )


def run() -> None:
    import uvicorn

    uvicorn.run(
        "consumer_dashboard.main:app",
        host="0.0.0.0",
        port=8080,
        factory=False,
    )
