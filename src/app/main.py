from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

from src.app.web.routes import router

app = FastAPI(title="Daily Brief Intel BI")

static_dir = Path(__file__).resolve().parent / "static"
static_dir.mkdir(parents=True, exist_ok=True)
app.mount("/static", StaticFiles(directory=static_dir), name="static")

app.include_router(router)


@app.get("/", include_in_schema=False)
def index():
    return RedirectResponse(url="/daily")
