# serve_published_genomes.py
from pathlib import Path
import os
from starlette.applications import Starlette
from starlette.staticfiles import StaticFiles
from starlette.middleware.cors import CORSMiddleware
import uvicorn

GENOME_DATA_DIR = Path("/data/genome_cache/publish").resolve()
HOST = os.environ.get("GENOME_DATA_HOST", "0.0.0.0")
PORT = int(os.environ.get("GENOME_DATA_PORT", "8765"))

def create_app() -> Starlette:
    app = Starlette()
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # tighten later if desired
        allow_methods=["GET", "HEAD", "OPTIONS"],
        allow_headers=["*"],
    )
    app.mount("/", StaticFiles(directory=str(GENOME_DATA_DIR), html=False), name="genomes")
    return app

if __name__ == "__main__":
    print(f"Serving published genomes from {GENOME_DATA_DIR} at http://{HOST}:{PORT}/")
    uvicorn.run(create_app(), host=HOST, port=PORT, log_level="info")
