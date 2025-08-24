"""FastAPI server for Bharat Resto MVP."""

import uvicorn
from pathlib import Path
import sys

# Add the app directory to the Python path
app_dir = Path(__file__).parent
sys.path.insert(0, str(app_dir.parent))

from app.api import app
from app.config import settings
from app.log import logger

def start_server(
    host: str = "0.0.0.0",
    port: int = 8000,
    reload: bool = False,
    workers: int = 1
):
    """Start the FastAPI server."""
    
    logger.info(
        "Starting Bharat Resto MVP server",
        host=host,
        port=port,
        reload=reload,
        workers=workers
    )
    
    # Ensure database is initialized
    from app import persist
    persist.db_manager.init_db()
    
    # Start server
    uvicorn.run(
        "app.api:app",
        host=host,
        port=port,
        reload=reload,
        workers=workers,
        log_level="info",
        access_log=True
    )

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Bharat Resto MVP FastAPI Server")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind to")
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload")
    parser.add_argument("--workers", type=int, default=1, help="Number of workers")
    
    args = parser.parse_args()
    
    start_server(
        host=args.host,
        port=args.port,
        reload=args.reload,
        workers=args.workers
    )