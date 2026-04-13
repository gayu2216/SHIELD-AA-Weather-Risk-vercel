"""Start Forecast API + static UI (forecast windows for scheduling)."""

import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "shield_pipeline.web.app:app",
        host="0.0.0.0",
        port=8765,
        reload=False,
    )
