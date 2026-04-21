"""Run Kronos PoC API locally."""

import uvicorn

from tools.kronos_poc.config import SERVICE_HOST, SERVICE_PORT


if __name__ == "__main__":
    uvicorn.run("tools.kronos_poc.api:app", host=SERVICE_HOST, port=SERVICE_PORT, reload=False)
