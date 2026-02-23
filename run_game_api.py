"""Run card game API locally."""

import uvicorn


if __name__ == "__main__":
    uvicorn.run("game_api.main:app", host="0.0.0.0", port=8787, reload=False)

