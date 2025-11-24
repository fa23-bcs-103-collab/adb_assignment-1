from fastapi import HTTPException, Header
import os

API_KEY = os.getenv("API_KEY", "dev-key-123")

async def verify_api_key(x_api_key: str = Header(...)):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return True