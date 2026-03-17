import logging
from pathlib import Path
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from dotenv import set_key

from app.dependencies import get_current_user
from config import get_settings

router = APIRouter()
logger = logging.getLogger(__name__)

class ReadonlyModeUpdate(BaseModel):
    readonly_mode: bool

@router.post("/readonly_mode", summary="Toggle READONLY_MODE")
async def update_readonly_mode(
    payload: ReadonlyModeUpdate,
    _: dict = Depends(get_current_user)
):
    settings = get_settings()
    settings.readonly_mode = payload.readonly_mode
    
    env_file = Path(".env")
    if env_file.exists():
        new_val = "true" if payload.readonly_mode else "false"
        set_key(str(env_file), "READONLY_MODE", new_val)
        logger.info(f"READONLY_MODE actualized to {new_val} in .env file")
        
    return {
        "message": "READONLY_MODE updated successfully", 
        "readonly_mode": settings.readonly_mode
    }
