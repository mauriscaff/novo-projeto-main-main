import asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from app.api.routes.dashboard import get_recoverable_storage
from app.models.base import AsyncSessionLocal
import json
from datetime import datetime

class DatetimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if hasattr(obj, 'model_dump'):
            return obj.model_dump()
        return super().default(obj)

async def run_test():
    try:
        async with AsyncSessionLocal() as db:
            result = await get_recoverable_storage(db=db, _={})
            print(json.dumps(result, cls=DatetimeEncoder, indent=2))
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(run_test())
