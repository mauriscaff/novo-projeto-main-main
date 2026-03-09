import asyncio
from sqlalchemy import select
from app.models.base import AsyncSessionLocal
from app.models.zombie_scan import ZombieScanJob
import json

async def main():
    async with AsyncSessionLocal() as db:
        res = await db.execute(select(ZombieScanJob).order_by(ZombieScanJob.created_at.desc()).limit(1))
        job = res.scalar_one_or_none()
        if job:
            print(f"Status: {job.status}")
            print(f"Errors: {job.error_messages}")
            print(f"Start: {job.started_at}, Finish: {job.finished_at}")
        else:
            print("No jobs found.")

if __name__ == "__main__":
    asyncio.run(main())
