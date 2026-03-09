import asyncio
from app.models.base import AsyncSessionLocal
from app.models.zombie_scan import ZombieScanJob, ZombieVmdkRecord

async def test():
    async with AsyncSessionLocal() as db:
        res = await db.execute(select(ZombieScanJob.job_id, ZombieScanJob.vcenter_ids, ZombieScanJob.finished_at).order_by(ZombieScanJob.finished_at.desc()))
        for row in res.all():
            job_id, vc_ids, finished = row
            print(f"Job: {job_id}, VCs: {vc_ids}, Type: {type(vc_ids)}, Finished: {finished}")

        # Check total records grouped by job_id
        res2 = await db.execute(select(ZombieVmdkRecord.job_id, func.count()).group_by(ZombieVmdkRecord.job_id))
        for row in res2.all():
            print(f"Record Count for Job {row[0]}: {row[1]}")

        # Check records for latest job Datastores
        res3 = await db.execute(select(ZombieVmdkRecord.datastore, func.count(), func.sum(ZombieVmdkRecord.tamanho_gb)).group_by(ZombieVmdkRecord.datastore))
        for row in res3.all():
            print(f"Datastore: {row[0]}, Count: {row[1]}, Total GB: {row[2]}")

asyncio.run(test())
