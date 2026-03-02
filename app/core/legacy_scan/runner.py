"""
Módulo responsável por coordenar a execução do motor de varredura (engine)
e a persistência dos resultados no banco de dados do Projeto 1,
utilizando os modelos ZombieScanJob e ZombieVmdkRecord.
"""

import uuid
from datetime import datetime, timezone
import logging

from sqlalchemy.ext.asyncio import AsyncSession
from app.models.zombie_scan import ZombieScanJob, ZombieVmdkRecord
from app.core.legacy_scan.engine import run_orphan_scan_engine

logger = logging.getLogger(__name__)

async def run_official_orphan_scan(session: AsyncSession, vcenter_ids: list[int] = None) -> None:
    """
    Coordena a execução do motor de varredura do Projeto 2 e salva
    os resultados no banco de dados do Projeto 1.
    """
    if vcenter_ids is None:
        vcenter_ids = []

    # 1. Cria o registro do Job
    job_id = str(uuid.uuid4())
    job = ZombieScanJob(
        job_id=job_id,
        vcenter_ids=vcenter_ids,
        status="running",
        started_at=datetime.now(timezone.utc)
    )
    session.add(job)
    await session.flush() # Adiciona o job sem commitar definitivamente, adquirindo um tempo no db
    
    # Prepara totalizadores
    total_vmdks = 0
    total_size_gb = 0.0

    try:
        # 2. Executa o motor (Projeto 2)
        # O motor pode demorar. Ele retorna uma lista de dicionários com os órfãos encontrados.
        results = await run_orphan_scan_engine(vcenter_ids=vcenter_ids)
        
        # 3. Processa e grava os resultados encontrados
        for item in results:
            record = ZombieVmdkRecord(
                job_id=job_id,
                path=item.get("path", ""),
                datastore=item.get("datastore", ""),
                folder=item.get("folder", ""),
                datastore_type=item.get("datastore_type", ""),
                tamanho_gb=item.get("tamanho_gb"),
                ultima_modificacao=item.get("ultima_modificacao"),
                tipo_zombie=item.get("tipo_zombie", "ORPHANED"),
                vcenter_host=item.get("vcenter_host", ""),
                vcenter_name=item.get("vcenter_name", ""),
                datacenter=item.get("datacenter", ""),
                detection_rules=item.get("detection_rules", []),
                likely_causes=item.get("likely_causes", []),
                confidence_score=item.get("confidence_score", 0),
                datacenter_path=item.get("datacenter_path", ""),
                datastore_name=item.get("datastore_name", ""),
                vmdk_folder=item.get("vmdk_folder", ""),
                vmdk_filename=item.get("vmdk_filename", ""),
            )
            session.add(record)
            
            total_vmdks += 1
            if item.get("tamanho_gb"):
                total_size_gb += item.get("tamanho_gb")

        # 4. Atualiza estado final do Job em sucesso
        job.status = "completed"
        job.total_vmdks = total_vmdks
        job.total_size_gb = total_size_gb

    except Exception as e:
        # Atualiza estado final do Job em caso de erro
        logger.error(f"Erro na varredura engine {job_id}: {e}", exc_info=True)
        job.status = "failed"
        job.error_messages = [str(e)]
    finally:
        job.finished_at = datetime.now(timezone.utc)
        # 5. Efetua o commit final após terminar tudo (sucesso ou falha tratada e gravada no status do job)
        try:
            await session.commit()
        except BaseException as e:
            logger.error(f"Erro efetuando o commit do job {job_id}", exc_info=True)
            await session.rollback()
            raise
