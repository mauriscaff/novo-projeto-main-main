import re

filepath = 'c:/Users/mscaff/Downloads/novo-projeto-main/novo-projeto-main/app/core/scanner/zombie_detector.py'

with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Update _normalize
old_normalize = """def _normalize(path: str) -> str:
    \"\"\"Normaliza caminhos para comparação case-insensitive com barras uniformes.\"\"\"
    return path.strip().lower().replace("\\\\", "/")"""

new_normalize = """def _normalize(path: str) -> str:
    \"\"\"Normaliza caminhos para comparação case-insensitive com barras uniformes e sem espaços múltiplos.\"\"\"
    norm = path.strip().lower().replace("\\\\", "/")
    return re.sub(r'\\s+', ' ', norm)"""

content = content.replace(old_normalize, new_normalize)

# 2. Update _classify_vmdk. It's a huge function from def _classify_vmdk to the end of its body.
# We will use regex to find the definition of _classify_vmdk and replace it up to the next definition.

classify_match = re.search(r'def _classify_vmdk\(.*?\)\s*-> [^{]*:', content, re.DOTALL)
if classify_match:
    start_idx = classify_match.start()
    
    # Find the next 'def ' strictly at the zero indentation level
    next_def_match = re.search(r'\n^def ', content[start_idx+1:], re.MULTILINE)
    if next_def_match:
        end_idx = start_idx + 1 + next_def_match.start()
    else:
        end_idx = len(content) # End of file if no other def
        
    old_classify = content[start_idx:end_idx]
    
    new_classify = """def _classify_vmdk(
    entry: _FileEntry,
    inventory: _InventorySnapshot,
    shared_datastores: set[str],
    folder_files: dict[str, set[str]],
    global_files: set[str],
    datacenter_name: str,
    datastore_name: str,
    ds_type: str,
    orphan_days: int,
    stale_snapshot_days: int,
    min_file_size_mb: int,
) -> ZombieVmdkResult | None:
    # READ-ONLY: no write operations

    if entry.is_vmx:
        return None

    name_lower = entry.name.lower()
    
    # FALSOS POSITIVOS — EXCLUIR SEMPRE DO SCAN:
    if (name_lower.endswith("-flat.vmdk") or 
        name_lower.endswith("-delta.vmdk") or 
        name_lower.endswith("-sesparse.vmdk") or 
        name_lower.endswith("-ctk.vmdk")):
        return None

    # Normalizar o caminho para comparação
    norm_path = _normalize(entry.full_path)
    
    # Comparar cada VMDK encontrado com os caminhos registrados
    if norm_path in inventory.vmdk_paths:
        return None  # Está em uso, não é órfão

    # Tipos e Motivos baseados nas regras SCAN-REGRAS-VMDK
    is_backup_artifact = "backup" in name_lower or "veeam" in name_lower or "pre-" in name_lower
    is_snapshot_leftover = "-000" in name_lower or "snap" in name_lower or "snapshot" in name_lower

    folder_norm = _normalize(entry.folder)
    folder_has_registered_vm = folder_norm in inventory.vm_folders

    # Inferência de motivo e tipo remapeado para banco de dados legado
    if not folder_has_registered_vm:
        reason = "VM removida do inventário mas arquivos não foram deletados"
        mapped_tipo = ZombieType.UNREGISTERED_DIR  # Mais próximo do legado para "pasta inteira órfã"
    elif is_backup_artifact:
        reason = "Possível artefato de backup"
        mapped_tipo = ZombieType.ORPHANED
    else:
        reason = "Arquivo VMDK sem VM associada no inventário"
        if is_snapshot_leftover:
            mapped_tipo = ZombieType.SNAPSHOT_ORPHAN
        else:
            mapped_tipo = ZombieType.ORPHANED

    # Construir e retornar ScanResult com os órfãos detectados
    return ZombieVmdkResult(
        path=entry.full_path,
        datastore=datastore_name,
        tamanho_gb=_bytes_to_gb(entry.size_bytes),
        ultima_modificacao=_utc(entry.modification),
        tipo_zombie=mapped_tipo,
        vcenter_host=inventory.vcenter_host,
        datacenter=datacenter_name,
        detection_rules=[
            "1. Obtido todas as VMs",
            "2. Montado conjunto de VMDKs em uso",
            "3. Discos de VMs orphaned incluídos",
            "4. Varridos datastores",
            "5. Comparação case-insensitive",
            "6. Falsos positivos excluídos"
        ],
        likely_causes=[reason],
        folder=entry.folder,
        datastore_type=ds_type,
        confidence_score=95,  # Score alto pois segue regra exata
    )
"""
    content = content[:start_idx] + new_classify + content[end_idx:]

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)

print("Patch applied.")
