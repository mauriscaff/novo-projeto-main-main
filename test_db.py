import sqlite3, json

conn = sqlite3.connect("vmdk_scanner.db")
c = conn.cursor()

with open("db_report2.txt", "w", encoding="utf-8") as f:
    # Emulate exactly what get_recoverable_storage does
    c.execute("""
        SELECT job_id, vcenter_ids, finished_at
        FROM zombie_scan_jobs
        WHERE status = 'completed'
        ORDER BY finished_at DESC
    """)
    all_jobs = c.fetchall()

    f.write("=== ALL COMPLETED JOBS ===\n")
    for j in all_jobs:
        f.write(f"  job={j[0]}, vc_ids={j[1]} (type={type(j[1]).__name__}), finished={j[2]}\n")

    # Emulate the mapping logic (WITH json.loads fix)
    vcenter_latest_job = {}
    for job_id, vc_ids, finished_at in all_jobs:
        if isinstance(vc_ids, str):
            try:
                vc_ids = json.loads(vc_ids)
            except:
                vc_ids = []
        if isinstance(vc_ids, list):
            for vid in vc_ids:
                if vid not in vcenter_latest_job:
                    vcenter_latest_job[vid] = job_id

    f.write(f"\n=== VCENTER -> LATEST JOB MAPPING ===\n")
    for vid, jid in vcenter_latest_job.items():
        f.write(f"  vcenter_id={vid} (type={type(vid).__name__}) -> job={jid}\n")

    target_job_ids = list(set(vcenter_latest_job.values()))
    f.write(f"\n=== TARGET JOB IDS ({len(target_job_ids)} jobs) ===\n")
    for jid in target_job_ids:
        f.write(f"  {jid}\n")

    # Now check what records these target_job_ids produce
    placeholders = ",".join(["?" for _ in target_job_ids])
    c.execute(f"""
        SELECT datastore, COUNT(*), ROUND(SUM(tamanho_gb), 2)
        FROM zombie_vmdk_records
        WHERE job_id IN ({placeholders})
        GROUP BY datastore
        ORDER BY SUM(tamanho_gb) DESC
    """, target_job_ids)
    f.write(f"\n=== RECORDS FOR TARGET JOBS ===\n")
    total = 0.0
    for r in c.fetchall():
        f.write(f"  ds={r[0]}, count={r[1]}, total_gb={r[2]}\n")
        total += r[2]
    f.write(f"  TOTAL: {total:.2f} GB = {total/1024:.2f} TB\n")

conn.close()
print("Report written to db_report2.txt")
