import os
import re

directory = 'web/static/js/'
for f in os.listdir(directory):
    if f.endswith('.js'):
        path = os.path.join(directory, f)
        with open(path, 'r', encoding='utf-8') as file:
            c = file.read()
        
        # Replace 1: { headers: { Accept: "application/json" } } -> { headers: { Accept: "application/json", "X-API-Key": window.ZH_API_KEY || "TROQUE_ESTA_API_KEY" } }
        c = re.sub(
            r'headers:\s*\{\s*"?Accept"?:\s*"application/json"\s*\}',
            r'headers: { "Accept": "application/json", "X-API-Key": window.ZH_API_KEY || "TROQUE_ESTA_API_KEY" }',
            c
        )
        
        # Replace 2: headers: { "Content-Type": "application/json" }
        c = re.sub(
            r'headers:\s*\{\s*"Content-Type":\s*"application/json"\s*\}',
            r'headers: { "Content-Type": "application/json", "X-API-Key": window.ZH_API_KEY || "TROQUE_ESTA_API_KEY" }',
            c
        )
        
        # Replace 3: fetch(..., { method: "DELETE" }) -> fetch(..., { method: "DELETE", headers: { "X-API-Key": window.ZH_API_KEY || "TROQUE_ESTA_API_KEY" } })
        c = re.sub(
            r'\{\s*method:\s*"DELETE"\s*\}',
            r'{ method: "DELETE", headers: { "X-API-Key": window.ZH_API_KEY || "TROQUE_ESTA_API_KEY" } }',
            c
        )
        
        with open(path, 'w', encoding='utf-8') as file:
            file.write(c)

print("Fetch patching done.")
