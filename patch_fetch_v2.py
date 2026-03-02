import os
import re

directory = 'web/static/js/'
for f in os.listdir(directory):
    if f.endswith('.js'):
        path = os.path.join(directory, f)
        with open(path, 'r', encoding='utf-8') as file:
            c = file.read()
        
        # We want to inject inside `headers: {` safely, but only if we haven't already
        # Find all `headers: {` and replace with `headers: { "X-API-Key": window.ZH_API_KEY || "TROQUE_ESTA_API_KEY", `
        # First, let's remove the previous injections to avoid duplicates
        c = c.replace('"X-API-Key": window.ZH_API_KEY || "TROQUE_ESTA_API_KEY", ', '')
        c = c.replace(', "X-API-Key": window.ZH_API_KEY || "TROQUE_ESTA_API_KEY"', '')
        c = c.replace('"X-API-Key": window.ZH_API_KEY || "TROQUE_ESTA_API_KEY"', '')
        
        # Now do the global injection
        c = re.sub(r'headers:\s*\{', r'headers: { "X-API-Key": window.ZH_API_KEY || "TROQUE_ESTA_API_KEY", ', c)
        
        with open(path, 'w', encoding='utf-8') as file:
            file.write(c)

print("Fetch patching done v2.")
