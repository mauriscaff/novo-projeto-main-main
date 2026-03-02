import urllib.request
import json

req = urllib.request.Request(
    'http://127.0.0.1:8000/api/v1/scan/start', 
    data=json.dumps({"vcenter_ids":[1],"datacenters":[]}).encode('utf-8'),
    method='POST', 
    headers={'X-API-Key': 'TROQUE_ESTA_API_KEY', 'Content-Type':'application/json'}
)
try:
    with urllib.request.urlopen(req) as response:
        print("SUCCESS:", response.read().decode())
except Exception as e:
    print("ERROR:", e)
    if hasattr(e, 'read'):
        print(e.read().decode())
