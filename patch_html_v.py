import os
import re

directory = 'web/templates/'
count = 0
for d, _, fs in os.walk(directory):
    for f in fs:
        if f.endswith('.html'):
            path = os.path.join(d, f)
            with open(path, 'r', encoding='utf-8') as file:
                c = file.read()
            
            # Replace explicit ?v=3 or ?v=4
            c = c.replace('.js?v=3', '.js?v={{ api_version }}')
            c = c.replace('.js?v=4', '.js?v={{ api_version }}')
            
            # Replace missing versions
            c = re.sub(r'\.js"', r'.js?v={{ api_version }}"', c)
            
            # Clean up potential duplicates
            c = c.replace('?v={{ api_version }}?v={{ api_version }}', '?v={{ api_version }}')
            
            with open(path, 'w', encoding='utf-8') as file:
                file.write(c)
                count += 1

print(f"Patched {count} HTML templates.")
