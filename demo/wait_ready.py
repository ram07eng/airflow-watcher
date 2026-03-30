#!/usr/bin/env python3
"""Wait for Airflow webservers to be ready."""
import urllib.request
import time

ports = [8080, 8081, 8082]
for port in ports:
    for attempt in range(60):
        try:
            urllib.request.urlopen(f"http://localhost:{port}/health", timeout=3)
            print(f"  :{port} ready")
            break
        except Exception:
            if attempt % 5 == 0:
                print(f"  :{port} not ready (attempt {attempt+1})...")
            time.sleep(3)
    else:
        print(f"  :{port} TIMEOUT after 60 attempts")

print("Done waiting.")
