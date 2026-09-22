import re

def make_immortal(file_path):
    with open(file_path, "r") as f:
        content = f.read()

    immortal_block = """
if __name__ == "__main__":
    import time
    while True:
        try:
            main()
        except Exception as e:
            print(f"[xVigilance-Consumer] FATAL CRASH CAUGHT: {e}", flush=True)
            import traceback
            traceback.print_exc()
            print("[xVigilance-Consumer] RESTARTING IN 5 SECONDS...", flush=True)
            time.sleep(5)
"""
    
    content = re.sub(r'if __name__ == "__main__":\s+main\(\)\s*', immortal_block, content)

    with open(file_path, "w") as f:
        f.write(content)

make_immortal("/var/www/linkx-backend/service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py")
print("Done")
