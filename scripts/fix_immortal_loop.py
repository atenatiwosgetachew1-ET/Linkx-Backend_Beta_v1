import re

def fix_immortal_loop(file_path):
    with open(file_path, "r") as f:
        content = f.read()

    old_block = """
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
    new_block = """
if __name__ == "__main__":
    import time
    while RUNNING:
        try:
            main()
        except Exception as e:
            print(f"[xVigilance-Consumer] FATAL CRASH CAUGHT: {e}", flush=True)
            import traceback
            traceback.print_exc()
            if RUNNING:
                print("[xVigilance-Consumer] RESTARTING IN 5 SECONDS...", flush=True)
                time.sleep(5)
    print("[xVigilance-Consumer] Daemon successfully exited.", flush=True)
"""
    
    content = content.replace(old_block.strip(), new_block.strip())

    with open(file_path, "w") as f:
        f.write(content)

fix_immortal_loop("/var/www/linkx-backend/service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py")
print("Done")
