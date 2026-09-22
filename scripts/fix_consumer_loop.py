import re

def fix_consumer_loop(file_path):
    with open(file_path, "r") as f:
        content = f.read()

    # Replace the error handling in the consumer loop to fully restart the daemon
    old_except = """
        except Exception as e:
            print(f"[xVigilance-Consumer] ERROR in consumer loop: {e}", flush=True)
            import traceback
            traceback.print_exc()
            if not RUNNING:
                break
            time.sleep(0.5)
"""
    new_except = """
        except Exception as e:
            print(f"[xVigilance-Consumer] FATAL KAFKA ERROR in consumer loop: {e}", flush=True)
            import traceback
            traceback.print_exc()
            print("[xVigilance-Consumer] Tearing down Kafka connection and rebooting consumer from scratch...", flush=True)
            try:
                c.close()
            except:
                pass
            return  # The immortal wrapper in __main__ will reboot it!
"""
    
    content = content.replace(old_except.strip(), new_except.strip())

    with open(file_path, "w") as f:
        f.write(content)

fix_consumer_loop("/var/www/linkx-backend/service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py")
print("Done")
