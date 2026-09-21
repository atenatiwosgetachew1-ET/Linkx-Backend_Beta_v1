import os

bind = f"0.0.0.0:{os.getenv('PORT', '8100')}"
workers = int(os.getenv("GUNICORN_WORKERS", "4"))
worker_class = "gthread"
threads = int(os.getenv("GUNICORN_THREADS", "10"))
timeout = 120
keepalive = 5
accesslog = "-"
errorlog = "-"
loglevel = "info"
