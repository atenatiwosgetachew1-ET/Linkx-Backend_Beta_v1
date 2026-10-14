file_path = '/var/www/linkx-backend/service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py'

with open(file_path, 'r') as f:
    content = f.read()

# Fix all the messy inline imports
content = content.replace("__import__('datetime').datetime.now()", "datetime.now()")
content = content.replace('__import__("datetime").datetime.now()', "datetime.now()")
content = content.replace(r"__import__(\'datetime\').datetime.now()", "datetime.now()")
content = content.replace(r"__import__('datetime').datetime.now()", "datetime.now()")

# Also, there's a literal backslash version
content = content.replace("__import__(\\'datetime\\').datetime.now()", "datetime.now()")

with open(file_path, 'w') as f:
    f.write(content)

print("Syntax fixed.")
