FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends default-jdk curl && rm -rf /var/lib/apt/lists/*

COPY src/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./

ENV PORT=8100
EXPOSE 8100

CMD ["python", "-u", "main.py"]
