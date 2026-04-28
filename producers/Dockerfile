FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common ./common
COPY domains ./domains
COPY configs ./configs
COPY main.py .

CMD ["python", "main.py"]