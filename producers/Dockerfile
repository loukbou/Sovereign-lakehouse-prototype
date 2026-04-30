FROM python:3.11-slim

WORKDIR /app

COPY producers/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producers/common ./common
COPY producers/domains ./domains
COPY producers/configs ./configs
COPY producers/main.py .
COPY schemas ./schemas

CMD ["python", "main.py"]