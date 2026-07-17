FROM python:3.12-slim

WORKDIR /app

# Dépendances système pour mysql-connector et ssh
RUN apt-get update && apt-get install -y --no-install-recommends \
    openssh-client \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Dépendances Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Code source
COPY main.py .
COPY extract_sofia.py .
COPY extract_si_emploi.py .

# Projet dbt (modèles, macros, seeds, packages)
COPY dbt/ ./dbt/

# Pas de fichier .env embarqué — les secrets viennent de Secret Manager
ENV PYTHONUNBUFFERED=1
ENV DBT_PROFILES_DIR=/app/dbt

CMD ["python", "main.py"]
