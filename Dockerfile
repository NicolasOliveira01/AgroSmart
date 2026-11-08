# Imagem única do AgroSmart, usada pelos dois serviços do docker-compose (ingestao e dbt).
# As credenciais NÃO entram na imagem: chegam pelo .env na hora de rodar.
FROM python:3.12-slim

# TZ: logs no horário de Brasília (o padrão do container é UTC)
# PYTHONUNBUFFERED: os logs aparecem na hora no "docker compose logs"
ENV TZ=America/Sao_Paulo \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN apt-get update \
 && apt-get install -y --no-install-recommends tzdata \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# dependências antes do código: enquanto o requirements.txt não mudar,
# o Docker reaproveita esta etapa e os próximos builds ficam rápidos
COPY requirements.txt .
RUN pip install --no-cache-dir --disable-pip-version-check -r requirements.txt

# usuário comum, sem privilégios de administrador (boa prática de segurança)
RUN useradd --create-home --uid 1000 agrosmart

COPY --chown=agrosmart:agrosmart 02_ingestao/ 02_ingestao/
COPY --chown=agrosmart:agrosmart 03_dbt/ 03_dbt/
COPY --chown=agrosmart:agrosmart docker/ docker/
RUN mkdir -p logs 02_ingestao/dados_landing \
 && chown agrosmart:agrosmart logs 02_ingestao/dados_landing

USER agrosmart

# padrão: o subscriber (o serviço do dbt troca este comando no docker-compose)
WORKDIR /app/02_ingestao
CMD ["python", "ingestao.py"]
