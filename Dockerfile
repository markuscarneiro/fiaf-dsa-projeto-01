# Usa uma imagem base oficial Python que seja leve
FROM python:3.13-slim-bullseye

# Instala o 'cron', o agendador de tarefas do Linux
RUN apt-get update && apt-get -y install cron

# Cria o diretório que será usado para o volume de dados
RUN mkdir /data

# Define o diretório de trabalho dentro do contêiner
WORKDIR /app

# Copia o arquivo de requisitos para o diretório de trabalho
COPY requirements.txt .

# Instala as dependências Python, sem guardar cache para manter a imagem menor
RUN pip install --no-cache-dir -r requirements.txt

# Copia o script Python e o arquivo crontab para o diretório de trabalho
COPY dsaprojeto1_agendado.py .
COPY crontab .

# "Instala" o arquivo crontab para que o serviço cron o utilize
RUN crontab crontab

# Comando que será executado quando o contêiner iniciar.
# Inicia o serviço cron em modo "foreground" (-f), o que mantém o contêiner rodando.
CMD ["cron", "-f"]