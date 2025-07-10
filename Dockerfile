# Use uma imagem base do Python leve
FROM python:3.13-slim

# Defina o diretório de trabalho dentro do container
WORKDIR /app

# Copie o arquivo de dependências e instale-as
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copie o script principal
COPY main.py .

# Defina o comando que será executado quando o container iniciar
CMD ["python", "main.py"]