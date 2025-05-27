# Usa una imagen base oficial de Python
FROM python:3.11-slim

# Variables de entorno para que apt no pregunte nada
ENV DEBIAN_FRONTEND=noninteractive

# Instala dependencias necesarias para pyodbc y el driver ODBC
RUN apt-get update && apt-get install -y \
    curl \
    gnupg2 \
    unixodbc-dev \
    apt-transport-https \
    software-properties-common \
    && rm -rf /var/lib/apt/lists/*

# Agrega repositorio de Microsoft para ODBC Driver 18
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*

# Copia requirements.txt y luego instala dependencias Python
COPY requirements.txt /app/requirements.txt
WORKDIR /app
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Copia el código fuente
COPY . /app

# Expone el puerto 3000 (o el que uses)
EXPOSE 3000

# Comando para arrancar la app
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "3000"]
