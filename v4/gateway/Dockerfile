FROM python:3.11-slim


WORKDIR /app

# Устанавливаем системные зависимости для psycopg2
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Копируем зависимости Python и устанавливаем
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt



# Копируем код
COPY . .

EXPOSE 8080
CMD ["python", "app.py"]