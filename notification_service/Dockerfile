FROM python:3.9-buster

RUN apt-get update && apt-get install -y sqlite3 libsqlite3-dev

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "notifier.py"]
