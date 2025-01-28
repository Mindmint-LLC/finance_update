FROM python:3.12-slim

RUN apt-get update && apt-get install -y git

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY app /app
WORKDIR /app

CMD ["gunicorn", "-b", "0.0.0.0:8080", "app:app", "--timeout", "600"]