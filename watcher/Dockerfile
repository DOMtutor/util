FROM python:3.9
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt && rm requirements.txt

COPY watcher.py .

CMD ["python", "watcher.py"]
