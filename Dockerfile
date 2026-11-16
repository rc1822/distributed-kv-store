FROM python:3.12-slim
WORKDIR /app
COPY . .
CMD ["python", "primary_server.py"]
