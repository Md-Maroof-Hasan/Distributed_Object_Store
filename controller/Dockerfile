FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common ./common
COPY controller ./controller
ENV PYTHONUNBUFFERED=1
CMD ["python", "-m", "controller.main"]
