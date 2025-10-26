FROM python:3.10-slim

WORKDIR /app

COPY ./docker/requirements.txt .
COPY ./src /app/src
RUN ls
RUN pip install --default-timeout=100 --upgrade pip
RUN pip install --default-timeout=100 --no-cache-dir -r requirements.txt

ENTRYPOINT ["python", "-m", "src.main_g2"]