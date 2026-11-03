FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY wfengine ./wfengine
COPY business ./business
RUN useradd -r -u 10001 app && mkdir -p /data && chown app /data
USER app
ENV DATA_DIR=/data
# The same image runs either role; compose overrides the command.
CMD ["python", "-m", "wfengine.worker"]
