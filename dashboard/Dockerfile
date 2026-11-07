# The dashboard: Flask + gunicorn. The code is not copied in: compose mounts the repository read-only at
# /opt/project, so the dashboard always serves the current templates and the latest export in data/serving/.
FROM python:3.10-slim

COPY requirements/dashboard.txt /tmp/dashboard.txt
RUN pip install --no-cache-dir -r /tmp/dashboard.txt && rm /tmp/dashboard.txt

ENV PYTHONPATH=/opt/project/src:/opt/project/dashboard \
    DATA_DIR=/opt/project/data/serving \
    GUIDE_PATH=/opt/project/docs/dashboard-guide.md \
    PYTHONUNBUFFERED=1
WORKDIR /opt/project
EXPOSE 5000
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--workers", "2", "--access-logfile", "-", "app:create_app()"]
