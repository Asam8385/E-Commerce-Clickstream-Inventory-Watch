FROM bitnami/spark:3.4.1

USER 0

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

USER 1001
