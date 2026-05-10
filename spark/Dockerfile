FROM apache/spark:3.5.0

USER root

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
	&& mkdir -p /home/spark/.ivy2/cache /home/spark/.ivy2/jars \
	&& chown -R spark:spark /home/spark

ENV HOME=/home/spark

USER spark
