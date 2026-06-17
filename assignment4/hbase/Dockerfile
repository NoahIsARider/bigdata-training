FROM eclipse-temurin:8-jre-jammy

ENV HBASE_VERSION=2.4.17
ENV HBASE_HOME=/opt/hbase
ENV PATH=$HBASE_HOME/bin:$PATH

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl bash procps python3 python3-pip python3-dev build-essential tini net-tools \
    && rm -rf /var/lib/apt/lists/* \
    && bash -lc 'set -e; \
      primary="https://dlcdn.apache.org/hbase/${HBASE_VERSION}/hbase-${HBASE_VERSION}-bin.tar.gz"; \
      fallback="https://archive.apache.org/dist/hbase/${HBASE_VERSION}/hbase-${HBASE_VERSION}-bin.tar.gz"; \
      if ! curl -fL "$primary" -o /tmp/hbase.tgz; then \
        for i in 1 2 3 4 5; do \
          if curl --retry 3 --retry-all-errors --retry-delay 5 -fL "$fallback" -o /tmp/hbase.tgz; then \
            exit 0; \
          fi; \
          sleep 3; \
        done; \
        exit 1; \
      fi' \
    && tar -xzf /tmp/hbase.tgz -C /opt \
    && mv /opt/hbase-${HBASE_VERSION} ${HBASE_HOME} \
    && rm /tmp/hbase.tgz \
    && pip3 install --no-cache-dir happybase thriftpy2

COPY hbase-site.xml ${HBASE_HOME}/conf/hbase-site.xml
COPY start-hbase.sh /usr/local/bin/start-hbase.sh

RUN sed -i 's/\r$//' /usr/local/bin/start-hbase.sh \
    && chmod +x /usr/local/bin/start-hbase.sh

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/start-hbase.sh"]
