FROM eclipse-temurin:8-jre-jammy

ENV HBASE_VERSION=2.4.17
ENV HBASE_HOME=/opt/hbase
ENV PATH=$HBASE_HOME/bin:$PATH

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl bash procps python3 python3-pip tini net-tools \
    && rm -rf /var/lib/apt/lists/* \
    && bash -lc 'set -e; \
      primary="https://dlcdn.apache.org/hbase/${HBASE_VERSION}/hbase-${HBASE_VERSION}-bin.tar.gz"; \
      fallback="https://archive.apache.org/dist/hbase/${HBASE_VERSION}/hbase-${HBASE_VERSION}-bin.tar.gz"; \
      echo "Trying $primary"; \
      if ! curl -fL "$primary" -o /tmp/hbase.tgz; then \
        echo "Primary mirror unavailable, fallback to archive"; \
        for i in 1 2 3 4 5; do \
          echo "Archive attempt $i"; \
          if curl --retry 3 --retry-all-errors --retry-delay 5 -fL "$fallback" -o /tmp/hbase.tgz; then \
            exit 0; \
          fi; \
          sleep 3; \
        done; \
        echo "Failed to download from archive mirror" >&2; \
        exit 1; \
      fi; \
      exit 0; \
      echo "Failed to download HBase binary from all mirrors" >&2; \
      exit 1' \
    && tar -xzf /tmp/hbase.tgz -C /opt \
    && mv /opt/hbase-${HBASE_VERSION} ${HBASE_HOME} \
    && rm /tmp/hbase.tgz

COPY hbase-site.xml ${HBASE_HOME}/conf/hbase-site.xml
COPY start-hbase.sh /usr/local/bin/start-hbase.sh

RUN chmod +x /usr/local/bin/start-hbase.sh

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/start-hbase.sh"]
