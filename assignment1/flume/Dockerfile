FROM docker-hadoop-hadoop:latest AS hadoopsrc

FROM eclipse-temurin:8-jre-jammy

ENV FLUME_VERSION=1.11.0
ENV FLUME_HOME=/opt/flume
ENV PATH=$FLUME_HOME/bin:$PATH

RUN sed -i 's|http://archive.ubuntu.com/ubuntu|http://mirrors.aliyun.com/ubuntu|g; s|http://security.ubuntu.com/ubuntu|http://mirrors.aliyun.com/ubuntu|g' /etc/apt/sources.list \
    && apt-get -o Acquire::Retries=5 update \
    && apt-get install -y --no-install-recommends curl ca-certificates bash procps tini \
    && rm -rf /var/lib/apt/lists/* \
    && curl --retry 5 --retry-delay 3 -fsSL https://archive.apache.org/dist/flume/${FLUME_VERSION}/apache-flume-${FLUME_VERSION}-bin.tar.gz -o /tmp/flume.tgz \
    && tar -xzf /tmp/flume.tgz -C /opt \
    && mv /opt/apache-flume-${FLUME_VERSION}-bin ${FLUME_HOME} \
    && rm /tmp/flume.tgz \
    && mkdir -p /data/spool/hdfs /data/spool/kafka /data/flume-hdfs/checkpoint /data/flume-hdfs/data /data/flume-kafka

COPY --from=hadoopsrc /home/hduser/hadoop-3.3.3/share/hadoop/common /opt/hadoop/share/hadoop/common
COPY --from=hadoopsrc /home/hduser/hadoop-3.3.3/share/hadoop/hdfs /opt/hadoop/share/hadoop/hdfs
RUN cp /opt/hadoop/share/hadoop/common/*.jar ${FLUME_HOME}/lib/ \
    && cp /opt/hadoop/share/hadoop/common/lib/*.jar ${FLUME_HOME}/lib/ \
    && cp /opt/hadoop/share/hadoop/hdfs/*.jar ${FLUME_HOME}/lib/ \
    && cp /opt/hadoop/share/hadoop/hdfs/lib/*.jar ${FLUME_HOME}/lib/

COPY start-flume.sh /usr/local/bin/start-flume.sh
RUN chmod +x /usr/local/bin/start-flume.sh

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/start-flume.sh"]
