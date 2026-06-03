FROM eclipse-temurin:8-jdk-jammy

RUN apt-get update -y \
    && export DEBIAN_FRONTEND=noninteractive \
    && apt-get install -y --no-install-recommends sudo curl ssh python3 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -m hduser \
    && echo "hduser:supergroup" | chpasswd \
    && adduser hduser sudo \
    && echo "hduser ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers \
    && ln -s /usr/bin/python3 /usr/bin/python

COPY ssh_config /etc/ssh/ssh_config

WORKDIR /home/hduser
USER hduser

RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa \
    && cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys \
    && chmod 0600 ~/.ssh/authorized_keys

ENV HADOOP_VERSION=3.3.3
ENV HADOOP_HOME=/home/hduser/hadoop-${HADOOP_VERSION}

RUN curl -sL --retry 3 \
    "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
    | tar -xz -C /home/hduser/ \
    && rm -rf ${HADOOP_HOME}/share/doc

ENV HDFS_NAMENODE_USER=hduser
ENV HDFS_DATANODE_USER=hduser
ENV HDFS_SECONDARYNAMENODE_USER=hduser
ENV YARN_RESOURCEMANAGER_USER=hduser
ENV YARN_NODEMANAGER_USER=hduser
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
ENV YARNSTART=0

COPY core-site.xml ${HADOOP_HOME}/etc/hadoop/
COPY hdfs-site.xml ${HADOOP_HOME}/etc/hadoop/
COPY yarn-site.xml ${HADOOP_HOME}/etc/hadoop/
COPY docker-entrypoint.sh ${HADOOP_HOME}/etc/hadoop/

RUN echo "export JAVA_HOME=/opt/java/openjdk/" >> ${HADOOP_HOME}/etc/hadoop/hadoop-env.sh

WORKDIR /usr/local/bin
RUN sudo ln -s ${HADOOP_HOME}/etc/hadoop/docker-entrypoint.sh .

WORKDIR /home/hduser

EXPOSE 9000 9864 9870 8088 22

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
