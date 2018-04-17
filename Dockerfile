FROM ubuntu:16.04

MAINTAINER Mohamed Nadjib Mami <mami@cs.uni-bonn.de>

ENV HADOOP_URL http://mirror.synyx.de/apache/hadoop/common/hadoop-2.8.3/hadoop-2.8.3.tar.gz
ENV HADOOP_VERSION 2.8.3

RUN set -x && \
    # Install Java 8
    apt-get update --fix-missing && \
    apt-get install -y --no-install-recommends curl vim openjdk-8-jdk-headless apt-transport-https && \
    # install SBT
    echo "deb https://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list && \
    apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823 && \
    apt-get update && \
    apt-get install -y sbt && \
    # cleanup
    apt-get clean

# Update environment
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/

RUN set -x && \
    # Install Hadoop
    curl -fSL -o - "$HADOOP_URL" | tar xz -C /opt/ && \
    ln -s /opt/hadoop-$HADOOP_VERSION/etc/hadoop /etc/hadoop && \
    cp /etc/hadoop/mapred-site.xml.template /etc/hadoop/mapred-site.xml && \
    mkdir /opt/hadoop-$HADOOP_VERSION/logs

# Update environment
ENV HADOOP_PREFIX=/opt/hadoop-$HADOOP_VERSION \
    HADOOP_CONF_DIR=/etc/hadoop

RUN set -x && \
    # Install MongoDB
    apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2930ADAE8CAF5059EE73BB4B58712A2291FA4AD5 && \
    echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu xenial/mongodb-org/3.6 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-3.6.list && \
    apt-get update && \
    apt-get install -y mongodb-org && \
    mkdir -p /data/db
    #mongod

# EXPOSE 27017

CMD ["mongod", "-DFOREGROUND"]

RUN set -x && \
    # Install Cassandra
    echo "deb http://www.apache.org/dist/cassandra/debian 311x main" | tee -a /etc/apt/sources.list.d/cassandra.sources.list && \
    curl https://www.apache.org/dist/cassandra/KEYS | apt-key add - && \
    apt-get update && \
    apt-key adv --keyserver pool.sks-keyservers.net --recv-key A278B781FE4B2BDA && \
    apt-get -y install cassandra

CMD ["cassandra","-R", "-DFOREGROUND"]
