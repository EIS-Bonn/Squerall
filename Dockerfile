FROM ubuntu:16.04

MAINTAINER Mohamed Nadjib Mami <mami@cs.uni-bonn.de>

# Install necessary utility software
RUN set -x && \
    apt-get update --fix-missing && \
    apt-get install -y --no-install-recommends curl vim openjdk-8-jdk-headless apt-transport-https openssh-server openssh-client wget maven git python telnet wget unzip time && \
    # cleanup
    apt-get clean

# Update environment
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
ENV HADOOP_VERSION 2.9.2
ENV HADOOP_URL http://mirror.synyx.de/apache/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz

# Configure SSH
# COPY ssh_config /root/.ssh/config
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa \
    && cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys \
    && chmod 0600 ~/.ssh/authorized_keys

# Install Hadoop
ENV HADOOP_HOME /usr/local/hadoop
ENV HADOOP_CONF_DIR $HADOOP_HOME/etc/hadoop
ENV HDFS_PORT 9000

RUN set -x && \
    curl -fSL -o - "$HADOOP_URL" | tar xz -C /usr/local && \
    mv /usr/local/hadoop-$HADOOP_VERSION /usr/local/hadoop

# Configure Hadoop
RUN sed -i 's@\${JAVA_HOME}@'"$JAVA_HOME"'@g' $HADOOP_CONF_DIR/hadoop-env.sh
RUN sed -ri ':a;N;$!ba;s@(<configuration>).*(</configuration>)@\1<property><name>fs.default.name</name><value>hdfs://localhost:'"$HDFS_PORT"'</value></property>\2@g' $HADOOP_CONF_DIR/core-site.xml
RUN sed -ri ':a;N;$!ba;s@(<configuration>).*(</configuration>)@\1<property><name>dfs.replication</name><value>1</value></property>\2@g' $HADOOP_CONF_DIR/hdfs-site.xml

# Install Hive
ENV HIVE_VERSION 3.1.1
ENV HIVE_URL https://www-eu.apache.org/dist/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz
ENV HIVE_HOME /usr/local/hive

RUN set -x && \
    curl -fSL -o - "$HIVE_URL" | tar xz -C /usr/local && \
    mv /usr/local/apache-hive-$HIVE_VERSION-bin /usr/local/hive

RUN wget http://central.maven.org/maven2/mysql/mysql-connector-java/8.0.13/mysql-connector-java-8.0.13.jar && \
    mv mysql-connector-java-8.0.13.jar /usr/local/hive/lib

COPY evaluation/Hive_files/hive-site.xml $HIVE_HOME/conf/

# Install Presto (Server and CLI)
ENV PRESTO_VERSION 0.215
ENV PRESTO_URL     https://repo1.maven.org/maven2/com/facebook/presto/presto-server/${PRESTO_VERSION}/presto-server-${PRESTO_VERSION}.tar.gz
ENV PRESTO_CLI_URL https://repo1.maven.org/maven2/com/facebook/presto/presto-cli/${PRESTO_VERSION}/presto-cli-${PRESTO_VERSION}-executable.jar

RUN set -x && \
    curl -fSL -o - "$PRESTO_URL" | tar xz -C /usr/local && \
    mv /usr/local/presto-server-${PRESTO_VERSION} /usr/local/presto

RUN set -x && \
    wget ${PRESTO_CLI_URL} && \
    mv presto-cli-${PRESTO_VERSION}-executable.jar /usr/local/presto/presto && \
    chmod +x /usr/local/presto/presto

# Configure Presto
ENV PRESTO_HOME /usr/local/presto

RUN set -x && \
    mkdir ${PRESTO_HOME}/etc && \
    mkdir ${PRESTO_HOME}/etc/catalog && \
    mkdir /var/lib/presto
    # If you change the latter, also change it in node.properties config file

COPY evaluation/Presto_files/config/* /usr/local/presto/etc/
COPY evaluation/Presto_files/catalog/* /usr/local/presto/etc/catalog/

# Install MongoDB
RUN set -x && \
    apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2930ADAE8CAF5059EE73BB4B58712A2291FA4AD5 && \
    echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu xenial/mongodb-org/3.6 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-3.6.list && \
    apt-get update && \
    apt-get install -y mongodb-org && \
    mkdir -p /data/db

# Install Cassandra
RUN set -x && \
    echo "deb http://www.apache.org/dist/cassandra/debian 311x main" | tee -a /etc/apt/sources.list.d/cassandra.sources.list && \
    curl https://www.apache.org/dist/cassandra/KEYS | apt-key add - && \
    apt-get update && \
    apt-key adv --keyserver pool.sks-keyservers.net --recv-key A278B781FE4B2BDA && \
    apt-get -y install cassandra

# Install MySQL
RUN set -x && \
    echo 'mysql-server mysql-server/root_password password root' | debconf-set-selections  && \
    echo 'mysql-server mysql-server/root_password_again password root' | debconf-set-selections && \
    apt-get update && \
    apt-get install -y --no-install-recommends vim && \
    apt-get -y install mysql-server
    # to solve "Can't open and lock privilege tables: Table storage engine for 'user' doesn't have this option"
    # sed -i -e "s/^bind-address\s*=\s*127.0.0.1/bind-address = 0.0.0.0/" /etc/mysql/my.cnf && \
    # /etc/init.d/mysql start

# Install Spark
ENV SPARK_VERSION 2.4.0
RUN set -x  && \
    curl -fSL -o - http://ftp-stud.hs-esslingen.de/pub/Mirrors/ftp.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop2.7.tgz | tar xz -C /usr/local && \
    mv /usr/local/spark-${SPARK_VERSION}-bin-hadoop2.7 /usr/local/spark

# Generate BSBM data
RUN set -x && \
    apt-get update --fix-missing && \
    apt-get install -y unzip

RUN set -x && \
    wget -O bsbm.zip https://sourceforge.net/projects/bsbmtools/files/latest/download && \
    unzip bsbm.zip && \
    rm bsbm.zip && \
    cd bsbmtools-0.2 && \
    ./generate -fc -pc 1000 -s sql -fn /root/data/input && \
    cd /root/data/input && \
    ls && \
    rm 01* 02* 05* 06* 07*

# Due to a (yet) explaineable behavior from spark-submit assembly plugin,
# jena-arq is not being picked up during the assembly of Squerall
# So we will provide it during spark submit
ENV JENA_VERSION 3.9.0

RUN set -x && \
    wget http://central.maven.org/maven2/org/apache/jena/jena-arq/${JENA_VERSION}/jena-arq-${JENA_VERSION}.jar && \
    mv jena-arq-${JENA_VERSION}.jar /root

COPY evaluation/SQLtoNOSQL /root/SQLtoNOSQL
COPY evaluation/input_files/* /root/input/
COPY evaluation/input_files/queries/* /root/input/queries/

# just to force rebuild
RUN ls

RUN set -x && \
    # Install Squerall
    cd /usr/local && \
    git clone https://github.com/EIS-Bonn/Squerall.git && \
    cd Squerall && \
    mvn package

COPY evaluation/scripts/* /root/

RUN echo "\nbash /root/welcome.sh\n" >> /root/.profile

CMD ["/bin/bash","--login"]
