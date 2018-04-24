FROM ubuntu:16.04

MAINTAINER Mohamed Nadjib Mami <mami@cs.uni-bonn.de>

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
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
ENV HADOOP_VERSION 2.8.3
ENV HADOOP_URL http://mirror.synyx.de/apache/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz

RUN set -x && \
    # Install Hadoop
    curl -fSL -o - "$HADOOP_URL" | tar xz -C /usr/local && \
    ln -s /usr/local/hadoop-$HADOOP_VERSION /hadoop && \
    cp /hadoop/etc/hadoop/mapred-site.xml.template /hadoop/etc/hadoop/mapred-site.xml && \
    mkdir /usr/local/hadoop-$HADOOP_VERSION/logs

# Update environment
ENV HADOOP_PREFIX /opt/hadoop-$HADOOP_VERSION \
    HADOOP_CONF_DIR /etc/hadoop

RUN set -x && \
    # Install MongoDB
    apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2930ADAE8CAF5059EE73BB4B58712A2291FA4AD5 && \
    echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu xenial/mongodb-org/3.6 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-3.6.list && \
    apt-get update && \
    apt-get install -y mongodb-org && \
    mkdir -p /data/db
    #mongod

# EXPOSE 27017

RUN set -x && \
    # Install Cassandra
    echo "deb http://www.apache.org/dist/cassandra/debian 311x main" | tee -a /etc/apt/sources.list.d/cassandra.sources.list && \
    curl https://www.apache.org/dist/cassandra/KEYS | apt-key add - && \
    apt-get update && \
    apt-key adv --keyserver pool.sks-keyservers.net --recv-key A278B781FE4B2BDA && \
    apt-get -y install cassandra

RUN set -x && \
    # Install MySQL
    echo 'mysql-server mysql-server/root_password password root' | debconf-set-selections  && \
    echo 'mysql-server mysql-server/root_password_again password root' | debconf-set-selections && \
    apt-get update && \
    apt-get install -y --no-install-recommends vim && \
    apt-get -y install mysql-server 
    # to solve "Can't open and lock privilege tables: Table storage engine for 'user' doesn't have this option"
    # sed -i -e "s/^bind-address\s*=\s*127.0.0.1/bind-address = 0.0.0.0/" /etc/mysql/my.cnf && \
    # /etc/init.d/mysql start

ENV SPARK_VERSION 2.1.2

RUN set -x  && \
    # Install Spark
    curl -fSL -o - http://ftp-stud.hs-esslingen.de/pub/Mirrors/ftp.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop2.7.tgz | tar xz -C /usr/local && \
    ln -s /usr/local/spark-${SPARK_VERSION}-bin-hadoop2.7 /spark

WORKDIR /root

RUN set -x && \
    # Install further needed tools
    apt-get install -y wget unzip && \
    # Get BSBM data generator
    wget -O bsbm.zip https://sourceforge.net/projects/bsbmtools/files/latest/download && \
    unzip bsbm.zip && \
    rm bsbm.zip && \
    cd bsbmtools-0.2 && \
    ./generate -fc -pc 1000 -s sql -fn data && \
    cd data && \
    # Remove not needed SQL dumps (eg Vendor)
    rm 01* 02* 05* 06* 07* && \
    # Get pre-configured Sparkall config file
    wget https://raw.githubusercontent.com/EIS-Bonn/sparkall/master/evaluation/config && \
    # Due to a (yet) explaineable behavior from sbt assembly plugin, jena-arq is not being picked up during the assembly of Sparkall
    # So we will provide it during spark submit
    wget http://central.maven.org/maven2/org/apache/jena/jena-arq/3.1.1/jena-arq-3.1.1.jar && \
    mv jena-arq-3.1.1.jar /root
	
RUN set -x && \
    # Install Sparkall
    cd /usr/local && \
    git clone https://github.com/EIS-Bonn/sparkall.git && \
    cd sparkall && \
    sbt assembly # to generate JAR to submit to spark-submit

COPY evaluation/scripts/* /root/

#RUN [“chmod”, “+x”, "/root/welcome-script.sh”]
#CMD /root/welcome-script.sh

CMD ["bash"]
