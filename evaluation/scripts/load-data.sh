#!/bin/bash
cassandra -R &> /dev/null &
mongod &> /dev/null &
echo "Hi, MongoDB and Cassandra are starting..."

cd /usr/local/Squerall/evaluation/SQLtoNOSQL
mvn clean install
echo "***************************Loading data to CSV***************************"
mvn exec:java -X -Dexec.args="/root/bsbmtools-0.2/data/09Person.sql Person /usr/local/Squerall/evaluation/config"

echo "***************************Loading data to Parquet***********************"
mvn exec:java -X -Dexec.args="/root/bsbmtools-0.2/data/10Review.sql Review /usr/local/Squerall/evaluation/config"

echo "***************************Preparing Cassandra DB************************"
cqlsh -e "CREATE KEYSPACE db WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;"
cqlsh -e 'CREATE TABLE db.product (nr int PRIMARY KEY, comment text,  label text, producer int, "propertyNum1" int, "propertyNum2" int, "propertyNum3" int, "propertyNum4" int, "propertyNum5" int, "propertyNum6" int, "propertyTex1" text, "propertyTex2" text, "propertyTex3" text, "propertyTex4" text, "propertyTex5" text, "propertyTex6" text, "publishDate" date, publisher int);'

echo "***************************Loading data to Cassandra*********************"
mvn exec:java -X -Dexec.args="/root/bsbmtools-0.2/data/04Product.sql Product /usr/local/Squerall/evaluation/config"

echo "***************************Loading data to MongoDB***********************"
mvn exec:java -X -Dexec.args="/root/bsbmtools-0.2/data/08Offer.sql Offer /usr/local/Squerall/evaluation/config"

chown -R mysql:mysql /var/lib/mysql /var/run/mysqld
/etc/init.d/mysql start

cd  /root/bsbmtools-0.2/data
mysql -u root --password=root mysql < 03Producer.sql
