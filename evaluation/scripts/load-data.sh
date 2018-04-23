#!/bin/bash

mongod
cassandra -R

cqlsh -e "CREATE KEYSPACE db WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;"
cqlsh -e "CREATE TABLE db.product (nr int PRIMARY KEY, comment text,  label text, producer int, "propertyNum1" int, "propertyNum2" int, "propertyNum3" int, "propertyNum4" int, "propertyNum5" int, "propertyNum6" int, "propertyTex1" text, "propertyTex2" text, "propertyTex3" text, "propertyTex4" text, "propertyTex5" text, "propertyTex6" text, "publishDate" date, publisher int);"

cd /usr/local/sparkall/evaluation/SQLtoNOSQL
sbt "run /root/bsbmtools-0.2/data/04Product.sql Product /usr/local/sparkall/evaluation/config"
sbt "run /root/bsbmtools-0.2/data/08Offer.sql Offer /usr/local/sparkall/evaluation/config"
sbt "run /root/bsbmtools-0.2/data/09Person.sql Person /usr/local/sparkall/evaluation/config"
sbt "run /root/bsbmtools-0.2/data/10Review.sql Review /usr/local/sparkall/evaluation/config"

chown -R mysql:mysql /var/lib/mysql /var/run/mysqld
sudo /etc/init.d/mysql start

mysql -u root --password=root mysql < 03Producer.sql
