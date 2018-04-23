#!/bin/bash

mongod
cassandra -R
sudo /etc/init.d/mysql start

cd /usr/local/sparkall/evaluation/SQLtoNOSQL
sbt "run /root/bsbmtools-0.2/data/04Product.sql Product /usr/local/sparkall/evaluation/config"
sbt "run /root/bsbmtools-0.2/data/08Offer.sql Offer /usr/local/sparkall/evaluation/config"
sbt "run /root/bsbmtools-0.2/data/09Person.sql Person /usr/local/sparkall/evaluation/config"
sbt "run /root/bsbmtools-0.2/data/10Review.sql Review /usr/local/sparkall/evaluation/config"
chown -R mysql:mysql /var/lib/mysql /var/run/mysqld
mysql -u root --password=root mysql < 03Producer.sql
