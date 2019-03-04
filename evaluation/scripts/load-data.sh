#!/bin/bash
cassandra -R &> /dev/null &
mongod &> /dev/null &
echo "Hi, MongoDB and Cassandra are starting..."

# Preprocess data: take values from SQL dump into the sources
mvn clean install -f /root/SQLtoNOSQL/pom.xml
echo "***************************Loading data to CSV***************************"
mvn exec:java -f /root/SQLtoNOSQL/pom.xml -X -Dexec.args="/root/data/input/09Person.sql Person /root/input/config"

echo "***************************Loading data to Parquet***********************"
mvn exec:java -f /root/SQLtoNOSQL/pom.xml -X -Dexec.args="/root/data/input/10Review.sql Review /root/input/config"

echo "***************************Preparing Cassandra DB************************"
cqlsh -e "CREATE KEYSPACE db WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;"
cqlsh -e 'CREATE TABLE db.product (nr int PRIMARY KEY, comment text,  label text, producer int, "propertyNum1" int, "propertyNum2" int, "propertyNum3" int, "propertyNum4" int, "propertyNum5" int, "propertyNum6" int, "propertyTex1" text, "propertyTex2" text, "propertyTex3" text, "propertyTex4" text, "propertyTex5" text, "propertyTex6" text, "publishDate" date, publisher int);'

echo "***************************Loading data to Cassandra*********************"
mvn exec:java -f /root/SQLtoNOSQL/pom.xml -X -Dexec.args="/root/data/input/04Product.sql Product /root/input/config"

echo "***************************Loading data to MongoDB***********************"
mvn exec:java -f /root/SQLtoNOSQL/pom.xml -X -Dexec.args="/root/data/input/08Offer.sql Offer /root/input/config"

# Start SSH in case it's not started (check later if needed)
service ssh restart

# Format namenode
/usr/local/hadoop/bin/hdfs namenode -format

# Start HDFS
/usr/local/hadoop/sbin/start-dfs.sh

# Start MySQL
# chown -R mysql:mysql /var/lib/mysql /var/run/mysqld
/etc/init.d/mysql start

# Create hive database
mysql -u root --password=root -e "CREATE DATABASE hive"

# Create hive user
mysql -u root --password=root -e "GRANT ALL PRIVILEGES ON hive.* TO 'hiveuser'@'localhost' IDENTIFIED BY 'hiveuser';"

# Run schematools to create hive schema
/usr/local/hive/bin/schematool -initSchema -dbType mysql --verbose

# Move CSV and Parquet to HDFS
/usr/local/hadoop/bin/hadoop fs -mkdir /root
/usr/local/hadoop/bin/hadoop fs -mkdir /root/data
/usr/local/hadoop/bin/hadoop fs -put /root/data/person.csv /root/data/
/usr/local/hadoop/bin/hadoop fs -put /root/data/review.parquet /root/data/

# Start Hive metastore
/usr/local/hive/bin/hive --service metastore &

# Create CSV external table using Hive CLI
/usr/local/hive/bin/hive -e "CREATE EXTERNAL TABLE IF NOT EXISTS default.person (nr INT, country VARCHAR(200), mbox_sha1sum VARCHAR(200), name VARCHAR(200), publishDate DATE, publisher INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION 'hdfs://localhost:9000/root/data/person.csv' TBLPROPERTIES ('skip.header.line.count'='1');"

# Start Presto server
/usr/local/presto/bin/launcher start
echo "Waiting for Presto to start..."
sleep 30
# Start Parquet external table using Presto CLI
/usr/local/presto/presto --execute "CREATE TABLE hive.default.review (nr int, product int, producer int, person int, reviewDate timestamp, title varchar, text varchar,language char(2), rating1 int, rating2 int, rating3 int, rating4 int, publisher int, publishDate date) with (format = 'parquet', external_location = 'hdfs://localhost:9000/root/data/review.parquet');"

mysql -u root --password=root mysql < /root/data/input/03Producer.sql

bash /root/welcome.sh
