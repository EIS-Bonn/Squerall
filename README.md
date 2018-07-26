[![Gitter](https://img.shields.io/gitter/room/DAVFoundation/DAV-Contributors.svg?style=flat-square)](https://gitter.im/squerall)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.1247913.svg)](https://zenodo.org/record/1247913)

# Squerall (previously Sparkall)
An implementation of the so-called Semantic Data Lake, using Apache Spark and Presto. Semantic Data Lake is a Data Lake accessed using Semantic Web technologies: ontologies and query language (SPARQL).

Currently supported data sources: **CSV, Parquet, MongoDB, Cassandra, Elasticsearch, Couchbase, JDBC (MySQL, SQL Server, etc.)**.

## Setup and Execution
- *Prerequisite:* As Squerall is built using Scala, the SBT build tool (similar to Maven) is needed. Both Scala and SBT need to be installed beforehand. Refer to the official documentations for installation instructions: [Scala](https://www.scala-lang.org/download) and [SBT](https://www.scala-sbt.org/1.0/docs/Setup.html). Once that is installed, run:
```
git clone https://github.com/EIS-Bonn/squerall.git
cd squerall
sudo sbt assembly
cd target/scala-xyz # xyz is the version of Scala installed
```
...you find a *squerall_01.jar* file.

- Squerall (previously Sparkall) uses Spark and Presto as query engine. User specifies which underlying query engine to use. Therefore Spark and/or Presto has to be installed beforehand. Download Spark from the [Spark official website](https://spark.apache.org/downloads.html) and Presto from [Presot official website](https://prestodb.io/docs/current/installation/deployment.html). Both Spark and Presto are known to among the easiest frameworks to configure and get started with. You can choose to run Spark/Presto and thus Squerall in a single node, or deploy them in a cluster. In order for Spark to run in a cluster, you need to configure a 'standalone cluster' following guidelines in the [official documentation page](https://spark.apache.org/docs/2.2.0/spark-standalone.html).

- Once Spark is installed, navigate to `bin` folder and run `spark-submit` script giving in arguments three files ---built using [Squerall-GUI](https://github.com/EIS-Bonn/squerall-gui) (see below).
The command line looks like:
`/bin/spark-submit --class [Main classpath] --master [master URI] --executor-memory [memory reserved to the app] [path to squerall_01.jar] [query file] [mappings file] [config file] [master URI] n s`

### Example:
`/bin/spark-submit --class org.sparkall.Main --master spark://127.140.106.146:3077 --executor-memory 250G /etc/sparkall_01.jar query.sparql mappings.ttl config spark://172.14.160.146:3077 n p`

  * query file: a file containing a correct SPARQL query, only.
  * mappings file: a file contains RML mappings linking data to ontology terms (classes and properties), in JSON format.
  * config file: a file containing information about how to access data sources (eg. host, user, password), in JSON format.

- Once Presto is installed, navigate to `bin` folder and run `squerall_01.jar` like you run any Java application:
`java -cp [path to squerall_01.jar] org.sparkall.Main [query file] [mappings file] [config file] [Presto server url (host:port)] n p`

### Example:
`java -cp /etc/squerall_01.jar org.sparkall.Main query.sparql mappings.ttl config jdbc:presto://localhost:8089 n p`

*Note:* if any error due to the Presto libs not found, append download presto-jdbc-xyz.jar "`:presto-jdbc-xyz.jar`" to `squerall.jar`.

## Sparkall-GUI
Squerall has 3 interfaces to (1) provide access configuration to data in the Data Lake, (2) map data to ontology terms and (3) query the mapped data. The allow to create the needed input files: config, mappings and query. Refer to Sparkall-GUI repository here: [Sparkall-GUI](https://github.com/EIS-Bonn/sparkall-gui) for more information.

## Publication
A preprint describing Squerall can be found at [Squerall: Virtual Ontology-Based Access to
Heterogeneous and Large Data Sources](http://www.semantic-web-journal.net/system/files/swj1957.pdf). The preprint details all the building blocks and show some experiments conducted to demonstrate Sparkall's mertits.

### Evaluation
We provide in this repository the code-source, queries and docker image for anyone who wants to try Squerall on their own. Refer to the [dedicated page](https://github.com/EIS-Bonn/sparkall/tree/master/evaluation).

*Note:* At the moment only Spark-based Sqeuerall is covered in the docker version, Presto part will come soon.

## Contact
For any setup difficulties or other enquireis, please contact me on: mami@cs.uni-bonn.de, or ask directly on [Gitter chat](https://gitter.im/squerall).

License
-------

This project is openly shared under the terms of the __Apache License
v2.0__ ([read for more](./LICENSE)).


