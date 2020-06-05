[![Gitter](https://img.shields.io/gitter/room/DAVFoundation/DAV-Contributors.svg?style=flat-square)](https://gitter.im/squerall)
[![DOI](https://zenodo.org/badge/99688061.svg)](https://zenodo.org/badge/latestdoi/99688061)

# Squerall
An implementation of the so-called Semantic Data Lake, using Apache Spark and Presto. Semantic Data Lake is a Data Lake accessed using Semantic Web technologies: ontologies and query language (SPARQL).

Currently supported data sources:
- Evaluated: CSV, Parquet, MongoDB, Cassandra, JDBC (MySQL, SQL Server, etc.).
- Experimental: Elasticsearch, Couchbase.
You can extend Squerall to support more, see [below](#extensibility).

To get an understanding of Squerall basics, which also helps understand the installation steps hereafter, please refer to this Wiki page: [Squerall Basics](https://github.com/EIS-Bonn/Squerall/wiki/Squerall-Basics).

## Setup and Execution
*- Prerequisite:* You need Maven to build Squerall from the source. Refer to the official documentations for installation instructions: [Maven](https://maven.apache.org/install.html) and [SBT](https://www.scala-sbt.org/1.0/docs/Setup.html). Once that is installed, run:
```
git clone https://github.com/EIS-Bonn/squerall.git
cd squerall
mvn package
cd target
```
...by default, you find a *squerall-0.2.0.jar* file.

Squerall (previously Sparkall) uses Spark and Presto as query engine. User specifies which underlying query engine to use. Therefore Spark and/or Presto has to be installed beforehand. Both Spark and Presto are known to among the easiest frameworks to configure and get started with. You can choose to run Spark/Presto and thus Squerall in a single node, or deploy them in a cluster.

### Spark
- Download Spark from the [Spark official website](https://spark.apache.org/downloads.html). In order for Spark to run in a cluster, you need to configure a 'standalone cluster' following guidelines in the [official documentation page](https://spark.apache.org/docs/2.2.0/spark-standalone.html).

- Once Spark is installed, navigate to `bin` folder and run `spark-submit` script giving in arguments three files ---built using [Squerall-GUI](https://github.com/EIS-Bonn/squerall-gui) (see below).
The command line looks like:
`/bin/spark-submit --class [Main classpath] --master [master URI] --executor-memory [memory reserved to the app] [path to squerall-0.2.0.jar] [query file] [mappings file] [config file] [master URI] n s`

- #### Example:
`/bin/spark-submit --class org.squerall.Main --master spark://127.140.106.146:3077 --executor-memory 250G /etc/squerall-0.2.0.jar query.sparql mappings.ttl config spark://172.14.160.146:3077 n p`

  * query file: a file containing a correct SPARQL query, only.
  * mappings file: a file contains RML mappings linking data to ontology terms (classes and properties), in JSON format.
  * config file: a file containing information about how to access data sources (eg. host, user, password), in JSON format.

### Presto
- Install Presto from [Presto official website](https://prestodb.io/docs/current/installation/deployment.html).
- Once Presto is installed, navigate to `bin` folder and run `squerall-0.2.0.jar` like you run any Java application:
`java -cp [path to squerall-0.2.0.jar] org.squerall.Main [query file] [mappings file] [config file] [Presto server url (host:port)] n p`

  * query, mappings and config files are identical to Spark command above.

- #### Example:
`java -cp /etc/squerall-0.2.0.jar org.squerall.Main query.sparql mappings.ttl config jdbc:presto://localhost:8080 n p`

  **- Note:** If any error raised due to Presto libs not found, download and append `presto-jdbc-{xyz}.jar` (e.g., from [here](http://central.maven.org/maven2/io/prestosql/presto-jdbc/304/presto-jdbc-304.jar
) for version 'presto-jdbc-304') "`:presto-jdbc-xyz.jar`" to `squerall-0.2.0.jar` in the command.

- #### Presto and Hive metastore
Presto is meant to access existing database management systems; therefore, it doesn't have its own metadata store. For file-based data sources, like CSV and Parquet, Presto uses Hive metastore. As a result, prior to running queries in Presto, CSV and Parque files have to be registered in Hive metastore. Parquet files can be registered using [Presto Hive connector (see 'Examples')](https://prestodb.io/docs/current/connector/hive.html); CSV files need to be registered inside Hive as an [*external* table (see 'Create an external table')](https://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.6.5/bk_data-access/content/moving_data_from_hdfs_to_hive_external_table_method.html).

## Squerall-GUI
Squerall has 3 interfaces to (1) provide access configuration to data in the Data Lake, (2) map data to ontology terms and (3) query the mapped data. These interfaces generate the needed input files used for query execution: config, mappings and query, respectively. Refer to Squerall-GUI repository here: [Squerall-GUI](https://github.com/EIS-Bonn/squerall-gui) for more information.

## Evaluation
We provide in this repository the code-source, queries and docker image for anyone who wants to try Squerall on their own. Refer to the [dedicated page](https://github.com/EIS-Bonn/Squerall/tree/master/evaluation).

## Extensibility
Squerall is extensible by design, developers can themselves add support to more data sources, or even add a new query engine alongside Spark and Presto. Refer to the [Wiki](https://github.com/EIS-Bonn/Squerall/wiki/Extending-Squerall) for the details.

## Publications
- [Squerall: Virtual Ontology-Based Access to Heterogeneous and Large Data Sources](https://www.researchgate.net/publication/334194326_Squerall_Virtual_Ontology-Based_Access_to_Heterogeneous_and_Large_Data_Sources)
- [Uniform Access to Multiform Data Lakes using Semantic Technologies](https://www.researchgate.net/publication/336363044_Uniform_Access_to_Multiform_Data_Lakes_using_Semantic_Technologies)
- [Querying Data Lakes using Spark and Presto](https://www.researchgate.net/publication/331530051_Querying_Data_Lakes_using_Spark_and_Presto)
- [How to feed the Squerall with RDF and other data nuts?](https://www.researchgate.net/publication/335001108_How_to_feed_the_Squerall_with_RDF_and_other_data_nuts)
- [Semantic Data Integration for the SMT Manufacturing Process using SANSA Stack](https://dgraux.github.io/publications/SANSA-DL_VQB_ESWC_2020_Industry.pdf)

## Contact
For any setup difficulties or other inquiries, please contact me on: mami@cs.uni-bonn.de, or ask directly on [Gitter chat](https://gitter.im/squerall).

License
-------

This project is openly shared under the terms of the __Apache License
v2.0__ ([read for more](./LICENSE)).
