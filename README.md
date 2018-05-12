[![Gitter](https://img.shields.io/gitter/room/DAVFoundation/DAV-Contributors.svg?style=flat-square)](https://gitter.im/sparkall)

# Sparkall
An implementation of the so-called Semantic Data Lake, using Apache Spark. Semantic Data Lake is a Data Lake accessed Semantic Web technologies: ontologies and query language (SPARQL).

## Setup and Execution
- *Prerequisite:* As Sparkall is built using Scala, the SBT build tool (similar to Maven) is needed. Both Scala and SBT need to be installed beforehand. Refer to the official documentations for installation instructions: [Scala](https://www.scala-lang.org/download) and [SBT](https://www.scala-sbt.org/1.0/docs/Setup.html). Once that is installed, run:
```
git clone https://github.com/EIS-Bonn/sparkall.git
cd sparkall
sudo sbt assembly
cd target/scala-xyz # xyz is the version of Scala installed
```
...you find a *sparkall_01.jar* file.

- Now you can run Sparkall using `spark-submit` giving in args three files ---built using [Sparkall-GUI](https://github.com/EIS-Bonn/sparkall-gui) (see below).
The command line looks like:
`/bin/spark-submit --class [Main classpath] --master [master URI] --executor-memory [memory reserved to the app] sparkall_01.jar [query file] [mappings file] [config file] [master URI]`

  * query file: a file containing a correct SPARQL query, only.
  * mappings file: a file contains RML mappings linking data to ontology terms (classes and properties), in JSON format.
  * config file: a file containing information about how to access data sources (eg. host, user, password), in JSON format.

### Example:
`/bin/spark-submit --class org.sparkall.Main --master spark://172.14.160.146:3077 --executor-memory 250G sparkall_01.jar query.sparql mappings.ttl config spark://172.14.160.146:3077`

## Sparkall-GUI
Sparkall has 3 interfaces to (1) provide access configuration to data in the Data Lake, (2) map data to ontology terms and (3) query the mapped data. The allow to create the needed input files: config, mappings and query. Refer to Sparkall-GUI repository here: [Sparkall-GUI](https://github.com/EIS-Bonn/sparkall-gui) for more information.

## Publication
A preprint describing Sparkall can be found at ["Teach me to fish" Querying Semantic Data Lakes](https://www.researchgate.net/publication/322526357_%27Teach_me_to_fish%27_Querying_Semantic_Data_Lakes). The preprint details all the building blocks and show some experiments conducted to demonstrate Sparkall's mertits.

### Evaluation
We provide in this repository the code-source, queries and docker image for anyone who wants to try Sparkall on their own. Refer to the [dedicated page](https://github.com/EIS-Bonn/sparkall/tree/master/evaluation).

## Contact
For any setup difficulties or other enquireis, please contact me on: mami@cs.uni-bonn.de, or ask directly on [Gitter chat](https://gitter.im/sparkall).

License
-------

This project is openly shared under the terms of the __Apache License
v2.0__ ([read for more](./LICENSE)).


