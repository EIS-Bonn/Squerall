# Sparkall
An implementation of the so-called Semantic Data Lake, using Apache Spark.

## Execution
- Clone the repository and package the project. As Sparkall is built using Scala, the SBT build tool (like Maven) is needed. Refer to the official documentation for the [installation](https://www.scala-sbt.org/1.0/docs/Setup.html) setps.
- Once SBT is installed, run: `sudo sbt assembly`, this will generate a JAR file called sparkall_01.jar.
- To run Sparkall, use `spark-submit` giving in args three files built using [Sparkall-GUI](https://github.com/EIS-Bonn/spakall-gui) (see below).
The command line looks like:
`/bin/spark-submit --class [Main classpath] --master [master URI] --executor-memory [memory reserved to the app] sparkall_01.jar [query file] [mappings file] [config file] [master URI]`

### Example:
`/bin/spark-submit --class org.sparkall.Main --master spark://172.14.160.146:3077 --executor-memory 250G sparkall.jar query.sparql mappings.ttl config spark://172.14.160.146:3077`

## Sparkall-GUI
Sparkall has 3 interfaces to (1) provide access configuration to data in the Data Lake, (2) map data to ontology terms and (3) query the mapped data. Refer to Sparkall-GUI repository here: [Sparkall-GUI](https://github.com/EIS-Bonn/sparkall-gui) for more information.

## Publication
Sparkall is described in a paper ["Teach me to fish" Querying Semantic Data Lakes](https://www.researchgate.net/publication/322526357_%27Teach_me_to_fish%27_Querying_Semantic_Data_Lakes). The paper details all the building blocks and experiments conducted to demonstrate Sparkall's mertits.

### Evaluation
We provide the script, queries and docker for anyone who wants to try Sparkall on their own. Refer to the dedicated page: https://github.com/EIS-Bonn/sparkall/tree/master/evaluation

'Teach me to fish' Querying Semantic Data Lakes. Available from: https://www.researchgate.net/publication/322526357_%27Teach_me_to_fish%27_Querying_Semantic_Data_Lakes [accessed Jan 21 2018]. 

For more enquireis, contact me on: mami@cs.uni-bonn.de
