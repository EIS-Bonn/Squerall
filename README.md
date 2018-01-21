# Sparkall
An implementation of the so-called Semantic Data Lake, using Apache Spark.

## Execution
To run Sparkall, use `spark-submit` giving in args three files built using [Sparkall-GUI](https://github.com/mnmami/spakall-gui) (see below).
The command line looks like:
`/bin/spark-submit --class [Main classpath] --master [master URI] --executor-memory [memory reserved to the app] sparkall.jar [query file] [mappings file] [config file] [master URI]`

### Example:
`/bin/spark-submit --class org.sparkall.Main --master spark://172.14.160.146:3077 --executor-memory 250G sparkall.jar query.sparql mappings.ttl config spark://172.14.160.146:3077`

## Sparkall-GUI
Sparkall has 3 interfaces to (1) provide access configuration to data in the Data Lake, (1) map data to ontology terms and (3) query the mapped data. Refer to Sparkall-GUI here: [Sparkall-GUI](https://github.com/mnmami/spakall-gui). 

## Publication
Sparkall is described in a paper ["Teach me to fish" Querying Semantic Data Lakes](https://www.researchgate.net/publication/322526357_%27Teach_me_to_fish%27_Querying_Semantic_Data_Lakes). The paper details all the building blocks and experiments conducted to demonstrate its mertits.

### Evaluation
To test out the performence of Sparkall, i.e., querying heterogeneous data residing in a Data Lake, we have generated data using Berlin Bernchmark (BSBM). We have taken 5 tables: Product, Producer, Offer, Review, and Person , and saved them in Cassandra, MySQL, Mon-goDB, Parquet and CSV, respectively.

We have generated three sizes of the data using the following BSBM scale factors (number of products): 500k, 1,5m, and 5m. To give a sense of the size of data, we provide how much those scale factors would generate of RDF triples: the 1,5m scale factor generate 500m triples, and the 5m scale factore generate 1,75b triples. As we took 5 tables, the previous numbers are not accurate, there are 5 more tables not considered. However, the taken tables contain most of the data.

'Teach me to fish' Querying Semantic Data Lakes. Available from: https://www.researchgate.net/publication/322526357_%27Teach_me_to_fish%27_Querying_Semantic_Data_Lakes [accessed Jan 21 2018]. 

For more enquireis, contact me on: mami@cs.uni-bonn.de
