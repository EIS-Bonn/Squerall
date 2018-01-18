# Sparkall
An implementation of the so-called Semantic Data Lake, using Apache Spark.

To run Sparkall, use spark-submit giving in args three files built using Sparkall-GUI, see below.
The command looks like:
`/bin/spark-submit --class [Main classpath] --master [master URI] --executor-memory [memory reserved to the app] sparkall.jar [query file] [mappings file] [config file] [master URI]`

Example:
`sudo /bin/spark-submit --class org.sparkall.Main --master spark://172.14.160.146:3077 --executor-memory 250G sparkall.jar query1.sparql mappings.ttl config spark://172.14.160.146:3077`

Sparkall has 3 GUIs to (1) provide access config to data in Data Lake, (1) map data and (3) query mapped data. Refer to Sparkall-GUI here: (Sparkall-GUI)[https://github.com/mnmami/spakall-gui] 

For more information, refer to the paper here: ["Teach me to fish" Querying Semantic Data Lakes](https://www.researchgate.net/publication/322526357_%27Teach_me_to_fish%27_Querying_Semantic_Data_Lakes)

More info to come soon. Contact me on: mami@cs.uni-bonn.de
