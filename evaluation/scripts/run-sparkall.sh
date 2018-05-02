#!/bin/bash

##
# Script to run  a Query on Sparkall
##

#Settings
export SPARK=/usr/local/spark-2.1.2-bin-hadoop2.7/bin
export SPARKALL_HOME=/usr/local/sparkall/target/scala-2.11

#t Parameters
SPARK_MASTER=$1; # Spark master
EXECUTOR_MEMORY=$2; # Ammount of memory allocated to Spark
QUERIE_S=$3; # Path where the benchmark of queries is located
MAPPINGS_FILE=$4 # Path to the mappings file
CONFIG_FILE=$5 # Path to the config file
REORDER_FLAG=$6 # Flag reordering
RESULT_FILE=$7; #Filename where the query output will be stored

#TIMEFORMAT=%R
if [[ -d $QUERIE_S ]]; then
    echo "***Executing all queries in $QUERIE_S"
    for i in `ls $QUERIE_S/*.sparql`; do
        echo "Executing Query: $i"
        echo "Clearing cache " | tee --append $RESULT_FILE > /dev/null
        #echo sync && echo 3 | tee /proc/sys/vm/drop_caches
        sync; echo 3 > /proc/sys/vm/drop_caches

        echo $i | tee --append $RESULT_FILE > /dev/null
        # echo $i >> $RESULT_FILE;

        # Run
        (/usr/bin/time -f "time: %e (sec)" $SPARK/spark-submit --class org.sparkall.Main --executor-memory $EXECUTOR_MEMORY --master $SPARK_MASTER --jars /root/jena-arq-3.1.1.jar $SPARKALL_HOME/sparkall_01.jar $i $MAPPINGS_FILE $CONFIG_FILE $SPARK_MASTER $REORDER_FLAG) >> $RESULT_FILE 2>&1

        # ../spark-2.1.0-bin-hadoop2.7/bin/spark-submit --class org.sparkall.Main --executor-memory 200G --master spark://host:port sparkall.jar query3.sparql mappings.ttl config spark://host:port r
    done

elif [[ -f $QUERIE_S ]]; then
    echo "***Executing one query: $QUERIE_S"
    echo "Clearing cache " | tee --append $RESULT_FILE > /dev/null

   (/usr/bin/time -f "time: %e (sec)" $SPARK/spark-submit --class org.sparkall.Main --executor-memory $EXECUTOR_MEMORY --master $SPARK_MASTER --jars /root/jena-arq-3.1.1.jar $SPARKALL_HOME/sparkall_01.jar $QUERIE_S $MAPPINGS_FILE $CONFIG_FILE $SPARK_MASTER $REORDER_FLAG) > $RESULT_FILE 2>&1
else
    echo "$QUERIE_S is not a valid query file or queries directory"
    exit 1
fi


cat $RESULT_FILE | grep -o -E "Number of results(.*?)|timeTaken(.*?)|Q(.*?).sparql"
