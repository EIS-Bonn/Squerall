#!/bin/bash

##
# Script to run  a Query on Squerall
##

#Settings
export SPARK=/usr/local/spark/bin
export SQUERALL_HOME=/usr/local/Squerall
export SQUERALL_EXEC=${SQUERALL_HOME}/target

#t Parameters
EXECUTOR_ADDRESS=$1; # Spark master
EXECUTOR_MEMORY=$2; # Ammount of memory allocated to Spark
QUERIE_S=$3; # Path where the benchmark of queries is located
MAPPINGS_FILE=$4; # Path to the mappings file
CONFIG_FILE=$5; # Path to the config file
REORDER_FLAG=$6; # Flag reordering
ENGINE_FLAG=$7; # Flag s: Spark or p: Presto (currently on Spark)
RESULT_FILE=$8; #Filename where the query output will be stored

[[ -e "$RESULT_FILE" ]] && rm "$RESULT_FILE"

#TIMEFORMAT=%R
if [[ -d ${QUERIE_S} ]]; then
    echo "***Executing all queries in $QUERIE_S"
    for i in "${QUERIE_S}"/*.sparql; do
        echo "Executing Query: $i"
        echo "Clearing cache " | tee --append "$RESULT_FILE" > /dev/null
        #echo sync && echo 3 | tee /proc/sys/vm/drop_caches
        sync; echo 3 > /proc/sys/vm/drop_caches

        echo "$i" | tee --append "$RESULT_FILE" > /dev/null
        # echo $i >> $RESULT_FILE;

        # Run
        if [[ "$ENGINE_FLAG" = "s" ]]; then
            (/usr/bin/time -f "time: %e (sec)" "$SPARK"/spark-submit --class org.squerall.Main --executor-memory "$EXECUTOR_MEMORY" --master "$EXECUTOR_ADDRESS" --jars /root/jena-arq-3.9.0.jar "$SQUERALL_EXEC"/squerall-0.1.0.jar "$i" "$MAPPINGS_FILE" "$CONFIG_FILE" "$EXECUTOR_ADDRESS" "$REORDER_FLAG" "$ENGINE_FLAG") >> "$RESULT_FILE" 2>&1
        elif [[ "$ENGINE_FLAG" = "p" ]]; then
            (/usr/bin/time -f "time: %e (sec)" java -cp "$SQUERALL_EXEC"/squerall-0.1.0.jar:/root/jena-arq-3.9.0.jar:/root/presto-jdbc-304.jar org.squerall.Main "$i" "$MAPPINGS_FILE" "$CONFIG_FILE" "$EXECUTOR_ADDRESS" n "$ENGINE_FLAG")
        fi
          #statements
        # ../spark-2.1.0-bin-hadoop2.7/bin/spark-submit --class org.squerall.Main --executor-memory 200G --master spark://host:port squerall.jar query3.sparql mappings.ttl config spark://host:port r
    done

elif [[ -f ${QUERIE_S} ]]; then
    echo "***Executing one query: $QUERIE_S"
    echo "Clearing cache " | tee --append "$RESULT_FILE" > /dev/null

    if [[ "$ENGINE_FLAG" = "s" ]]; then
        (/usr/bin/time -f "time: %e (sec)" "$SPARK"/spark-submit --class org.squerall.Main --executor-memory "$EXECUTOR_MEMORY" --master "$EXECUTOR_ADDRESS" --jars /root/jena-arq-3.9.0.jar "$SQUERALL_EXEC"/squerall-0.1.0.jar "$QUERIE_S" "$MAPPINGS_FILE" "$CONFIG_FILE" "$EXECUTOR_ADDRESS" "$REORDER_FLAG" "$ENGINE_FLAG") > "$RESULT_FILE" 2>&1
    elif [[ "$ENGINE_FLAG" = "p" ]]; then
      (/usr/bin/time -f "time: %e (sec)" java -cp ${SQUERALL_EXEC}/squerall-0.1.0.jar:/root/jena-arq-3.9.0.jar:/root/presto-jdbc-304.jar org.squerall.Main "$QUERIE_S" "$MAPPINGS_FILE" "$CONFIG_FILE" "$EXECUTOR_ADDRESS" n "$ENGINE_FLAG")
    fi
else
    echo "$QUERIE_S is not a valid query file or queries directory"
    exit 1
fi
