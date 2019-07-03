package org.squerall

import org.apache.spark.sql.DataFrame
import org.squerall.model.DataQueryFrame

/**
  * Created by mmami on 26.01.17.
  */
object Main extends App {

    var queryFile = args(0)
    val mappingsFile = args(1)
    val configFile = args(2)
    val executorID = args(3)
    val reorderJoin = args(4)
    val queryEngine = args(5)

    if (queryEngine == "s") { // Spark as query engine
        val executor : SparkExecutor = new SparkExecutor(executorID, mappingsFile)

        val run = new Run[DataFrame](executor)
        run.application(queryFile,mappingsFile,configFile,executorID)

    } else if(queryEngine == "p") { // Presto as query engine
        val executor : PrestoExecutor = new PrestoExecutor(executorID, mappingsFile)
        val run = new Run[DataQueryFrame](executor)
        run.application(queryFile,mappingsFile,configFile,executorID)
    }

}
