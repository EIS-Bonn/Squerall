package org.sparkall

import model.DataQueryFrame
import org.apache.spark.sql.DataFrame

/**
  * Created by mmami on 26.01.17.
  */
object Main extends App {

    var queryFile = args(0)
    val mappingsFile = args(1)
    val configFile = args(2)
    val executorID = args(3)
    val queryEngine = args(5)

    if(queryEngine == "s") {
        val executor : SparkExecutor = new SparkExecutor(executorID, mappingsFile)
        //val finalResults = executor.getType()

        val run = new Run[DataFrame](executor)
        run.application(queryFile,mappingsFile,configFile,executorID)
    } else {
        val executor : PrestoExecutor = new PrestoExecutor(executorID, mappingsFile)
        val run = new Run[DataQueryFrame](executor)
        run.application(queryFile,mappingsFile,configFile,executorID)
    }

}