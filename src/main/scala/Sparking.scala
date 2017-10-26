import java.util

import com.mongodb.spark.config.ReadConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.{HashMap, Set}


/**
  * Created by mmami on 30.01.17.
  */

import org.apache.log4j.{Level, Logger}

class Sparking(sparkURI: String) {

    def query (sources : Set[(HashMap[String, String], String, String)], optionsMap: HashMap[String, Map[String, String]], toJoinWith: Boolean, star: String, prefixes: Map[String, String], select: util.List[String]): DataFrame = {

        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)

        val spark = SparkSession.builder.master(sparkURI).appName("Sparkall").getOrCreate;

        var finalDF : DataFrame = null
        var datasource = 0

        for (s <- sources) {
            println("\nNEXT SOURCE...")
            datasource += 1 // in case of multiple relevant data sources to union

            var attr_pred = s._1
            println("attr_pred: " + attr_pred)
            var sourcePath = s._2
            var sourceType = Helpers.getTypeFromURI(s._3)
            var options = optionsMap(sourcePath)

            var columns = Helpers.getSelectColumnsFromSet(attr_pred, Helpers.omitQuestionMark(star), prefixes, select)

            println("Relevant source (" + datasource + ") is: [" + sourcePath + "] of type: [" + sourceType + "]")

            println("...from which columns (" + columns + ") are going to be projected")
            println("...with the following configuration options: " + options)

            if(toJoinWith) { // That kind of table who is the 2nd operand of a join operation
                var id = Helpers.getID(sourcePath)
                println("... is to be joined with using the ID: " + Helpers.omitQuestionMark(star) + "_" + id + " (obtained from subjectMap)")
                columns = columns + "," + id + " AS " + Helpers.omitQuestionMark(star) + "_ID"
            }

            var df : DataFrame = null
            sourceType match {
                case "csv" => df = spark.read.options(options).csv(sourcePath)
                case "parquet" => df = spark.read.options(options).parquet(sourcePath)
                case "cassandra" =>
                    //spark.conf.set("spark.cassandra.connection.host", "127.0.0.1")
                    //println("CASSANDRACONF:" + spark.conf.get("spark.cassandra.connection.host"))
                    df = spark.read.format("org.apache.spark.sql.cassandra").options(options).load
                case "mongodb" =>
                    //spark.conf.set("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.myCollection")
                    val vals = options.values.toList
                    val mongoConf = Helpers.makeMongoURI(vals(0), vals(1), vals(2))
                    val mongoOptions: ReadConfig = ReadConfig(Map("uri" -> mongoConf))
                    df = spark.read.format("com.mongodb.spark.sql").options(mongoOptions.asOptions).load
                case "jdbc" =>
                    df = spark.read.format("jdbc").options(options).load()
                case _ =>
            }

            df.createOrReplaceTempView("table")
            var newDF = spark.sql("SELECT " + columns + " FROM table")

            if(datasource == 1) {
                finalDF = newDF

                //newDF.show()

                //finalDF.show()
                //println("-Print before union ")
                //finalDF.collect.foreach(t => println(t))
                //finalDF.printSchema
            } else {
                finalDF = finalDF.union(newDF)

                //finalDF.show()
                //println("-Print after union ")
                //finalDF.collect.foreach(t => println(t))
                //finalDF.printSchema
            }

            //println("2")
            //finalDF.printSchema()
        }

        return finalDF
    }
}