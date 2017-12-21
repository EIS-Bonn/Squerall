import java.util

import Helpers.get_NS_predicate
import com.google.common.collect.ArrayListMultimap
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.collection.mutable.{HashMap, Set}


/**
  * Created by mmami on 30.01.17.
  */

import Helpers._
import org.apache.log4j.{Level, Logger}

class Sparking(sparkURI: String) {

    def query (sources : Set[(HashMap[String, String], String, String)],
               optionsMap: HashMap[String, Map[String, String]],
               toJoinWith: Boolean,
               star: String,
               prefixes: Map[String, String],
               select: util.List[String],
               star_predicate_var: mutable.HashMap[(String, String), String],
               neededPredicates: Set[String],
               filters: ArrayListMultimap[String, (String, String)],
               leftJoinTransformations: (String, Array[String]),
               rightJoinTransformations: Array[String],
               joinPairs: Map[(String,String), String]
        ): DataFrame = {

        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)

        val spark = SparkSession.builder.master(sparkURI).appName("Sparkall").getOrCreate;

        var finalDF : DataFrame = null
        var datasource_count = 0

        for (s <- sources) {
            println("\nNEXT SOURCE...")
            datasource_count += 1 // in case of multiple relevant data sources to union

            val attr_predicate = s._1
            println("Star: " + star)
            println("attr_predicate: " + attr_predicate)
            val sourcePath = s._2
            val sourceType = getTypeFromURI(s._3)
            val options = optionsMap(sourcePath)

            // TODO: move to another class better
            var columns = getSelectColumnsFromSet(attr_predicate, omitQuestionMark(star), prefixes, select, star_predicate_var, neededPredicates)

            println("Relevant source (" + datasource_count + ") is: [" + sourcePath + "] of type: [" + sourceType + "]")

            println("...from which columns (" + columns + ") are going to be projected")
            println("...with the following configuration options: " + options)

            if (toJoinWith) { // That kind of table that is the 1st or 2nd operand of a join operation
                val id = getID(sourcePath)
                println("...is to be joined with using the ID: " + omitQuestionMark(star) + "_" + id + " (obtained from subjectMap)")
                if(columns == "") {
                    //println("heeey id = " + id + " star " + star)
                    columns = id + " AS " + omitQuestionMark(star) + "_ID"
                } else
                    columns = columns + "," + id + " AS " + omitQuestionMark(star) + "_ID"
            }

            println("star_predicate_var: " + star_predicate_var)

            var df : DataFrame = null
            sourceType match {
                case "csv" => df = spark.read.options(options).csv(sourcePath)
                case "parquet" => df = spark.read.options(options).parquet(sourcePath)
                case "cassandra" =>
                    //spark.conf.set("spark.cassandra.connection.host", "127.0.0.1")
                    //println("CASSANDRA CONF:" + spark.conf.get("spark.cassandra.connection.host"))
                    df = spark.read.format("org.apache.spark.sql.cassandra").options(options).load
                case "mongodb" =>
                    //spark.conf.set("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.myCollection")
                    val values = options.values.toList
                    val mongoConf = makeMongoURI(values(0), values(1), values(2))
                    val mongoOptions: ReadConfig = ReadConfig(Map("uri" -> mongoConf))
                    df = spark.read.format("com.mongodb.spark.sql").options(mongoOptions.asOptions).load
                case "jdbc" =>
                    df = spark.read.format("jdbc").options(options).load()
                case _ =>
            }

            df.createOrReplaceTempView("table")
            var newDF = spark.sql("SELECT " + columns + " FROM table")

            if(datasource_count == 1) {
                finalDF = newDF

            } else {
                finalDF = finalDF.union(newDF)
            }

            // Transformations
            if(leftJoinTransformations._2 != null) {
                val column : String = leftJoinTransformations._1
                println("leftJoinTransformations: " + column + " - " + leftJoinTransformations._2.mkString("."))
                val ns_pred = get_NS_predicate(column)
                val ns = prefixes(ns_pred._1)
                val pred = ns_pred._2
                val col = omitQuestionMark(star) + "_" + pred + "_" + ns
                finalDF = transform(finalDF, col, leftJoinTransformations._2)
            }
            if(!rightJoinTransformations.isEmpty) {
                println("rightJoinTransformations: " + rightJoinTransformations.mkString("_"))
                val col = omitQuestionMark(star) + "_ID"
                finalDF = transform(finalDF, col, rightJoinTransformations)
            }
        }

        println("\n- filters: " + filters + " ======= " + star)

        var whereString = ""

        val it = filters.keySet().iterator()
        while (it.hasNext) {
            val value = it.next()
            val predicate = star_predicate_var.
                filter(t => t._2 == value).
                keys. // To obtain (star, predicate) pairs having as value the FILTER'ed value
                filter(t => t._1 == star).
                map(f => f._2).toList

            if(predicate.nonEmpty) {
                val ns_p = get_NS_predicate(predicate.head) // Head because only one value is expected to be attached to the same star an same (object) variable
                val column = omitQuestionMark(star) + "_" + ns_p._2 + "_" + prefixes(ns_p._1)
                println("column: " + column)

                val conditions = filters.get(value).iterator()

                while (conditions.hasNext) {
                    val operand_value = conditions.next()
                    println("operand_value" + operand_value)
                    whereString = column + operand_value._1 + operand_value._2
                    println("whereString: " + whereString)
                    finalDF = finalDF.filter(whereString)
                }
            }
        }

        finalDF
    }

    def transform (df: DataFrame, column: String, transformationsArray : Array[String]): DataFrame = {

        var ndf : DataFrame = null
        for (t <- transformationsArray) {
            t match {
                case "toInt" => ndf = df.withColumn(column, df(column).cast(IntegerType))
                // From SO: values not castable will become null
                case s if s.contains("scl") =>
                    val scaleValue = s.replace("scl","").trim.stripPrefix("(").stripSuffix(")")
                    println("SCL found: " + scaleValue)
                    val operation = scaleValue.charAt(0)
                    operation match {
                        case '+' => ndf = df.withColumn(column, df(column) + scaleValue.substring(0).toInt)
                        case '-' => ndf = df.withColumn(column, df(column) - scaleValue.substring(0).toInt)
                    }
                case s if s.contains("skp") =>
                    val skipValue = s.replace("skp","").trim.stripPrefix("(").stripSuffix(")")
                    println("SKP found: " + skipValue)
                    ndf = df.filter(!df(column).equalTo(skipValue))
                case s if s.contains("replc") =>
                    val replaceValues = s.replace("replc","").trim.stripPrefix("(").stripSuffix(")").split("\\,")
                    val valToReplace = replaceValues(0)
                    val valToReplaceWith = replaceValues(1)
                    println("REPLC found: " + replaceValues.mkString(" "))
                    ndf = df.withColumn(column, when(col(column).equalTo(valToReplace), valToReplaceWith))
                    //ndf = df.withColumn(column, when(col(column) === valToReplace, valToReplaceWith).otherwise(col(column)))
                case _ =>
            }
        }

        ndf
    }
}