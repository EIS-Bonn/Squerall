import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * Created by mmami on 06.03.17.
  */
object CassandraConnector extends App {

    var sparkURI = "local"
    val spark = SparkSession.builder.master(sparkURI).appName("Sparkall").getOrCreate;

    var options : mutable.Map[String,String] = mutable.Map()
    options.put("keyspace", "db")
    options.put("table", "reviewers")

    spark.conf.set("spark.cassandra.connection.host", "127.0.0.1")
    println("CASSANDRACONF:" + spark.conf.get("spark.cassandra.connection.host"))

    var df = spark.read.format("org.apache.spark.sql.cassandra").options(options).load

    df.createOrReplaceTempView("table")

    df.printSchema()

    var ndf = spark.sql("select rid from table")

    ndf.printSchema()

    ndf.foreach(t => println(t))

}
