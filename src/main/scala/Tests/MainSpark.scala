package Tests

import org.apache.spark.sql.SparkSession

import scala.sys.process._

/**
  * Created by mmami on 07.03.17.
  */
object MainSpark extends App {

    override def main(args: Array[String]) = {

        var input_path = args(0) //"/home/mmami/Documents/Datasets/Crimes_2012-2016.csv"
        var delimiter = args(1) // ,

        val session = SparkSession.builder.master("local").appName("Spark CSV Reader").getOrCreate;

        // load csv
        val df = session.read
            .option("header", "true") //reading the headers
            .option("mode", "DROPMALFORMED")
            .option("delimiter", delimiter)
            //.option("quote",'"')
            .format("csv")
            .csv(input_path);

        // 1. Query CSV directly
        df.createOrReplaceTempView("csv")

        var results = session.sql("select * from csv where RD > 1000")

        var now = System.nanoTime
        results.count()
        var timeElapsed = System.nanoTime - now
        println("It took " + timeElapsed + " to query CSV file directly")

        val cmd = "free && sync && echo 3 > /proc/sys/vm/drop_caches && free" // Your command
        val output = cmd.!! // Captures the output
        // *************************************************************************
        // load csv
        var now1 = System.nanoTime
        val df1 = session.read
            .option("header", "true") //reading the headers
            .option("mode", "DROPMALFORMED")
            .option("delimiter", delimiter)
            //.option("quote",'"')
            .csv(input_path);

        // 2.1 Save CSV as parquet
        df1.write.parquet("csv.parquet")

        // 2.2 Read parquet
        val dfp = session.read
            .option("header", "true") //reading the headers
            .option("quote",'"')
            .parquet("csv.parquet");

        // 2.2 Query parquet
        dfp.createOrReplaceTempView("csvparquet")
        var results1 = session.sql("select * from csvparquet where RD > 1000")

        results1.count()
        var timeElapsed1 = System.nanoTime - now1
        println("It took " + timeElapsed1 + " to query csv after loading it into a parquet file")
    }
}
