package Tests

import org.apache.spark.sql.SparkSession

/**
  * Created by mmami on 06.03.17.
  */
object ParquetSDL extends App {

    var input_path = "/home/mmami/Documents/Datasets/test_data.tsv"

    val session = SparkSession.builder.master("local").appName("Spark CSV Reader").getOrCreate;

    // load csv
    val df = session.read
        .option("header", "true") //reading the headers
        .option("mode", "DROPMALFORMED")
        .option("delimiter","\t")
        .option("quote",'"')
        .csv(input_path);

    // 1. Query CSV directly
    df.createOrReplaceTempView("csv")

    var results = session.sql("select * from csv")

    var now = System.nanoTime
    results.count()
    var timeElapsed = System.nanoTime - now
    println("It took " + timeElapsed + " to query CSV file directly")


    // *************************************************************************
    // load csv
    val df1 = session.read
        .option("header", "true") //reading the headers
        .option("mode", "DROPMALFORMED")
        .option("delimiter","\t")
        .option("quote",'"')
        .csv(input_path);

    // 2.1 Save CSV as parquet
    df1.write.parquet("csv.parquet")

    // 2.2 Read parquet
    val dfp = session.read
        .option("header", "true") //reading the headers
        .option("mode", "DROPMALFORMED")
        .option("quote",'"')
        .parquet("csv.parquet");

    // 2.2 Query parquet
    dfp.createOrReplaceTempView("csvparquet")
    var results1 = session.sql("select * from csvparquet")

    var now1 = System.nanoTime
    results.count()
    var timeElapsed1 = System.nanoTime - now1
    println("It took " + timeElapsed1 + " to query CSV file directly")

}
