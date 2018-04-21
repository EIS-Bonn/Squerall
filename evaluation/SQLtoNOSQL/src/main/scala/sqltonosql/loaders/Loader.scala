package loaders

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

case class Loader(configFile: String) {


    def offer(inputPath: String, url: String, database: String, collection: String) {

        val spark = SparkSession.builder.master("local[*]").appName("SQL dump to NoSQL Loader").getOrCreate;
        spark.sparkContext.setLogLevel("ERROR")


        val sql = spark.sparkContext.textFile(inputPath)

        val lines = sql.filter(_.contains("INSERT INTO"))

        val lines1 = lines.map(l =>l.substring(28)) // change - count "..("

        val lines2 = lines1.map(l =>l.dropRight(2))

        val lines3 = lines2.flatMap(l => l.split("\\),\\("))

        val lines4 = lines3.map(_.split(","))

        import org.apache.spark.sql._
        import org.apache.spark.sql.types._

        val rowRDD : RDD[Row] = lines4.map(l => Row(l(0).toInt, l(1).toInt, l(2).toInt, l(3).toInt, l(4).toDouble, java.sql.Date.valueOf(l(5).replace("'","")), java.sql.Date.valueOf(l(6).replace("'","")), l(7).toInt, l(8), l(9).toInt, java.sql.Date.valueOf(l(10).replace("'","")))).toJavaRDD

        // NOTE _ID instead of NR to benefit from indexing
        val fields = Seq(StructField("_id", IntegerType, nullable = true), StructField("product", IntegerType, nullable = true), StructField("producer", IntegerType, nullable = true), StructField("vendor", IntegerType, nullable = true), StructField("price", DoubleType, nullable = true), StructField("validFrom", DateType, nullable = true), StructField("validTo", DateType, nullable = true), StructField("deliveryDays", IntegerType, nullable = true), StructField("offerWebpage", StringType, nullable = true), StructField("publisher", IntegerType, nullable = true), StructField("publishDate", DateType, nullable = true))

        val schema = StructType(fields)

        val offersDF = spark.createDataFrame(rowRDD, schema)

		offersDF.show()

        offersDF.write.format("com.mongodb.spark.sql").option("uri", "mongodb://" + url + "/" + database + "." + collection).mode("overwrite").save()


		spark.stop()
    }

}

