package org.sqltonosql.loaders

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

	def product(inputPath: String, keyspace: String, table: String) {

        val spark = SparkSession.builder.master("local[*]").appName("SQL dump to NoSQL Loader").getOrCreate;
        spark.sparkContext.setLogLevel("ERROR")

        val sql = spark.sparkContext.textFile(inputPath)

        val lines = sql.filter(_.contains("INSERT INTO"))

        val lines1 = lines.map(l =>l.substring(30)) // change

        val lines2 = lines1.map(l =>l.dropRight(2))

        val lines3 = lines2.flatMap(l => l.split("\\),\\("))

        val lines4 = lines3.map(_.split(","))

        import org.apache.spark.sql._

        var linesA = lines4.map(l => if(l(4)=="null") Array(l(0),l(1),l(2),l(3),-1,l(5),l(6),l(7),l(8),l(9),l(10),l(11),l(12),l(13),l(14),l(15),l(16),l(17)) else l)
        linesA = linesA.map(l => if(l(5)=="null") Array(l(0),l(1),l(2),l(3),l(4),-1,l(6),l(7),l(8),l(9),l(10),l(11),l(12),l(13),l(14),l(15),l(16),l(17)) else l)
        linesA = linesA.map(l => if(l(6)=="null") Array(l(0),l(1),l(2),l(3),l(4),l(5),-1,l(7),l(8),l(9),l(10),l(11),l(12),l(13),l(14),l(15),l(16),l(17)) else l)
        linesA = linesA.map(l => if(l(7)=="null") Array(l(0),l(1),l(2),l(3),l(4),l(5),l(6),-1,l(8),-1,l(10),l(11),l(12),l(13),l(14),l(15),l(16),l(17)) else l)
        linesA = linesA.map(l => if(l(8)=="null") Array(l(0),l(1),l(2),l(3),l(4),l(5),l(6),l(7),-1,l(9),l(10),l(11),l(12),l(13),l(14),l(15),l(16),l(17)) else l)
        linesA = linesA.map(l => if(l(9)=="null") Array(l(0),l(1),l(2),l(3),l(4),l(5),l(6),l(7),l(8),-1,l(10),l(11),l(12),l(13),l(14),l(15),l(16),l(17)) else l)
        linesA = linesA.map(l => if(l(16)=="null") Array(l(0),l(1),l(2),l(3),l(4),l(5),l(6),l(7),l(8),l(8),l(10),l(11),l(12),l(13),l(14),l(15),-1,l(17)) else l)

        val rowRDD = linesA.map(l => Row(l(0).toString.toInt, l(1), l(2), l(3).toString.toInt, l(4).toString.toInt, l(5).toString.toInt, l(6).toString.toInt, l(7).toString.toInt, l(8).toString.toInt, l(9).toString.toInt, l(10).toString, l(11).toString, l(12).toString, l(13).toString, l(14).toString, l(15).toString, l(16).toString.toInt, java.sql.Date.valueOf(l(17).toString.replace("'",""))))


        import org.apache.spark.sql.types._

        val fields = Seq(StructField("nr", IntegerType, nullable = true), StructField("label", StringType, nullable = true), StructField("comment", StringType, nullable = true), StructField("producer", IntegerType, nullable = true), StructField("propertyNum1", IntegerType, nullable = true), StructField("propertyNum2", IntegerType, nullable = true), StructField("propertyNum3", IntegerType, nullable = true), StructField("propertyNum4", IntegerType, nullable = true), StructField("propertyNum5", IntegerType, nullable = true), StructField("propertyNum6", IntegerType, nullable = true), StructField("propertyTex1", StringType, nullable = true), StructField("propertyTex2", StringType, nullable = true), StructField("propertyTex3", StringType, nullable = true), StructField("propertyTex4", StringType, nullable = true), StructField("propertyTex5", StringType, nullable = true), StructField("propertyTex6", StringType, nullable = true), StructField("publisher", IntegerType, nullable = true), StructField("publishDate", DateType, nullable = true))

        val schema = StructType(fields)

        val productsDF = spark.createDataFrame(rowRDD, schema)

        import org.apache.spark.sql.cassandra._

        productsDF.write.format("org.apache.spark.sql.cassandra").options(Map("table" -> table, "keyspace" -> keyspace)).save()
    }

	   def person(inputPath: String, header: String, delimiter: String, mode: String, outputFile: String) {

        val spark = SparkSession.builder.master("local[*]").appName("SQL dump to NoSQL Loader").getOrCreate;
        spark.sparkContext.setLogLevel("ERROR")

        val sql = spark.sparkContext.textFile(inputPath)

        val lines = sql.filter(_.contains("INSERT INTO"))

        val lines1 = lines.map(l =>l.substring(29))

        val lines2 = lines1.map(l =>l.dropRight(2))

        val lines3 = lines2.flatMap(l => l.split("\\),\\("))

        import org.apache.spark.sql._
        import org.apache.spark.sql.types._

        val fields = Seq(StructField("nr", IntegerType, nullable = true), StructField("name", StringType, nullable = true), StructField("mbox_sha1sum", StringType, nullable = true), StructField("country", StringType, nullable = true), StructField("publisher", IntegerType, nullable = true), StructField("publishDate", DateType, nullable = true))

        val schema = StructType(fields)

        //val format = new java.text.SimpleDateFormat("yyyy-MM-dd")

        val rowRDD = lines3.map(_.split(",")).map(attributes => Row(attributes(0).toInt, attributes(1), attributes(2), attributes(3), attributes(4).toInt, java.sql.Date.valueOf(attributes(5).replace("'",""))))

        val personsDF = spark.createDataFrame(rowRDD, schema)

        personsDF.write.option("header","true").csv(outputFile)
    }

    def review(inputPath: String, outputFile: String) {

        val spark = SparkSession.builder.master("local[*]").appName("SQL dump to NoSQL Loader").getOrCreate;
        spark.sparkContext.setLogLevel("ERROR")

        val sql = spark.sparkContext.textFile(inputPath)

        val lines = sql.filter(_.contains("INSERT INTO"))

        val lines1 = lines.map(l =>l.substring(29))

        val lines2 = lines1.map(l =>l.dropRight(2))

        val lines3 = lines2.flatMap(l => l.split("\\),\\("))

        val lines4 = lines3.map(_.split(","))

        import org.apache.spark.sql._

        var linesA = lines4.map(l => if(l(8)=="null") Array(l(0),l(1),l(2),l(3),l(4),l(5),l(6),l(7),-1,l(9),l(10),l(11),l(12),l(13)) else l)
        linesA = linesA.map(l => if(l(9)=="null") Array(l(0),l(1),l(2),l(3),l(4),l(5),l(6),l(7),l(8),-1,l(10),l(11),l(12),l(13)) else l)
        linesA = linesA.map(l => if(l(10)=="null") Array(l(0),l(1),l(2),l(3),l(4),l(5),l(6),l(7),l(8),l(9),-1,l(11),l(12),l(13)) else l)
        linesA = linesA.map(l => if(l(11)=="null") Array(l(0),l(1),l(2),l(3),l(4),l(5),l(6),l(7),l(8),l(9),l(10),-1,l(12),l(13)) else l)

        val rowRDD = linesA.map(l => Row(l(0).toString.toInt, l(1).toString.toInt, l(2).toString.toInt, l(3).toString.toInt, java.sql.Date.valueOf(l(4).toString.replace("'","")), l(5).toString, l(6).toString, l(7).toString, l(8).toString.toInt, l(9).toString.toInt, l(10).toString.toInt, l(11).toString.toInt, l(12).toString.toInt, java.sql.Date.valueOf(l(13).toString.replace("'",""))))

        import org.apache.spark.sql.types._

        val fields = Seq(StructField("nr", IntegerType, nullable = true), StructField("product", IntegerType, nullable = true), StructField("producer", IntegerType, nullable = true), StructField("person", IntegerType, nullable = true), StructField("reviewDate", DateType, nullable = true), StructField("title", StringType, nullable = true), StructField("text", StringType, nullable = true), StructField("language", StringType, nullable = true), StructField("rating1", IntegerType, nullable = true), StructField("rating2", IntegerType, nullable = true), StructField("rating3", IntegerType, nullable = true), StructField("rating4", IntegerType, nullable = true), StructField("publisher", IntegerType, nullable = true), StructField("publishDate", DateType, nullable = true))

        val schema = StructType(fields)

        val reviewsDF = spark.createDataFrame(rowRDD, schema)

        reviewsDF.write.parquet(outputFile)
    }

}
