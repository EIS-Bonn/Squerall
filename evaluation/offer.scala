val args = spark.conf.get("spark.driver.args").split("\\s+")
val source = args(0)
val mongoHost = args(1)
val mongoDB = args(2)
val mongoCollection = args(3)
val mongoURI = "mongodb://" + mongoHost + "/" + mongoDB + "." + mongoCollection
//...?replicaSet=mongo-rs

val sql = sc.textFile(source)

val lines = sql.filter(_.contains("INSERT INTO"))

val lines1 = lines.map(l =>l.substring(28)) // 28 = size of "INSERT INTO `offer` VALUES " 

val lines2 = lines1.map(l =>l.dropRight(2))

val lines3 = lines2.flatMap(l => l.split("\\),\\("))

val lines4 = lines3.map(_.split(","))

import org.apache.spark.sql._
import org.apache.spark.sql.types._

val rowRDD = lines4.map(l => Row(l(0).toString.toInt, l(1).toString.toInt, l(2).toString.toInt, l(3).toString.toInt, l(4).toString.toDouble, java.sql.Date.valueOf(l(5).toString.replace("'","")), java.sql.Date.valueOf(l(6).toString.replace("'","")), l(7).toString.toInt, l(8).toString, l(9).toString.toInt, java.sql.Date.valueOf(l(10).toString.replace("'",""))))

// NOTE _ID instead of NR to benefit from indexing
val fields = Seq(StructField("_id", IntegerType, nullable = true), StructField("product", IntegerType, nullable = true), StructField("producer", IntegerType, nullable = true), StructField("vendor", IntegerType, nullable = true), StructField("price", DoubleType, nullable = true), StructField("validFrom", DateType, nullable = true), StructField("validTo", DateType, nullable = true), StructField("deliveryDays", IntegerType, nullable = true), StructField("offerWebpage", StringType, nullable = true), StructField("publisher", IntegerType, nullable = true), StructField("publishDate", DateType, nullable = true))

val schema = StructType(fields)

val offersDF = spark.createDataFrame(rowRDD, schema)

offersDF.write.format("com.mongodb.spark.sql").option("uri", mongoURI).mode("overwrite").save()
