import io.gatling.jsonpath._

import loaders.Loader
import scala.io.Source
import org.codehaus.jackson.map.ObjectMapper

object Main extends App {

	override def main(args: Array[String]) = {
		val inputSQLDump = args(0) // /media/mmami/EIS_Ext/Sparkall/BSBM/bsbmtools-0.2/data/08Offer.sql
        val entityName = args(1) // "Offer"
        val configFile = args(2) // "/media/mmami/Extra/Scala/Web/play-scala-starter-example/conf/config"  


        val fileContent = Source.fromFile(configFile).getLines.mkString

        val json = (new ObjectMapper()).readValue(fileContent, classOf[Object])


        entityName match {
            case ("Offer") =>
                val resURl = JsonPath.query("$.sources[?(@.entity == 'Offer')].options.url", json).right.map(_.toVector)
                val url = resURl match { case Right(Vector(url)) => url case _ => /*default -> fail*/ }

                val resDatabase = JsonPath.query("$.sources[?(@.entity == 'Offer')].options.database", json).right.map(_.toVector)
                val database = resDatabase match { case Right(Vector(database)) => database case _ => /*default -> fail*/ }

                val resCollection = JsonPath.query("$.sources[?(@.entity == 'Offer')].options.collection", json).right.map(_.toVector)
                val collection = resCollection match { case Right(Vector(collection)) => collection case _ => /*default -> fail*/ }

                println(s"MongoDB: $url + $database + $collection")

                val loader = new Loader(configFile)

                loader.offer(inputSQLDump,url.toString,database.toString,collection.toString)

            case ("Product") =>
                val resKeyspacel = JsonPath.query("$.sources[?(@.entity == 'Product')].options.keyspace", json).right.map(_.toVector)
                val keyspace = resKeyspacel match { case Right(Vector(keyspace)) => keyspace case _ => /*default -> fail*/ }

                val resTable = JsonPath.query("$.sources[?(@.entity == 'Product')].options.table", json).right.map(_.toVector)
                val table = resTable match { case Right(Vector(table)) => table case _ => /*default -> fail*/ }

                println(s"Cassandra: $keyspace + $table")

                val loader = new Loader(configFile)

                loader.product(inputSQLDump,keyspace.toString,table.toString)
            case ("Producer") =>
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

			def review(inputPath: String, outputFile: String): Unit = {

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
	}
}
