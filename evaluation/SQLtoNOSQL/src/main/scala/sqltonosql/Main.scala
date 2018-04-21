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
            case ("Review") =>
            case ("Person") =>
        }
	}
}
