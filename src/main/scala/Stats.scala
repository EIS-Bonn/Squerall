import java.sql.DriverManager

import io.gatling.jsonpath.JsonPath
import org.codehaus.jackson.map.ObjectMapper

import scala.io.Source

object Stats extends App {
    override def main(args: Array[String]) = {

        val configFile = args(0)



        Class.forName("org.h2.Driver")
        val con = DriverManager.getConnection("jdbc:h2:/media/mmami/Extra/Datasets/sparkall", "metadata", "")
        val stmt = con.createStatement
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS TABLE_STATS (DATASOURCE VARCHAR(50), TABLE_NAME VARCHAR(50), CARDINALITY INT)")
        //stmt.executeUpdate("INSERT INTO TABLE_STATS VALUES ('cassandra', 'product', 500000)")
        val rs = stmt.executeQuery("SELECT * FROM TABLE_STATS")
        while (rs.next()) {
            val name = rs.getString("CARDINALITY")
            //println(name)
        }
        stmt.close
        con.close()

        val fileContent = Source.fromFile(configFile).getLines.mkString
        val json = (new ObjectMapper()).readValue(fileContent, classOf[Object])

        val resURl = JsonPath.query("$.sources[*].type", json).right.map(_.toVector)

        val types = resURl.right.toSeq.head

        println("types: " + types)
        /*
        for(t <- types) {
            t match {
                case ("mongodb") =>
                    val resURl = JsonPath.query("$.sources[?(@.entity == 'Offer')].options.url", json).right.map(_.toVector)
                    val url = resURl match { case Right(Vector(url)) => url case _ => /*default -> fail*/ }

                    val resDatabase = JsonPath.query("$.sources[?(@.entity == 'Offer')].options.database", json).right.map(_.toVector)
                    val database = resDatabase match { case Right(Vector(database)) => database case _ => /*default -> fail*/ }

                    val resCollection = JsonPath.query("$.sources[?(@.entity == 'Offer')].options.collection", json).right.map(_.toVector)
                    val collection = resCollection match { case Right(Vector(collection)) => collection case _ => /*default -> fail*/ }

                    import org.mongodb.scala._

                    val mongoClient: MongoClient = MongoClient("mongodb://localhost")
                    val db: MongoDatabase = mongoClient.getDatabase(database.toString)
                    val col: MongoCollection[Document] = db.getCollection(collection.toString);

                    /*val colTest: MongoCollection[Document] = db.getCollection("test");
                    val doc: Document = Document("_id" -> 8, "name" -> "MongoDB", "type" -> "database",
                        "count" -> 1, "info" -> Document("x" -> 203, "y" -> 102))
                    colTest.insertOne(doc).results*/

                    val count = for {
                        countResult <- col.count()
                    } yield countResult

                    val card = count.headResult()

                    println(s"MongoDB: $url + $database + $collection --> count: $card")

                case ("cassandra") =>
                    val resKeyspacel = JsonPath.query("$.sources[?(@.entity == 'Product')].options.keyspace", json).right.map(_.toVector)
                    val keyspace = resKeyspacel match { case Right(Vector(keyspace)) => keyspace case _ => /*default -> fail*/ }

                    val resTable = JsonPath.query("$.sources[?(@.entity == 'Product')].options.table", json).right.map(_.toVector)
                    val table = resTable match { case Right(Vector(table)) => table case _ => /*default -> fail*/ }

                    import com.datastax.driver.core.Cluster
                    import scala.collection.JavaConversions._

                    val cluster = Cluster.builder.addContactPoint("127.0.0.1").build
                    val session = cluster.connect("db")

                    val results = session.execute("SELECT count(nr) as count FROM " + table)
                    val card = results.head.getLong("count")

                    println(s"Cassandra: $keyspace + $table -> count: $card")

                case ("jdbc") =>
                    val resURl = JsonPath.query("$.sources[?(@.entity == 'Producer')].options.url", json).right.map(_.toVector)
                    val url = resURl match { case Right(Vector(url)) => url case _ => /*default -> fail*/ }

                    val resDriver = JsonPath.query("$.sources[?(@.entity == 'Producer')].options.driver", json).right.map(_.toVector)
                    val driver = resDriver match { case Right(Vector(driver)) => driver case _ => /*default -> fail*/ }

                    val resDbtable = JsonPath.query("$.sources[?(@.entity == 'Producer')].options.dbtable", json).right.map(_.toVector)
                    val dbtable = resDbtable match { case Right(Vector(dbtable)) => dbtable case _ => /*default -> fail*/ }

                    val resUser = JsonPath.query("$.sources[?(@.entity == 'Producer')].options.user", json).right.map(_.toVector)
                    val user = resUser match { case Right(Vector(user)) => user case _ => /*default -> fail*/ }

                    val resPassword = JsonPath.query("$.sources[?(@.entity == 'Producer')].options.password", json).right.map(_.toVector)
                    val password = resPassword match { case Right(Vector(password)) => password case _ => /*default -> fail*/ }


                    var connection : Connection = null
                    var card = 0
                    try {
                        Class.forName("com.mysql.jdbc.Driver")
                        connection = DriverManager.getConnection(url.toString, user.toString, password.toString)
                        val statement = connection.createStatement
                        val rs = statement.executeQuery("SELECT COUNT(nr) AS count FROM " + dbtable)
                        while (rs.next) {
                            card = rs.getInt("count")
                        }
                    } catch {
                        case e: Exception => e.printStackTrace
                    }
                    connection.close

                    println(s"JDBC: $url + $driver + $dbtable + $user + $password -> count: $card")

                case ("parquet") =>
                    val resOutputFile = JsonPath.query("$.sources[?(@.entity == 'Review')].source", json).right.map(_.toVector)
                    val outputFile = resOutputFile match { case Right(Vector(outputFile)) => outputFile case _ => /*default -> fail*/ }

                    println(s"Parquet: $outputFile -> count: card")


                case ("csv") =>
                    val resHeader = JsonPath.query("$.sources[?(@.entity == 'Person')].options.header", json).right.map(_.toVector)
                    val header = resHeader match { case Right(Vector(header)) => header case _ => /*default -> fail*/ }

                    val resDelimiter = JsonPath.query("$.sources[?(@.entity == 'Person')].options.delimiter", json).right.map(_.toVector)
                    val delimiter = resDelimiter match { case Right(Vector(delimiter)) => delimiter case _ => /*default -> fail*/ }

                    val resMode = JsonPath.query("$.sources[?(@.entity == 'Person')].options.mode", json).right.map(_.toVector)
                    val mode = resMode match { case Right(Vector(mode)) => mode case _ => /*default -> fail*/ }

                    val resOutputFile = JsonPath.query("$.sources[?(@.entity == 'Person')].source", json).right.map(_.toVector)
                    val outputFile = resOutputFile match { case Right(Vector(outputFile)) => outputFile case _ => /*default -> fail*/ }

                    println(s"CSV: $header + $delimiter + $mode + $outputFile -> count: card")

                case _ => ""
            }
        }

*/








    }
}
