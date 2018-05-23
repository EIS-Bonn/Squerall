package Tests

import org.apache.spark.sql.SparkSession

/**
  * Created by mmami on 07.03.17.
  */

object Main2 extends App {

    override def main(args: Array[String]) = {

        /*var queryString =
            "PREFIX rml: <http://semweb.mmlab.be/ns/rml#> " +
            "PREFIX ql: <http://semweb.mmlab.be/ns/ql#> " +
            "SELECT ?data_location " +
            "WHERE {" +
                "<#InsuranceMapping> rml:logicalSource ?s ." +
                "?s rml:source ?data_location ." +
                "OPTIONAL {?s ?o ?p . ?g ?o ?d . ?a ?b 'dd'}" +
            "}"

        //logger.info(queryString)

        // 1 Read RML mappings file
        var inputFileName = "RMLmappings.ttl"
        var in = FileManager.get().open(inputFileName)
        if (in == null) {
            throw new IllegalArgumentException(
                "File: " + inputFileName + " not found")
        }

        // 2 Prepare RML mappings (model)
        var model = ModelFactory.createDefaultModel()
        model.read(in, null, "TURTLE")

        // 3 get BGPs of the query
        var query = QueryFactory.create(queryString)
        print(query.getQueryPattern.toString)

        var op = Algebra.compile(query);

        var bgpWalk = new bgpWalker()

        ElementWalker.walk(query.getQueryPattern(), bgpWalk);

        // 3 Execute the query and obtain results
        var qe = QueryExecutionFactory.create(query, model)

        var results = qe.execSelect()
        results = ResultSetFactory.copyResults(results);*/

        // 4 show results
        /*var x = ""
        while(results.hasNext) { // only one result expected
            var soln = results.nextSolution()
            var x = soln.get("data_location").asLiteral().getString
            var l = soln.getLiteral("VarL") ;   // Get a result variable - must be a literal
            println(x)
            //print(l)
        }

        // Output query results
        //ResultSetFormatter.out(System.out, results, query);

        // Important - free up resources used running the query
        qe.close()*/

        /*import com.mongodb.MongoClient
        val mongoClient = new MongoClient("127.0.0.1", 27017)
        val database = mongoClient.getDatabase("db")
        val collection = database.getCollection("institutes")

        val myDoc = collection.find.first
        println("collection: " + myDoc)


        /*val cursor = collection.find.iterator
        try while ( {
            cursor.hasNext
        }) println(cursor.next.toJson)
        finally cursor.close()*/

        var set = Set[String]()
        import scala.collection.JavaConverters._
        var col = collection.find.asScala
        for (cur <- col) {
            for (x <- cur.asScala){
                set = set + x._1
            }
        }

        println(set.toString())
        mongoClient.close()*/

        /*val driver = "com.mysql.cj.jdbc.Driver"
        val url = "jdbc:mysql://localhost/db"
        val username = "root"
        val password = "root"

        // there's probably a better way to do this
        var connection: Connection = null

        try {
            // make the connection
            Class.forName(driver)
            connection = DriverManager.getConnection(url, username, password)

            // create the statement, and run the select query
            val statement = connection.createStatement()
            val resultSet = statement.executeQuery("SHOW COLUMNS FROM editors")
            while ( resultSet.next() ) {
                val Name = resultSet.getString("Field")
                println("Field = " + Name)
            }
        } catch {
            case e => e.printStackTrace
        }
        connection.close()*/

        // Test CouchBase Spark connector

        val spark = SparkSession.builder.master("local[*]").appName("Sparkall")
            .config("spark.couchbase.nodes", "127.0.0.1") // connect to couchbase on localhost
            .config("spark.couchbase.bucket.Vendor", "Vendor") // open the travel-sample bucket with empty password
            .getOrCreate;

        spark.sparkContext.setLogLevel("ERROR")

        //var options : Map[String, String] =  Map()
        //options += ("spark.couchbase.nodes" -> "127.0.0.1")
        //options += ("spark.couchbase.bucket.Vendor" -> "Vendor")
        //options += ("com.couchbase.username" -> "couchbase")
        //options += ("com.couchbase.password" -> "couchbase")
        //options += ("schemaFilter" -> "type=\"airline\"")
        //val df = spark.read.format("com.couchbase.spark.sql.DefaultSource").option("spark.couchbase.bucket.Vendor","Vendor").load()
        //val df = spark.read.format("com.couchbase.spark.sql.DefaultSource").options(options).schema().load()
        //val df = sqlContext.read.format("com.couchbase.spark.sql.DefaultSource").option("schemaFilter", "type=\"airline\"").load()
        //val airlines = spark.read.couchbase(schemaFilter = EqualTo("type", "airline"))
        //val df = spark.read.option("spark.couchbase.bucket.Vendor","Vendor").couchbase()

        import com.couchbase.spark.sql._
        val df = spark.read.couchbase()

        df.show()

        val options = Map("path" -> "bsbm/vendor", "es.nodes" -> "127.0.0.1", "es.port" -> "9200", "pushdown" -> "true")
        // es.nodes is used when we want specific nodes, otherwise es.nodes.discovery default true will discover and query * nodes
        val dfes = spark.read.format("org.elasticsearch.spark.sql").options(options).load // or load("index/type")
        dfes.show()

    }
}

//var tp = scala.collection.mutable.Set[Triple]()

/*class bgpWalker extends ElementVisitorBase {
    override def visit(el: ElementPathBlock ) {
        // ...go through all the triples...
        val triples = el.patternElts();
        while (triples.hasNext()) {
            // ...and grab the subject
            //tp += triples.next().asTriple()
            println("---" + triples.next().asTriple());
        }
    }
}*/