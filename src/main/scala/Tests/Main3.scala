package Tests

/**
  * Created by mmami on 07.03.17.
  */

object Main3 extends App {

    override def main(args: Array[String]) = {

        /*var q = mutable.Queue[String]()

        q.enqueue("1")
        q.enqueue("3")
        q.enqueue("5")
        q.enqueue("2")
        q.enqueue("4")
        q.enqueue("6")

        println("pendingJoins! " + q)

        q.dequeue()
        q.dequeue()

        println("pendingJoins " + q)

        var i = 0
        while (q.nonEmpty) {
            val e = q.head
            println(e)

            if (i < 2){
                i += 1
                q.enqueue(e)
            }

            q = q.tail
        }

        println("pendingJoins " + q)*/

        /*var parquet_schema: String = "java -jar /media/mmami/Extra/Scala/Web/parquet-mr/parquet-tools/target/parquet-tools-1.9.0.jar schema /media/mmami/Extra/Datasets/books.parquet" !!

        println(parquet_schema)

        parquet_schema  = parquet_schema.substring(parquet_schema.indexOf('\n') + 1)

        var set = parquet_schema.split("\n").toSeq.map(_.trim).filter(_ != "}").map(f => f.split(" ")(2))

        var schema = ""
        for (s <- set) {
            schema = schema + "," + s
        }

        schema = schema.substring(1)*/


        // To directly connect to the default server localhost on port 27017
        /*val client: MongoClient = MongoClient("mongodb://127.0.0.1:27017")
        val database: MongoDatabase = client.getDatabase("db")
        val collection: MongoCollection[Document] = database.getCollection("col")

        val replacementDoc: Document = Document("_id" -> 1, "x" -> 2, "y" -> 3)

        collection.find().collect().subscribe((results: Seq[Document]) => println(s"Found: #${results.size}"))*/

        /*val client: MongoClient = MongoClient()
        val database: MongoDatabase = client.getDatabase("db")
        val collection: MongoCollection[Document] = database.getCollection("institutes")

        // insert a document
        /*val document: Document = Document("IID" -> 2, "Name" -> "FFA")
        val insertObservable: SingleObservable[Completed] = collection.insertOne(document)

        insertObservable.subscribe(new Observer[Completed] {
            override def onNext(result: Completed): Unit = println(s"onNext: $result")
            override def onError(e: Throwable): Unit = println(s"onError: $e")
            override def onComplete(): Unit = println("onComplete")
        })*/


        // collection.find().collect().subscribe((results: Seq[Document]) => println(s"Found: #${results.size}"))
        // collection.find().subscribe((doc: Document) => println(doc.toJson()))

        var fields = ""
        for(x <- collection.find().limit(10)) {
            for(i <- x) {
                println("dddd " + i._1)
            }
        }

        println("fields: " + fields)

        scala.Console.readLine()
        client.close()*/

        /*var query = "TRANSFORM (?k?a.l.remove(\"a\") && ?k?l.right+16)\n}"

        val requiredString = query.substring(query.indexOf("TRANSFORM") + 9, query.lastIndexOf(")"))

        println("requiredString: " + requiredString)*/

        var scores = Map("a" -> 1, "in" -> 2, "r" -> 2, "k" -> 3, "c" -> 3, "e" -> 3)

        println("initial scores: " + scores)

        var i = 0
        while (scores.size > 0 && i < 100) {
            val it = scores.iterator
            while ({it.hasNext}) {
                val nxt = it.next()
                val a = nxt._1
                if(a == "e") {
                    scores -= a
                }

                scores -= "in"

                i = i + 1

                if(a == "in" && i < 4)
                    scores -= a

                if(a == "a" && i > 6)
                    scores -= a

                if(a == "c" && i > 8)
                    scores -= a

                if(a == "r" && i < 10)
                    scores -= a

                if(a == "k" && i > 12)
                    scores -= a

                println("scores: " + scores)
            }
        }

        println("final scores: " + scores)

    }
}

