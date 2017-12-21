import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import Helpers._
/**
  * Created by mmami on 26.01.17.
  */
object Main extends App {

    Logger.getLogger("ac.biu.nlp.nlp.engineml").setLevel(Level.OFF)
    Logger.getLogger("org.BIU.utils.logging.ExperimentLogger").setLevel(Level.OFF)
    Logger.getRootLogger().setLevel(Level.OFF)

    if (args.length == 1)
        println(s"Hello, ${args(0)}!")
    else
        println("Hello, anonymous!")

    // 1. Read SPARQL query
    println("\n/*******************************************************************/")
    println("/*                         QUERY ANALYSIS                          */")
    println("/*******************************************************************/")
    var queryFile = Config.get("query")

    val queryString = scala.io.Source.fromFile(queryFile)
    var query = try queryString.mkString finally queryString.close()

    // Transformations
    val trans = query.substring(query.indexOf("TRANSFORM") + 9, query.lastIndexOf(")")) // E.g. ?k?a.toInt && ?a?l.r.toInt.scl(_+61)
    val transformations = trans.trim().substring(1).split("&&") // [?k?a.l.+60, ?a?l.r.toInt]
    var transmap_left : Map[String,(String, Array[String])] = Map.empty
    var transmap_right : Map[String,Array[String]] = Map.empty
    for (t <- transformations) { // E.g. ?a?l.r.toInt.scl[61]
        val tbits = t.trim.split("\\.", 2) // E.g.[?a?l, r.toInt.scl(_+61)]
        val vars = tbits(0).substring(1).split("\\?") // [a, l]
        val operation = tbits(1) // E.g. r.toInt.scl(_+60)
        val temp = operation.split("\\.", 2) // E.g. [r, toInt.scl(_+61)]
        val lORr = temp(0) // E.g. r
        val functions = temp(1).split("\\.") // E.g. [toInt, scl(_+61)]
        if (lORr == "l") {
            transmap_left += (vars(0) -> (vars(1), functions))
        } else
            transmap_right += (vars(1) -> functions)
    }

    query = query.replace("TRANSFORM" + trans + ")","")

    // 2. Extract star-shaped BGPs
    var qa = new QueryAnalyser(query)

    var stars = qa.getStars
    val prefixes = qa.getPrefixes
    val select = qa.getProject
    val filters = qa.getFilters

    println("\n- Predicates per star:")
    for (v <- stars._1) {
        println("* " + v._1 + ") " + v._2)
    }

    // Build ((s,p) -> o) map to check later if predicates appearing in WHERE appears actually in SELECT
    val star_predicate_var = stars._2 // assuming no (star,predicate) with two vars?

    // 3. Generate plan of joins
    println("\n/*******************************************************************/")
    println("/*                  PLAN GENERATION & MAPPINGS                     */")
    println("/*******************************************************************/")
    var pl = new Planner(stars._1)
    var pln = pl.generateJoinPlan
    var joins = pln._1
    var joinedToFlag = pln._2
    var joinedFromFlag = pln._3
    var joinPairs = pln._4

    //println("JOINS detected: " + sources)

    var neededPredicates = pl.getNeededPredicates(star_predicate_var, joins, select)

    val neededPredicatesAll = neededPredicates._1
    val neededPredicatesSelect = neededPredicates._2

    //println("joinedToFlag: " + joinedToFlag)
    println("--> Needed predicates all: " + neededPredicatesAll)

    // 4. Check mapping file
    println("---> MAPPING CONSULTATION")
    var mappingsFile = Config.get("mappings.file")
    var mappers = new Mapper(mappingsFile)
    var results = mappers.findDataSources(stars._1)

    var star_df : Map[String, DataFrame] = Map.empty

    println("\n---> GOING TO SPARK NOW TO JOIN STUFF")
    for (s <- results) {
        val star = s._1
        val datasources = s._2
        val options = s._3

        println("* Getting DF relevant to the start: " + star)

        val spark = new Sparking(Config.get("spark.url"))

        // Transformations
        val str = omitQuestionMark(star)
        var leftJoinTransformations : (String, Array[String]) = ("",null)
        var rightJoinTransformations = Array[String]()
        if (transmap_left.keySet.contains(str)) {
            // Get wth whom there is a join
            val rightOperand = transmap_left(str)._1
            val ops = transmap_left(str)._2

            // Get the predicate of the join
            val joinLeftPredicate = joinPairs((str,rightOperand))
            leftJoinTransformations = (joinLeftPredicate, ops)
            //println("Transform (left) on predicate " + joinLeftPredicate + " using " + ops.mkString("_"))
        }
        //println("transmap_right.keySet: " + transmap_right.keySet)
        if (transmap_right.keySet.contains(str)) {
            rightJoinTransformations = transmap_right(str)
            //println("Transform (right) ID using " + rightJoinTransformations.mkString("_"))
        }

        var ds : DataFrame = null
        if (joinedToFlag.contains(star) || joinedFromFlag.contains(star)) {
            //println("TRUE: " + star)
            //println("-> datasources: " + datasources)
            ds = spark.query(datasources, options, true, star, prefixes, select, star_predicate_var, neededPredicatesAll, filters, leftJoinTransformations, rightJoinTransformations, joinPairs)
            println("...with DataFrame schema: " + ds)
            ds.printSchema()
        } else if (!joinedToFlag.contains(star) && !joinedFromFlag.contains(star)) {
            //println("FALSE: " + star)
            //println("-> datasources: " + datasources)
            ds = spark.query(datasources, options, false, star, prefixes, select, star_predicate_var, neededPredicatesAll, filters, leftJoinTransformations, rightJoinTransformations, joinPairs)
            println("...with DataFrame schema: " + ds)
            ds.printSchema()
        }

        //ds.collect().foreach(s => println(s))

        star_df += (star -> ds) // DataFrame representing a star
    }

    println("\n/*******************************************************************/")
    println("/*                         QUERY EXECUTION                         */")
    println("/*******************************************************************/")
    println("- Here are the (Star, DataFrame) pairs: " + star_df)
    var df_join : DataFrame = null

    println("- Here are join pairs: " + joins + "\n")

    var firstTime = true
    val join = " x "

    val seenDF : ListBuffer[(String,String)] = ListBuffer()

    var pendingJoins = mutable.Queue[(String, (String, String))]()

    var jDF : DataFrame = null
    val it = joins.entries.iterator
    while ({it.hasNext}) {
        val entry = it.next

        val op1 = entry.getKey
        val op2 = entry.getValue._1
        val jVal = entry.getValue._2
        // TODO: add omitQuestionMark and omit it from the next

        println("-> Joining (" + op1 + join + op2 + ") using " + jVal + "...")

        var njVal = get_NS_predicate(jVal)
        var ns = prefixes(njVal._1)

        println("njVal: " + ns)

        it.remove

        val df1 = star_df(op1)
        val df2 = star_df(op2)

        if (firstTime) { // First time look for joins in the join hashmap
            println("ENTERED FIRST TIME")
            seenDF.add((op1, jVal))
            seenDF.add((op2, "ID"))
            firstTime = false

            // Join level 1
            jDF = df1.join(df2, df1.col(omitQuestionMark(op1) + "_" + omitNamespace(jVal) + "_" + ns).equalTo(df2(omitQuestionMark(op2) + "_ID")))

            jDF.show()
        } else {
            val dfs_only = seenDF.map(_._1)
            if (dfs_only.contains(op1) && !dfs_only.contains(op2)) {
                println("ENTERED NEXT TIME >> " + dfs_only)

                val leftJVar = omitQuestionMark(op1) + "_" + omitNamespace(jVal) + "_" + ns
                val rightJVar = omitQuestionMark(op2) + "_ID"
                jDF = jDF.join(df2, jDF.col(leftJVar).equalTo(df2.col(rightJVar)))

                seenDF.add((op2,"ID"))
                jDF.show()
            } else if (!dfs_only.contains(op1) && dfs_only.contains(op2)) {
                println("ENTERED NEXT TIME << " + dfs_only)

                val leftJVar = omitQuestionMark(op1) + "_" + omitNamespace(jVal) + "_" + ns
                val rightJVar = omitQuestionMark(op2) + "_ID"
                jDF = df1.join(jDF, df1.col(leftJVar).equalTo(jDF.col(rightJVar)))

                seenDF.add((op1,jVal))
                jDF.show()
            } else if (!dfs_only.contains(op1) && !dfs_only.contains(op2)) {
                println("GOING TO THE QUEUE")
                pendingJoins.enqueue((op1, (op2, jVal)))
            }
        }
    }

    while (pendingJoins.nonEmpty) {
        println("ENTERED QUEUED AREA: " + pendingJoins)
        val dfs_only = seenDF.map(_._1)

        val e = pendingJoins.head

        val op1 = e._1
        val op2 = e._2._1
        val jVal = e._2._2

        var njVal = get_NS_predicate(jVal)
        var ns = prefixes(njVal._1)

        println("-> Joining (" + op1 + join + op2 + ") using " + jVal + "...")

        val df1 = star_df(op1)
        val df2 = star_df(op2)

        if (dfs_only.contains(op1) && !dfs_only.contains(op2)) {
            val leftJVar = omitQuestionMark(op1) + "_" + omitNamespace(jVal)
            val rightJVar = omitQuestionMark(op2) + "_ID"
            jDF = jDF.join(df2, jDF.col(leftJVar).equalTo(df2.col(rightJVar)))

            seenDF.add((op2,"ID"))
        } else if (!dfs_only.contains(op1) && dfs_only.contains(op2)) {
            val leftJVar = omitQuestionMark(op1) + "_" + omitNamespace(jVal) + "_" + ns
            val rightJVar = omitQuestionMark(op2) + "_ID"
            jDF = df1.join(jDF, df1.col(leftJVar).equalTo(jDF.col(rightJVar)))

            seenDF.add((op1,jVal))
        } else if (!dfs_only.contains(op1) && !dfs_only.contains(op2)) {
            pendingJoins.enqueue((op1, (op2, jVal)))
        }

        pendingJoins = pendingJoins.tail
    }

    //println("\n--Join series: " + seenDF)

    println("--> Needed predicates select: " + neededPredicatesSelect)

    var columnNames = Seq[String]()

    for (i <- neededPredicatesSelect) {

        val star = i._1
        val ns_predicate = i._2
        val bits = get_NS_predicate(ns_predicate)

        val selected_predicate = omitQuestionMark(star) + "_" + bits._2 + "_" + prefixes(bits._1)
        columnNames = columnNames :+ selected_predicate
    }

    println("columnNames: " + columnNames)
    jDF = jDF.select(columnNames.head, columnNames.tail: _*)

    println("- Final results DF schema: ")
    jDF.printSchema()

    println("results: ")
    jDF.show()
    //df_join.collect().foreach(t => println(t))

}