package org.sparkall

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.sparkall.Helpers._

import scala.collection.JavaConversions._
import scala.collection.mutable

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

    //var queryFile = Config.get("query")
    var queryFile = args(0)

    val queryString = scala.io.Source.fromFile(queryFile)
    var query = try queryString.mkString finally queryString.close()

    // Transformations
    var transformExist = false
    var trans = ""
    if (query.contains("TRANSFORM")) {
        trans = query.substring(query.indexOf("TRANSFORM") + 9, query.lastIndexOf(")")) // E.g. ?k?a.toInt && ?a?l.r.toInt.scl(_+61)
        query = query.replace("TRANSFORM" + trans + ")","") // TRANSFORM is not defined in Jena
        transformExist = true
    }

    // 2. Extract star-shaped BGPs
    var qa = new QueryAnalyser(query)

    var stars = qa.getStars
    val prefixes = qa.getPrefixes
    val select = qa.getProject
    val filters = qa.getFilters


    println("\n- Predicates per star:")
    for (v <- stars._1) {
        println(s"* $v._1) $v._2")
    }

    // Build ((s,p) -> o) map to check later if predicates appearing in WHERE appears actually in SELECT
    val star_predicate_var = stars._2 // assuming no (star,predicate) with two vars?

    // 3. Generate plan of joins
    println("\n/*******************************************************************/")
    println("/*                  PLAN GENERATION & MAPPINGS                     */")
    println("/*******************************************************************/")
    val pl = new Planner(stars._1)
    val pln = pl.generateJoinPlan
    val joins = pln._1
    val joinedToFlag = pln._2
    val joinedFromFlag = pln._3
    val joinPairs = pln._4

    //println("JOINS detected: " + sources)

    val neededPredicates = pl.getNeededPredicates(star_predicate_var, joins, select)

    val neededPredicatesAll = neededPredicates._1
    val neededPredicatesSelect = neededPredicates._2

    //println("joinedToFlag: " + joinedToFlag)
    println("--> Needed predicates all: " + neededPredicatesAll)

    // 4. Check mapping file
    println("---> MAPPING CONSULTATION")
    //var mappingsFile = Config.get("mappings.file")
    val mappingsFile = args(1)
    val configFile = args(2)

    val mappers = new Mapper(mappingsFile)
    val results = mappers.findDataSources(stars._1, configFile)

    var star_df : Map[String, DataFrame] = Map.empty
    var star_nbrFilters : Map[String, Integer] = Map()

    //val executorID = Config.get("spark.url")
    val executorID = args(3)

    val executor : SparkExecutor = new SparkExecutor(executorID, mappingsFile)

    var starDataTypesMap : Map[String, mutable.Set[String]] = Map()

    println("\n---> GOING NOW TO JOIN STUFF")
    for (s <- results) {
        val star = s._1
        val datasources = s._2
        val options = s._3

        val dataTypes = datasources.map(d => d._3)

        starDataTypesMap += (star -> dataTypes)

        println("* Getting DF relevant to the start: " + star)

        // Transformations
        var leftJoinTransformations : (String, Array[String]) = null
        var rightJoinTransformations : Array[String]  = null
        if (transformExist) {
            val (transmap_left, transmap_right) = qa.getTransformations(trans)
            val str = omitQuestionMark(star)
            if (transmap_left.keySet.contains(str)) {
                // Get wth whom there is a join
                val rightOperand = transmap_left(str)._1
                val ops = transmap_left(str)._2

                // Get the predicate of the join
                val joinLeftPredicate = joinPairs((str, rightOperand))
                leftJoinTransformations = (joinLeftPredicate, ops)
                println("Transform (left) on predicate " + joinLeftPredicate + " using " + ops.mkString("_"))
            }
            //println("transmap_right.keySet: " + transmap_right.keySet)
            if (transmap_right.keySet.contains(str)) {
                rightJoinTransformations = transmap_right(str)
                println("Transform (right) ID using " + rightJoinTransformations.mkString("_"))
            }
        }

        var ds : DataFrame = null
        var numberOfFiltersOfThisStar = 0
        var queryResults: (DataFrame,Integer) = null
        if (joinedToFlag.contains(star) || joinedFromFlag.contains(star)) {
            //println("TRUE: " + star)
            //println("-> datasources: " + datasources)
            queryResults = executor.query(datasources, options, true, star, prefixes, select, star_predicate_var, neededPredicatesAll, filters, leftJoinTransformations, rightJoinTransformations, joinPairs)
            ds = queryResults._1
            println("...with DataFrame schema: " + ds)
            ds.printSchema()
        } else if (!joinedToFlag.contains(star) && !joinedFromFlag.contains(star)) {
            //println("FALSE: " + star)
            //println("-> datasources: " + datasources)
            queryResults = executor.query(datasources, options, false, star, prefixes, select, star_predicate_var, neededPredicatesAll, filters, leftJoinTransformations, rightJoinTransformations, joinPairs)
            ds = queryResults._1
            println("...with DataFrame schema: " + ds)
            ds.printSchema()
        }

        star_df += (star -> ds) // DataFrame representing a star

        numberOfFiltersOfThisStar = queryResults._2
        star_nbrFilters += star -> numberOfFiltersOfThisStar
    }

    println("\n/*******************************************************************/")
    println("/*                         QUERY EXECUTION                         */")
    println("/*******************************************************************/")
    println("- Here are the (Star, DataFrame) pairs: " + star_df)
    var df_join : DataFrame = null

    println(s"- Here are join pairs: $joins \n")
    println(s"star_nbrFilters: $star_nbrFilters")

    var joinsToReorder : Map[String, String] = Map()

    for (j <- joins.entries)
        joinsToReorder += j.getKey -> j.getValue._1

    pl.reorder(joinsToReorder, starDataTypesMap, star_nbrFilters, configFile)

    var jDF : DataFrame = executor.join(joins,prefixes,star_df)

    //println("\n--Join series: " + seenDF)

    println(s"--> Needed predicates select: $neededPredicatesSelect")

    var columnNames = Seq[String]()

    for (i <- neededPredicatesSelect) {

        val star = i._1
        val ns_predicate = i._2
        val bits = get_NS_predicate(ns_predicate)

        val selected_predicate = omitQuestionMark(star) + "_" + bits._2 + "_" + prefixes(bits._1)
        columnNames = columnNames :+ selected_predicate
    }

    println(s"Select column names: $columnNames")

    jDF = executor.project(jDF,columnNames)

    println("- Final results DF schema: ")
    executor.schemaOf(jDF)

    val cnt = executor.count(jDF)
    println(s"Number of results ($cnt): ")
    jDF.show()

    //df_join.collect().foreach(t => println(t))

}