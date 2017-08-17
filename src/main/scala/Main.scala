import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

/**
  * Created by mmami on 26.01.17.
  */
object Main extends App {

    Logger.getLogger("ac.biu.nlp.nlp.engineml").setLevel(Level.OFF);
    Logger.getLogger("org.BIU.utils.logging.ExperimentLogger").setLevel(Level.OFF);
    Logger.getRootLogger().setLevel(Level.OFF);

    if (args.length == 1)
        println(s"Hello, ${args(0)}")
    else
        println("I didn't get your name.")

    // 1. Read SPARQL query
    println("\n/*******************************************************************/")
    println("/*                         QUERY ANALYSIS                          */")
    println("/*******************************************************************/")
    var queryFile = Config.get("query")

    val queryString = scala.io.Source.fromFile(queryFile)
    val query = try queryString.mkString finally queryString.close()

    // 2. Extract star-shaped BGPs
    var qa = new QueryAnalyser(query)
    var stars = qa.getStars()
    val prefixes = qa.getProlog()

    println("\n- Predicates per star:")
    for(v <- stars) {
        println("* " + v._1 + ") " + v._2)
    }

    // 3. Generate plan of joins
    println("\n/*******************************************************************/")
    println("/*                  PLAN GENERATION & MAPPINGS                     */")
    println("/*******************************************************************/")
    var pl = new Planner(stars)
    var pln = pl.generateJoinPlan()
    var srcs = pln._1
    var joinFlags = pln._2

    println("JOINS detected: " + srcs)

    // 4. Check mapping file
    println("---> MAPPING CONSULTATION")
    var mappingsFile = Config.get("mappings.file")
    var mappers = new Mapper(mappingsFile)
    var results = mappers.findDataSources(stars)
    var star_df : Map[String, DataFrame] = Map()

    println("\n- The following are the join variables: " + joinFlags)

    println("\n---> GOING TO SPARK NOW TO JOIN STUFF")
    for(s <- results) {
        val star = s._1
        val datasources = s._2
        val options = s._3

        //println("Start: " + star)

        var spark = new Sparking(Config.get("spark.url"))

        var ds : DataFrame = null
        if(joinFlags.contains(star)) {
            //println("TRUE: " + star)
            //println("->datasources: " + datasources)
            ds = spark.query(datasources, options, true, star)
            println("...with DataFrame schema: ")
            ds.printSchema()
        } else {
            //println("FALSE: " + star)
            //println("->datasources: " + datasources)
            ds = spark.query(datasources, options, false, star)
            println("...with DataFrame schema: ")
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

    println("- Here are join pairs: " + srcs + "\n")

    var firstTime = true
    val join = " x "

    val seenDF : ListBuffer[(String,String)] = ListBuffer()

    val it = srcs.entries.iterator
    while ({it.hasNext}) {
        val entry = it.next

        val op1 = entry.getKey
        val op2 = entry.getValue._1
        val jVal = entry.getValue._2

        println("-> (" + op1 + join + op2 + ") using " + jVal)

        it.remove

        var jDF : DataFrame = null

        if (firstTime) { // First time look for joins in the join hashmap, later look for them in the previously joined DFs so to join with them
            seenDF.add((op1,jVal))
            seenDF.add((op2,"ID"))
            // TODO: var jdf = df(op1).join(op2).on(op1.jVal = op2.ID)
            val df1 = star_df(op1)
            val df2 = star_df(op2)

            jDF = df1.join(df2,df1.col(Helpers.omitQuestionMark(op1) + "_" + Helpers.omitNamespace(jVal)).equalTo(df2(Helpers.omitQuestionMark(op2) + "_ID")))

            jDF.show()

            val pairsHavingAsValue = srcs.entries().filter(entry => entry.getValue()._1 == op1)
            println("\n- Pairs having as value: " + op1 + " are " + pairsHavingAsValue)
            for (i <- pairsHavingAsValue) {
                println(i.getKey + " join " + op1)

                println(i.getKey + " JOIN jDF ON " + Helpers.omitQuestionMark(i.getKey) + "_" +  Helpers.omitNamespace(i.getValue._2) + " = " + Helpers.omitQuestionMark(op1) + "_ID")

                // For clarity, break down:
                val leftJ = star_df(i.getKey) // left is the jDF
                val leftJVar = Helpers.omitQuestionMark(i.getKey) + "_" + Helpers.omitNamespace(i.getValue._2) // left join variable
                val rightJVar = Helpers.omitQuestionMark(op1) + "_ID"
                jDF = leftJ.join(jDF, leftJ.col(leftJVar).equalTo(jDF.col(rightJVar)))

                jDF.show()
                seenDF.add((i.getKey, i.getValue._2))
            }

            val pairsHavingAsKey = srcs.entries().filter(entry => entry.getKey == op2)
            println("\n- Pairs having as key: " + pairsHavingAsKey)
            for (j <- pairsHavingAsKey) {
                println(op2 + " join " + j.getValue._1)

                // For clarity, break down:
                val rightJ = star_df(j.getKey) // left is the jDF
                val leftJVar = Helpers.omitQuestionMark(op2) + "_" + Helpers.omitNamespace(j.getValue._2)
                val rightJVar = Helpers.omitQuestionMark(j.getValue._1) + "_ID"
                jDF = jDF.join(rightJ, jDF.col(leftJVar).equalTo(jDF.col(rightJVar)))

                seenDF.add((j.getValue._1,"ID"))
            }


            firstTime = false

        } else {
            val dfs_only = seenDF.map(_._1)

            if(dfs_only.contains(op1) && !dfs_only.contains(op2)) {
                seenDF.add((op2,"ID"))
            } else if(!dfs_only.contains(op1) && dfs_only.contains(op2)) {
                seenDF.add((op1,jVal))
            }
        }
    }

    println("--Join series: " + seenDF)

    for(s <- seenDF) {

    }

    /*for(v <- srcs) {
        println("- DF1 of (" + v._1 + ") joins DF2 of (" + v._2 + ") using [" + Helpers.omitNamespace(v._3) + " (from " + v._3 + ") = ID]")
        val df1 = star_df(v._1)
        val df2 = star_df(v._2)

        println("DF1: ")
        //df1.collect().foreach(t => println(t.getAs("author")))
        //df1.printSchema()
        df1.show()

        println("DF2: ")
        //df2.collect().foreach(z => println(z.getAs("ID")))
        //df2.printSchema()
        df2.show()
        df_join = df1.join(df2, df1.col(Helpers.omitNamespace(v._3)).equalTo(df2("ID"))) // people.col("deptId").equalTo(department("id"))

    }*/

    println("- Final results DF schema: ")
    df_join.printSchema()

    println("results: ")
    df_join.show()
    //df_join.collect().foreach(t => println(t))

}