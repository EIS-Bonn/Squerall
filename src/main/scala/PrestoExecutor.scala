package org.squerall

import java.sql.DriverManager
import java.util

import com.google.common.collect.ArrayListMultimap
import com.typesafe.scalalogging.Logger
import model.DataQueryFrame
import org.apache.spark.sql.DataFrame
import org.squerall.Helpers._

import scala.collection.mutable
import scala.collection.mutable.{HashMap, ListBuffer, Set}

class PrestoExecutor(prestoURI: String, mappingsFile: String) extends QueryExecutor[DataQueryFrame] {

    val logger = Logger("Squerall")

    def getType() = {
        val dataframe : DataQueryFrame = null
        dataframe
    }

    private var query = ""

    def query(sources : Set[(HashMap[String, String], String, String, HashMap[String, (String, Boolean)])],
              optionsMap_entity: HashMap[String, (Map[String, String],String)],
              toJoinWith: Boolean,
              star: String,
              prefixes: Map[String, String],
              select: util.List[String],
              star_predicate_var: mutable.HashMap[(String, String), String],
              neededPredicates: Set[String],
              filters: ArrayListMultimap[String, (String, String)],
              leftJoinTransformations: (String, Array[String]),
              rightJoinTransformations: Array[String],
              joinPairs: Map[(String,String), String]
        ): (DataQueryFrame, Integer) = {

        //val spark = SparkSession.builder.master(sparkURI).appName("Squerall").getOrCreate;
        //spark.sparkContext.setLogLevel("ERROR")
        //TODO: **PRESTO?** get from the function if there is a relevant data source that requires setting config to SparkSession

        //var finalDF : String = null
        val finalDQF = new DataQueryFrame()
        var datasource_count = 0

        for (s <- sources) {
            logger.info("NEXT SOURCE...")
            datasource_count += 1 // in case of multiple relevant data sources to union

            val attr_predicate = s._1
            logger.info("Star: " + star)
            logger.info("attr_predicate: " + attr_predicate)
            val sourcePath = s._2
            val sourceType = getTypeFromURI(s._3)
            val options = optionsMap_entity(sourcePath)._1
            val entity = optionsMap_entity(sourcePath)._2

            // TODO: move to another class better
            var columns = getSelectColumnsFromSet(attr_predicate, omitQuestionMark(star), prefixes, select, star_predicate_var, neededPredicates)

            logger.info("Relevant source (" + datasource_count + ") is: [" + sourcePath + "] of type: [" + sourceType + "]")

            logger.info("...from which columns (" + columns + ") are going to be projected")
            logger.info("...with the following configuration options: " + options)

            if (toJoinWith) { // That kind of table that is the 1st or 2nd operand of a join operation
                val id = getID(sourcePath, mappingsFile)
                logger.info("...is to be joined with using the ID: " + omitQuestionMark(star) + "_" + id + " (obtained from subjectMap)")
                if(columns == "") {
                    columns = id + " AS " + omitQuestionMark(star) + "_ID"
                } else
                    columns = columns + "," + id + " AS " + omitQuestionMark(star) + "_ID"
            }

            logger.info("sourceType: " + sourceType)

            var table = ""
            sourceType match {
                case "csv" => table = s"hive.default.$entity" // get entity
                case "parquet" => table = s"hive.default.$entity" // get entity
                case "cassandra" => table = s"""cassandra.${options("keyspace")}.${options("table")}"""
                case "elasticsearch" => table = ""
                case "mongodb" => table = s"""mongodb.${options("database")}.${options("collection")}"""
                case "jdbc" => table = s"""mysql.${options("url").split("/")(3).split("\\?")(0)}.${options("dbtable")}""" // get only DB from the URL
                // jdbc:mysql://localhost:3306/db?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCod
                //TODO: currently JDBC is only MySQL, improve this
                case _ =>
            }

            // We will get the Select later to add to finalDataSet (in Run)
            finalDQF.addSelect((columns.replace("`",""),table,omitQuestionMark(star))) // Presto atm doesn't support '`'

            if (leftJoinTransformations != null && leftJoinTransformations._2 != null) {
                val column: String = leftJoinTransformations._1
                logger.info("leftJoinTransformations: " + column + " - " + leftJoinTransformations._2.mkString("."))
                val ns_pred = get_NS_predicate(column)
                val ns = prefixes(ns_pred._1)
                val pred = ns_pred._2
                val col = omitQuestionMark(star) + "_" + pred + "_" + ns
                finalDQF.addTransform(col, leftJoinTransformations._2)

            }
            if (rightJoinTransformations != null && !rightJoinTransformations.isEmpty) {
                logger.info("rightJoinTransformations: " + rightJoinTransformations.mkString("_"))
                val col = omitQuestionMark(star) + "_ID"
                finalDQF.addTransform(col, rightJoinTransformations)
            }
        }

        logger.info("- filters: " + filters + " ======= " + star)

        var whereString = ""

        var nbrOfFiltersOfThisStar = 0

        val it = filters.keySet().iterator()
        while (it.hasNext) {
            val value = it.next()
            val predicate = star_predicate_var.
                filter(t => t._2 == value).
                keys. // To obtain (star, predicate) pairs having as value the FILTER'ed value
                filter(t => t._1 == star).
                map(f => f._2).toList

            if (predicate.nonEmpty) {
                val ns_p = get_NS_predicate(predicate.head) // Head because only one value is expected to be attached to the same star an same (object) variable
                val column = omitQuestionMark(star) + "_" + ns_p._2 + "_" + prefixes(ns_p._1)
                logger.info("--- Filter column: " + column)

                nbrOfFiltersOfThisStar = filters.get(value).size()

                val conditions = filters.get(value).iterator()
                while (conditions.hasNext) {
                    val operand_value = conditions.next()
                    logger.info(s"--- Operand - Value: $operand_value")
                    whereString = column + operand_value._1 + operand_value._2
                    logger.info(s"--- WHERE string: $whereString")

                    if (operand_value._1 != "regex")
                        finalDQF.addFilter(whereString)
                    else
                        finalDQF.addFilter(s"$column like '${operand_value._2.replace("\"","")}'")
                        // regular expression with _ matching an arbitrary character and % matching an arbitrary sequence
                }
                //finalDF.show()
            }
        }
        logger.info(s"Number of filters of this star is: $nbrOfFiltersOfThisStar")

        (finalDQF, nbrOfFiltersOfThisStar)
    }

    def transform(df: Any, column: String, transformationsArray : Array[String]): (String, Boolean) = {

        var newCol = ""
        var castToVarchar = false
        for (t <- transformationsArray) {
            logger.info("Transformation next: " + t)
            t match {
                case "toInt" =>
                    logger.info("TOINT found")
                    newCol = s"cast($column AS integer)"
                case s if s.contains("scl") =>
                    val scaleValue = s.replace("scl","").trim.stripPrefix("(").stripSuffix(")")
                    logger.info("SCL found: " + scaleValue)
                    val operation = scaleValue.charAt(0) // + - *
                    if (newCol == "")
                        newCol = s"$column $operation ${scaleValue.substring(1).toInt}"
                    else
                        newCol = newCol.replace(column,s"($column $operation ${scaleValue.substring(1).toInt})")

                    //castToVarchar = true
                    logger.info(s"new value: $newCol")
                case s if s.contains("replc") =>
                    val replaceValues = s.replace("replc","").trim.stripPrefix("(").stripSuffix(")").split("\\,")
                    val valToReplace = replaceValues(0).replace("\"","")
                    val valToReplaceWith = replaceValues(1).replace("\"","")
                    logger.info("REPLC found: " + replaceValues.mkString(" -> ") + " on column: " + column)
                    if(newCol == "")
                        newCol = s"replace(cast($column AS varchar),'$valToReplace','$valToReplaceWith')"
                    else
                        newCol = s"replace(cast($newCol AS varchar),'$valToReplace','$valToReplaceWith')"

                    castToVarchar = true
                    logger.info(s"new value: $newCol")
                /*case s if s.contains("skp") =>
                    val skipValue = s.replace("skp","").trim.stripPrefix("(").stripSuffix(")")
                    logger.info("SKP found: " + skipValue)
                    ndf = ndf.filter(!ndf(column).equalTo(skipValue))
                case s if s.contains("substit") =>
                    val replaceValues = s.replace("substit","").trim.stripPrefix("(").stripSuffix(")").split("\\,")
                    val valToReplace = replaceValues(0)
                    val valToReplaceWith = replaceValues(1)
                    logger.info("SUBSTIT found: " + replaceValues.mkString(" -> "))
                    ndf = ndf.withColumn(column, when(col(column).equalTo(valToReplace), valToReplaceWith))
                    //ndf = df.withColumn(column, when(col(column) === valToReplace, valToReplaceWith).otherwise(col(column)))

                case s if s.contains("prefix") =>
                    val prefix = s.replace("prfix","").trim.stripPrefix("(").stripSuffix(")")
                    logger.info("PREFIX found: " + prefix)
                    ndf = ndf.withColumn(column, concat(lit(prefix), ndf.col(column)))
                case s if s.contains("postfix") =>
                    val postfix = s.replace("postfix","").trim.stripPrefix("(").stripSuffix(")")
                    logger.info("POSTFIX found: " + postfix)
                    ndf = ndf.withColumn(column, concat(lit(ndf.col(column), postfix)))
                */
                case _ =>
            }
        }

        (newCol,castToVarchar)
    }

    def join(joins: ArrayListMultimap[String, (String, String)], prefixes: Map[String, String], star_df: Map[String, DataQueryFrame]): DataQueryFrame = {
        import scala.collection.JavaConversions._
        import scala.collection.mutable.ListBuffer

        var pendingJoins = mutable.Queue[(String, (String, String))]()
        val seenDF : ListBuffer[(String,String)] = ListBuffer()
        var firstTime = true
        val join = " x "
        val jDQF = new DataQueryFrame()

        val it = joins.entries.iterator
        while (it.hasNext) {
            val entry = it.next

            val table1 = entry.getKey
            val table2 = entry.getValue._1
            val jVal = entry.getValue._2
            // TODO: add omitQuestionMark and omit it from the next

            logger.info(s"-> GOING TO JOIN ($table1 $join $table2) USING $jVal...")

            val njVal = get_NS_predicate(jVal)
            val ns = prefixes(njVal._1)

            logger.info("njVal: " + ns)

            it.remove()

            if (firstTime) { // First time look for joins in the join hashmap
                logger.info("...that's the FIRST JOIN")
                seenDF.add((table1, jVal))
                seenDF.add((table2, "ID"))
                firstTime = false

                // Join level 1
                jDQF.addJoin((omitQuestionMark(table1),omitQuestionMark(table2),omitQuestionMark(table1) + "_" + omitNamespace(jVal) + "_" + ns,omitQuestionMark(table2) + "_ID"))
                //jDQF = df1.join(df2, df1.col(omitQuestionMark(table1) + "_" + omitNamespace(jVal) + "_" + ns).equalTo(df2(omitQuestionMark(table2) + "_ID")))
                logger.info("...done")
            } else {
                val dfs_only = seenDF.map(_._1)
                logger.info(s"EVALUATING NEXT JOIN \n ...checking prev. done joins: $dfs_only")
                if (dfs_only.contains(table1) && !dfs_only.contains(table2)) {
                    logger.info("...we can join (this direction >>)")

                    val leftJVar = omitQuestionMark(table1) + "_" + omitNamespace(jVal) + "_" + ns
                    val rightJVar = omitQuestionMark(table2) + "_ID"
                    jDQF.addJoin((omitQuestionMark(table1),omitQuestionMark(table2),leftJVar,rightJVar))
                    //jDQF = jDQF.join(df2, jDQF.col(leftJVar).equalTo(df2.col(rightJVar)))

                    seenDF.add((table2,"ID"))

                } else if (!dfs_only.contains(table1) && dfs_only.contains(table2)) {
                    logger.info("...we can join (this direction >>)")

                    val leftJVar = omitQuestionMark(table1) + "_" + omitNamespace(jVal) + "_" + ns
                    val rightJVar = omitQuestionMark(table2) + "_ID"
                    jDQF.addJoin((omitQuestionMark(table1),omitQuestionMark(table2),leftJVar,rightJVar))
                    //jDQF = df1.join(jDQF, df1.col(leftJVar).equalTo(jDQF.col(rightJVar)))

                    seenDF.add((table1,jVal))

                } else if (!dfs_only.contains(table1) && !dfs_only.contains(table2)) {
                    logger.info("...no join possible -> GOING TO THE QUEUE")
                    pendingJoins.enqueue((table1, (table2, jVal)))
                }
                // TODO: add case of if dfs_only.contains(table1) && dfs_only.contains(table2)
            }
        }

        while (pendingJoins.nonEmpty) {
            logger.info("ENTERED QUEUED AREA: " + pendingJoins)
            val dfs_only = seenDF.map(_._1)

            val e = pendingJoins.head

            val table1 = e._1
            val table2 = e._2._1
            val jVal = e._2._2

            val njVal = get_NS_predicate(jVal)
            val ns = prefixes(njVal._1)

            logger.info(s"-> Joining ($table1 $join $table2) using $jVal...")


            if (dfs_only.contains(table1) && !dfs_only.contains(table2)) {
                val leftJVar = omitQuestionMark(table1) + "_" + omitNamespace(jVal) + "_" + ns
                val rightJVar = omitQuestionMark(table2) + "_ID"
                jDQF.addJoin((omitQuestionMark(table1),omitQuestionMark(table2),leftJVar,rightJVar))
                seenDF.add((table2,"ID"))
            } else if (!dfs_only.contains(table1) && dfs_only.contains(table2)) {
                val leftJVar = omitQuestionMark(table1) + "_" + omitNamespace(jVal) + "_" + ns
                val rightJVar = omitQuestionMark(table2) + "_ID"
                jDQF.addJoin((omitQuestionMark(table1),omitQuestionMark(table2),leftJVar,rightJVar))

                seenDF.add((table1,jVal))
            } else if (!dfs_only.contains(table1) && !dfs_only.contains(table2)) {
                pendingJoins.enqueue((table1, (table2, jVal)))
            }

            pendingJoins = pendingJoins.tail
        }

        for(sdf <- star_df) { // Even though we added those before we need to get them and add them to the big finalDataSet
            val selects = sdf._2.asInstanceOf[DataQueryFrame].getSelects.head

            // head of (0) because one star has one select, it's the whole join star that contains multiple selects
            jDQF.addSelect(selects)


            val filters = sdf._2.asInstanceOf[DataQueryFrame].getFilters
            if(filters.nonEmpty) jDQF.asInstanceOf[DataQueryFrame].addFilter(s"${filters.mkString(" AND ")}")
            // head of (0) because one star has one filter, it's the whole join star that contains multiple filter sets

            val transformations = sdf._2.asInstanceOf[DataQueryFrame].getTransform
            if(transformations.nonEmpty) jDQF.addTransformations(transformations)
        }

        jDQF
    }

    def joinReordered(joins: ArrayListMultimap[String, (String, String)], prefixes: Map[String, String], star_df: Map[String, DataFrame], startingJoin: (String, (String, String)), starWeights: Map[String, Double]): String = {
        null
    }

    def project(jDQF: Any, columnNames: Seq[String], distinct: Boolean) : DataQueryFrame = {
        if(!distinct)
            jDQF.asInstanceOf[DataQueryFrame].addProject((columnNames, false))
        else
            jDQF.asInstanceOf[DataQueryFrame].addProject((columnNames, true))

        jDQF.asInstanceOf[DataQueryFrame]
    }

    def schemaOf(jDF: DataQueryFrame) = {
        null // TODO: prntiSchema in Presto?
    }

    def count(jDQF: DataQueryFrame): Long = {
        -12345789 // TODO: think about COUNT in Presto
    }

    def orderBy(jDQF: Any, direction: String, variable: String) : DataQueryFrame = {
        logger.info("ORDERING...")

        if (direction == "-1") {
            jDQF.asInstanceOf[DataQueryFrame].addOrderBy((variable,1))  // 1: asc
            //jDF.orderBy(asc(variable))
        } else { // TODO: assuming the other case is automatically -1 IFNOT change to "else if (direction == "-2") {"
            jDQF.asInstanceOf[DataQueryFrame].addOrderBy((variable,-1)) // 2: desc
            //jDF.orderBy(desc(variable))
        }

        jDQF.asInstanceOf[DataQueryFrame]
    }

    def groupBy(jDQF: Any, groupBys: (ListBuffer[String], Set[(String,String)])): DataQueryFrame = {

        val groupByVars = groupBys._1
        val aggregationFunctions = groupBys._2

        logger.info("aggregationFunctions: " + aggregationFunctions)

        var aggSet : Set[(String,String)] = Set()
        for (af <- aggregationFunctions){
            aggSet += ((af._1,af._2))
        }
        val agg = aggSet.toList

        jDQF.asInstanceOf[DataQueryFrame].addGroupBy(groupByVars)
        jDQF.asInstanceOf[DataQueryFrame].addAggregate(agg)

        // eg df.groupBy("department").agg(max("age"), sum("expense"))
        // ("o_price_cbo","sum"),("o_price_cbo","max")
        //newJDF.printSchema()

        jDQF.asInstanceOf[DataQueryFrame]
    }

    def limit(jDQF: Any, limitValue: Int) : DataQueryFrame = {
        jDQF.asInstanceOf[DataQueryFrame].addLimit(limitValue)

        jDQF.asInstanceOf[DataQueryFrame]
    }

    def show(jDQF: Any) = {
        val selects: mutable.Seq[(String, String, String)] = jDQF.asInstanceOf[DataQueryFrame].getSelects
        val joins: mutable.Seq[(String, String, String, String)] = jDQF.asInstanceOf[DataQueryFrame].getJoins
        val filters: mutable.Seq[String] = jDQF.asInstanceOf[DataQueryFrame].getFilters
        val project: (Seq[String], Boolean) = jDQF.asInstanceOf[DataQueryFrame].getProject
        val groupBy: mutable.Seq[String] = jDQF.asInstanceOf[DataQueryFrame].getGroupBy
        val orderBy: (String, Int) = jDQF.asInstanceOf[DataQueryFrame].getOrderBy
        val aggreggate = jDQF.asInstanceOf[DataQueryFrame].getAggregate
        val limit: Int = jDQF.asInstanceOf[DataQueryFrame].getLimit
        val transformations : Map[String, Array[String]] = jDQF.asInstanceOf[DataQueryFrame].getTransform

        // Prepare the sub-selects
        var subSelects : Map[String,(String,String)] = Map()
        var castedToVarchar : Set[String] = Set()

        for (s <- selects) {
            var select = s._1

            if (transformations.nonEmpty) {
                val selectedColumns = select.split(",")
                for (sc <- selectedColumns) {
                    val col = sc.split(" AS ")(0)
                    val alias = sc.split(" AS ")(1)
                    //var transformedColumn = ""
                    //var castToVarchar = false
                    var transformedColumnWithAlias = ""
                    if (transformations.contains(alias)) {
                        val (transformedColumn,castToVarchar) = transform(null, col, transformations(alias)) // NULL because we don't change anything (unlike in SparkExecutor)
                        transformedColumnWithAlias = s"$transformedColumn AS $alias"

                        //if (transformedColumn != "") { // Needs to change when TRANSFORM on multiple joins?
                        select =  select.replace(sc,transformedColumnWithAlias)
                        //}
                        if (castToVarchar)
                            castedToVarchar += alias // need this to solve type mismatch (varchar - int)
                    }
                }
            }

            val from = s._2
            val tableAlias = s._3
            subSelects += (tableAlias -> (select,from))
        }

        val distinct = if(project._2) " distinct " else " "
        var query = s"SELECT$distinct${project._1.mkString(",")} FROM ("

        val joinedSelect : Set[String] = Set()
        // Construct the SELECT & JOIN .. ON

        for (j <- joins) {
            val leftTable = j._1
            val rightTable = j._2

            logger.info("leftTable: " + leftTable + " rightTable " + rightTable)
            if (!joinedSelect.contains(leftTable) && !joinedSelect.contains(rightTable)) {
                joinedSelect += leftTable
                joinedSelect += rightTable

                val lselect = subSelects(leftTable)._1
                val lfrom = subSelects(leftTable)._2

                val rselect = subSelects(rightTable)._1
                val rfrom = subSelects(rightTable)._2

                query += s"\n(SELECT $lselect FROM $lfrom) AS $leftTable"
                query += "\nJOIN"
                query += s"\n(SELECT $rselect FROM $rfrom) AS $rightTable"
            } else if (joinedSelect.contains(leftTable) && !joinedSelect.contains(rightTable)) {
                joinedSelect += rightTable

                val rselect = subSelects(rightTable)._1
                val rfrom = subSelects(rightTable)._2

                query += "\nJOIN"
                query += s"\n(SELECT $rselect FROM $rfrom) AS $rightTable"
            } else if(!joinedSelect.contains(leftTable) && joinedSelect.contains(rightTable)) {
                joinedSelect += leftTable

                val lselect = subSelects(leftTable)._1
                val lfrom = subSelects(leftTable)._2

                query += "\nJOIN"
                query += s"\n(SELECT $lselect FROM $lfrom) AS $leftTable"
            }

            val leftVar = j._3
            val rightVar = j._4
            var leftVarFull = s"$leftTable.${j._3}"
            var rightVarFull = s"$rightTable.${j._4}"

            if (castedToVarchar.contains(leftVar) && !castedToVarchar.contains(rightVar)) {
                rightVarFull = s"cast($rightVarFull AS VARCHAR)"
            } else if (!castedToVarchar.contains(leftVar) && castedToVarchar.contains(rightVar)) {
                leftVarFull = s"cast($leftVarFull AS VARCHAR)"
            }

            query += s"\nON $leftVarFull=$rightVarFull"
        }
        query += "\n)"
        if (filters.nonEmpty) query += s"\nWHERE ${filters.mkString(" AND ").replace("\"","\'")}"
        if (groupBy.nonEmpty) query += s"\nGROUP BY ${groupBy.mkString(",")}"
        if (orderBy != ("",0)) {
            query += s"\nORDER BY "
            val asc_desc = orderBy._2
            val col = orderBy._1
            val ob = if (asc_desc == 1) s"$col ASC" else s"$col DESC"
            query += ob
        }
        if (limit > 0) query += s"\nlimit $limit"
        // limit is 0 => no limit

        logger.info(s"\nQuery:\n$query")
        this.query = query
    }

    def run(jDF: Any) = {
        // TODO: jDF isn't used here, figure it out
        this.show(jDF)

        // example prestoURI = "jdbc:presto://localhost:8080"
        val connection = DriverManager.getConnection(prestoURI, "presto_user", null) // null: properties

        try {
            val st = connection.createStatement
            val resultSet = st.executeQuery(query)

            val metadata = resultSet.getMetaData
            val columnCount = metadata.getColumnCount

            // Printing schema
            print("\nResults (showing first 20):\n")
            for (i <- 1 to columnCount) {
                val name = metadata.getColumnName(i)
                print(s"$name |")
            }
            print("\n")
            var count = 0
            while (resultSet.next && count < 20) {
                var row = "|"
                for (i <- 1 to columnCount) {
                    row += resultSet.getString(i) + "|"
                }
                logger.info(row)
                count += 1
            }
        } catch  {
            case e: Exception => e.printStackTrace()
        } finally {
            connection.close()
        }

    }

}
